#!/usr/bin/env python3
"""Render and run isolated Kubernetes benefit scenarios; see k8s-automation.md."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid

import yaml

from k8s_cases import cases

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
DOC = ROOT / "docs/design/vertical-elastic-benefit-k8s-mock-test-cn.md"
NAMES = ["00-namespace.yaml", "00-rbac.yaml", "00-storage.yaml",
         "00-benefit-env.yaml", "00-fixture-scenario.yaml", "00-fixture-code.yaml",
         "00-fixture-service.yaml", "01-manual-idle.yaml", "02-job.yaml",
         "03-trigger.yaml", "04-exploration-env.yaml"]
JOB_LABEL = "elasticjob.dlrover/name"
ROLE_LABEL = "elasticjob.dlrover/replica-type"


class DependencyMissing(RuntimeError):
    pass


def dump(path, value):
    Path(path).write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def records(log, marker):
    """Only parse JSON after the known logger marker, ignoring unrelated lines."""
    found = []
    for line in log.splitlines():
        if marker not in line:
            continue
        try:
            value, _ = json.JSONDecoder().raw_decode(line.split(marker, 1)[1].lstrip())
            if isinstance(value, dict):
                found.append(value)
        except ValueError:
            continue
    return found


def step_values(log, pattern):
    return [int(match.group(1)) for match in re.finditer(pattern, log)]


def extract(destination):
    blocks = re.findall(r"```yaml\n(.*?)\n```", DOC.read_text(), re.S)
    if len(blocks) != len(NAMES):
        raise ValueError("Document YAML layout changed; check extraction mapping")
    destination.mkdir(parents=True, exist_ok=True)
    if any((destination / name).exists() for name in NAMES):
        raise ValueError("Destination already contains templates; refusing to overwrite edits")
    for name, block in zip(NAMES, blocks):
        (destination / name).write_text(block + "\n")


def configuration(path, case_id):
    config = yaml.safe_load(path.read_text())
    overrides = config.pop("cases", {}).get(case_id, {})
    # Lists replace defaults, including an explicit empty infrastructure list.
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(config.get(key), dict):
            config[key] = {**config[key], **value}
        else:
            config[key] = value
    config["base_dir"] = str(path.resolve().parent)
    return config


def path_for(config, name):
    return Path(config["base_dir"]) / name


def render(config, case, out):
    """No cluster access. Each input filename is independently replaceable."""
    out.mkdir(parents=True, exist_ok=False)
    ns, job_name = config["namespace"], config.get("job_name", "benefit-mock")
    source = path_for(config, config["manifests_dir"])
    inputs = config.get("templates", {})

    def read(name):
        path = path_for(config, inputs[name]) if name in inputs else source / name
        docs = list(yaml.safe_load_all(path.read_text()))
        for obj in docs:
            if not obj:
                continue
            meta = obj.setdefault("metadata", {})
            if obj["kind"] == "Namespace":
                meta["name"] = ns
            else:
                meta["namespace"] = ns
            for subject in obj.get("subjects", []):
                if subject.get("kind") == "ServiceAccount":
                    subject["namespace"] = ns
        return docs

    env = read("00-benefit-env.yaml")[0]
    env["data"].update(DLROVER_BENEFIT_MODE=case["mode"],
                       DLROVER_BENEFIT_OBJECTIVE=case["objective"],
                       DLROVER_BENEFIT_EXPLORE="false")
    if case["id"].startswith("E"):
        env["data"].update(read("04-exploration-env.yaml")[0]["data"])
        env["data"]["DLROVER_BENEFIT_HTTP_URL"] = config.get("adapter_url", "REPLACE_EXECUTION_ADAPTER")
        env["data"]["DLROVER_BENEFIT_OBJECTIVE"] = case["objective"]
    env["data"].update({k: str(v) for k, v in config.get("env", {}).items()})
    env["data"].update(case["env"])
    scenario = read("00-fixture-scenario.yaml")[0]
    scenario["data"]["scenario.json"] = json.dumps(case["fixture"])
    objects = {"00-benefit-env.yaml": [env], "00-fixture-scenario.yaml": [scenario]}
    infrastructure = config.get("infrastructure", NAMES[:7])
    apply_files = []
    for index, name in enumerate(infrastructure):
        # Built-in names use templates mapping. Other paths are relative to config.
        if name in NAMES:
            docs = objects[name] if name in objects else read(name)
        else:
            docs = list(yaml.safe_load_all(path_for(config, name).read_text()))
        filename = f"infra-{index:02}.yaml"
        (out / filename).write_text(yaml.safe_dump_all(docs, sort_keys=False))
        apply_files.append(filename)
    job = read("02-job.yaml")[0]
    job["metadata"]["name"] = job_name
    job["metadata"].setdefault("labels", {})["benefit-test-run"] = out.name
    if case["id"] == "S03":
        job["metadata"].get("labels", {}).pop("low_code_platform_task_id", None)
    replicas = job["spec"]["replicaSpecs"]
    if replicas["worker"].get("replicas") != 1:
        raise ValueError("The documented scenario matrix requires exactly one worker")
    master = next(c for c in replicas["dlrover-master"]["template"]["spec"]["containers"]
                  if c["name"] == config.get("master_container", "main"))
    for flag, value in (("--namespace", ns), ("--job_name", job_name)):
        if flag in master.get("args", []):
            master["args"][master["args"].index(flag) + 1] = value
    worker = next(c for c in replicas["worker"]["template"]["spec"]["containers"]
                  if c["name"] == config.get("worker_container", "main"))
    gpu = config.get("gpu_resource", "nvidia.com/gpu")
    for resource in ("requests", "limits"):
        # Only the configured resource key is changed; reject a template with another GPU type.
        resources = worker["resources"][resource]
        if gpu not in resources:
            raise ValueError(f"Worker template lacks {gpu} {resource}; supply a matching template")
        resources[gpu] = case["initial_gpus"]
    replacements = {"initial_gpus": case["initial_gpus"],
                    "accumulation": 32 // case["initial_gpus"], "run_id": out.name}
    if config.get("worker_args"):
        worker["args"] = [arg.format(**replacements) for arg in config["worker_args"]]
    else:
        args = "\n".join(worker.get("args", []))
        for flag, value in (("nproc_per_node", case["initial_gpus"]),
                            ("gradient_accumulation_steps", replacements["accumulation"])):
            args, count = re.subn(r"(--" + flag + r"[= ])\d+", lambda m: m[1] + str(value), args)
            if not count:
                raise ValueError(f"Cannot adapt --{flag}; provide worker_args in configuration")
        args, count = re.subn(r"(--output_dir[= ])\S+", lambda m: m[0] + "/" + out.name, args)
        if not count:
            raise ValueError("Provide worker_args with a unique {run_id} output directory")
        # The documented template has one shell argument. Do not merge arbitrary argv.
        if len(worker.get("args", [])) != 1:
            raise ValueError("Multi-argument worker commands require explicit worker_args")
        worker["args"] = [args]
    objects["job.yaml"] = [job]
    for name, action in (("idle.yaml", "not_adjust"), ("trigger.yaml", "manual")):
        cm = read("01-manual-idle.yaml" if action == "not_adjust" else "03-trigger.yaml")[0]
        value = json.loads(cm["data"]["config.json"])
        value.update(scale_action=action, job_names=job_name)
        value["customize_worker_resource"].update(gpus=case["target_gpus"], gpu_type=gpu)
        cm["data"]["config.json"] = json.dumps(value)
        objects[name] = [cm]
    for name, docs in objects.items():
        (out / name).write_text(yaml.safe_dump_all(docs, sort_keys=False))
    expected = dict(case=case, namespace=ns, job_name=job_name, context=config.get("context"),
                    infrastructure=apply_files, expected_env=env["data"])
    dump(out / "plan.json", expected)
    return expected


class Runner:
    def __init__(self, config, plan, out):
        self.config, self.plan, self.out = config, plan, out
        self.case = plan["case"]
        self.master = None
        self.uid = None
        self.plan_id = None
        self.created = False
        self.triggered = False
        self.poll = float(config.get("poll_seconds", 5))
        self.timeout = float(config.get("timeout_seconds", 1800))
        self.prefix = [config.get("kubectl", "kubectl")]
        if config.get("context"):
            self.prefix += ["--context", config["context"]]
        self.prefix += ["--request-timeout=30s", "-n", plan["namespace"]]

    def kubectl(self, *args, stdin=None):
        proc = subprocess.run(self.prefix + list(args), input=stdin, text=True,
                              capture_output=True, timeout=60)
        if proc.returncode:
            raise RuntimeError(f"kubectl {' '.join(args[:3])}: {proc.stderr.strip()}")
        return proc.stdout

    def remote(self, code, *args):
        return self.kubectl("exec", "-i", self.master, "-c",
                            self.config.get("master_container", "main"),
                            "--", "python", "-", *map(str, args), stdin=code)

    def wait(self, description, check, timeout=None):
        print(f"[{self.case['id']}] {description}", flush=True)
        deadline = time.monotonic() + (self.timeout if timeout is None else timeout)
        while True:
            result = check()
            if result:
                return result
            if time.monotonic() >= deadline:
                raise TimeoutError(description)
            time.sleep(self.poll)

    def pods(self, role=None):
        selector = f"{JOB_LABEL}={self.plan['job_name']}"
        if role:
            selector += f",{ROLE_LABEL}={role}"
        pods = json.loads(self.kubectl("get", "pods", "-l", selector, "-o", "json"))["items"]
        # Controllers may use a transitive owner; the unique job label is checked with job UID separately.
        return [p for p in pods if not p["metadata"].get("deletionTimestamp")]

    def live(self, role):
        return [p for p in self.pods(role) if p["status"].get("phase") == "Running"]

    def logs(self, pod=None, worker=False):
        return self.kubectl("logs", pod or self.master, "-c",
                            self.config.get("worker_container" if worker else "master_container", "main"),
                            "--timestamps")

    def flow(self):
        log = self.logs()
        (self.out / "master.log").write_text(log)
        return records(log, "BenefitFlow ")

    def events(self):
        return records(self.logs(), "VerticalElasticEvent ")

    def workers(self, gpus):
        pods = self.live("worker")
        if len(pods) != 1:
            return None
        p = pods[0]
        c = next(c for c in p["spec"]["containers"] if c["name"] == self.config.get("worker_container", "main"))
        key = self.config.get("gpu_resource", "nvidia.com/gpu")
        if any(float(c["resources"][r].get(key, 0)) != gpus for r in ("requests", "limits")):
            return None
        return p

    def signature(self, pod):
        return (pod["metadata"]["uid"], tuple((c["name"], c.get("restartCount", 0))
                for c in pod["status"].get("containerStatuses", [])))

    def progress(self, gpus):
        first = None
        signature = None
        pattern = self.config["step_regex"]

        def check():
            nonlocal first, signature
            pod = self.workers(gpus)
            if not pod:
                return False
            values = step_values(self.logs(pod["metadata"]["name"], True), pattern)
            if not values:
                return False
            current = self.signature(pod)
            if signature != current:
                signature, first = current, values[-1]
                return False
            return pod if values[-1] > first else False
        return self.wait("等待真实训练 step 在两次采样间增长", check)

    def stable_delay(self, seconds, pod):
        # Use Master time for sample boundaries; monitor Pod identity during the wait.
        end = float(self.remote("import time; print(time.time())")) + seconds
        signature = self.signature(pod)

        def check():
            current = self.workers(self.case["initial_gpus"])
            if not current or self.signature(current) != signature:
                raise RuntimeError("Worker changed during baseline wait; start a fresh case")
            return float(self.remote("import time; print(time.time())")) >= end
        self.wait(f"等待 {seconds:g} 秒并监视 Worker", check, seconds + 90)

    def hook(self, name, required=False):
        command = self.config.get("hooks", {}).get(name)
        if not command:
            if required:
                raise DependencyMissing(f"Missing hooks.{name}")
            return
        if not isinstance(command, list) or not all(isinstance(x, str) for x in command):
            raise ValueError(f"hooks.{name} must be an argv list, never a shell string")
        context = dict(case=self.case, namespace=self.plan["namespace"],
                       job_name=self.plan["job_name"], job_uid=self.uid,
                       master=self.master, plan_id=self.plan_id, artifact_dir=str(self.out.resolve()))
        context_file = self.out / "hook-context.json"
        dump(context_file, context)
        env = {**os.environ, "BENEFIT_TEST_CONTEXT": str(context_file.resolve())}
        with (self.out / f"hook-{name}.log").open("w") as log:
            subprocess.run(command, cwd=self.config["base_dir"], env=env, stdout=log,
                           stderr=subprocess.STDOUT, timeout=self.timeout, check=True)

    def preflight(self):
        if self.case["id"].startswith("E") and not self.config.get("adapter_url"):
            raise DependencyMissing("Exploration requires a real adapter_url")
        if "REPLACE_" in self.config.get("context", ""):
            raise DependencyMissing("Fill the kubectl context in configuration")
        for file in self.out.glob("*.yaml"):
            if file.name in self.plan["infrastructure"] + ["job.yaml", "idle.yaml", "trigger.yaml"]:
                if "REPLACE_" in file.read_text():
                    raise DependencyMissing(f"Unfilled placeholder in {file.name}")
        if not self.config.get("step_regex") or re.compile(self.config["step_regex"]).groups != 1:
            raise DependencyMissing("step_regex must capture exactly one cumulative optimizer step")
        if self.poll <= 0 or self.timeout <= 0:
            raise ValueError("poll_seconds and timeout_seconds must be positive")
        expected = self.plan["expected_env"]
        if float(expected["DLROVER_BENEFIT_MAX_AGE_SECONDS"]) >= 1200 and self.case["id"] == "G15":
            raise ValueError("G15 snapshot_age must exceed the configured max_age")
        if float(expected["DLROVER_BENEFIT_TIMEOUT_SECONDS"]) >= 15 and self.case["id"] == "G19":
            raise ValueError("G19 sleep_seconds must exceed the configured HTTP timeout")
        if self.case["id"].startswith("E"):
            required = ["prepare_adapter", "verify_adapter"]
            if self.case["id"] in ("E08", "E09", "E10"):
                required += ["drive_fault"]
            for name in required:
                if not self.config.get("hooks", {}).get(name):
                    raise DependencyMissing(f"Exploration requires hooks.{name}")

    def verify_adapter(self):
        self.hook("verify_adapter", required=True)
        report = json.loads((self.out / "adapter-verification.json").read_text())
        for field, expected in (("case", self.case["id"]), ("job_uid", self.uid), ("plan_id", self.plan_id)):
            if report.get(field) != expected:
                raise AssertionError(f"Adapter verification {field} mismatch")
        required = {
            "E01": ["result_recorded", "candidate_retained", "no_reverse_plan"],
            "E02": ["result_recorded", "candidate_retained", "no_reverse_plan"],
            "E03": ["result_recorded", "candidate_retained", "no_reverse_plan"],
            "E04": ["fenced", "result_recorded", "no_reverse_plan"],
            "E05": ["admission_rejected"],
            "E06": ["no_exploration_started"],
            "E07": ["no_exploration_started"],
            "E08": ["journal_recovered", "no_duplicate_replacement", "training_recovered"],
            "E09": ["fenced", "no_mutation_replayed", "checkpoint_resources_checked"],
            "E10": ["authorization_denied", "no_worker_replacement", "checkpoint_resources_checked"],
        }[self.case["id"]]
        if any(report.get("checks", {}).get(key) is not True for key in required):
            raise AssertionError(f"Adapter verification requires checks: {required}")
        if not report.get("evidence"):
            raise AssertionError("Adapter verification must link saved execution evidence")
        for filename in report["evidence"]:
            path = (self.out / filename).resolve()
            if self.out.resolve() not in path.parents or not path.is_file() or not path.stat().st_size:
                raise AssertionError("Adapter evidence must be nonempty files inside artifact_dir")

    def generate(self, start=100, end=160, window=60, now=None, label="swanlab"):
        task = "wrong-task" if self.case["id"] == "S02" else self.config.get("task_id", "123")
        args = ["--task-id", str(task), "--assume-fixed-batch", "--window-seconds", str(window),
                "--work-start", str(start), "--work-end", str(end),
                "--total-steps", str(self.case["total_steps"]), "--output",
                self.plan["expected_env"]["DLROVER_SWANLAB_MOCK_FILE"]]
        if now is not None:
            args += ["--now", str(now)]
        self.remote((HERE / "generate_swanlab_mock.py").read_text(), *args)
        path = self.plan["expected_env"]["DLROVER_SWANLAB_MOCK_FILE"]
        code = "import json,os,sys\nfrom pathlib import Path\np=Path(sys.argv[1]); rows=json.loads(p.read_text())\n"
        if self.case["id"] == "S07":
            code += "for row in rows:\n if row['key']=='global_step':\n  for point,value in zip(row['scalarMetricsList'],[100,101,160]): point['data']=str(value)\n"
        if self.case["id"] == "S08":
            code += "rows=[r for r in rows if r['key']!='train_speed']\n"
        code += "tmp=p.with_suffix('.tmp'); tmp.write_text(json.dumps(rows)); os.replace(tmp,p); print(json.dumps(rows))"
        (self.out / f"{label}.json").write_text(self.remote(code, path))

    def journal(self):
        state = json.loads(self.remote("""import json,sys
from pathlib import Path
matches=[json.loads(p.read_text()) for p in Path(sys.argv[1]).glob('*.json')]
matches=[s for s in matches if s.get('id')==sys.argv[2]]
if len(matches)>1: raise RuntimeError('Duplicate journal')
print(json.dumps(matches[0] if matches else {}))
""", self.plan["expected_env"]["DLROVER_BENEFIT_STATE_DIR"], self.plan_id))
        dump(self.out / "trial-state.json", state)
        return state

    def explore(self):
        key = self.case["id"]
        if key in ("E08", "E09", "E10"):
            # These faults depend on controller restart semantics and adapter authorization.
            # The configured driver must implement the documented timing and write its evidence.
            self.hook("drive_fault", required=True)
            self.master = self.wait("等待重启后的 Master", lambda: next(
                (p["metadata"]["name"] for p in self.live("dlrover-master")), None))
            current = json.loads(self.kubectl("get", "elasticjob", self.plan["job_name"], "-o", "json"))
            if current["metadata"]["uid"] != self.uid:
                raise AssertionError("Fault driver changed job UID")
            self.verify_adapter()
            return
        if key == "E04":
            state = self.wait("等待 OBSERVING（必须在240秒内到达）", lambda: (
                s if (s := self.journal()).get("status") == "OBSERVING" else None), 240)
            if float(self.remote("import time; print(time.time())")) - state["started_at"] > 240:
                raise AssertionError("E04 did not reach OBSERVING within 240 seconds")
        else:
            self.windows("OBSERVING", 2, {"E01": 120, "E02": 30, "E03": 60}[key], 100060)
        state = self.wait("等待探索结果及回传确认", lambda: (
            s if (s := self.journal()).get("status") in
            ("INCONCLUSIVE", "FAILED", "ACCEPTED", "RETAINED")
            and s.get("reported") else None))
        if state["status"] != self.case["terminal"]:
            raise AssertionError(f"Expected {self.case['terminal']}, got {state['status']}: {state.get('reason')}")
        self.verify_adapter()

    def windows(self, status, gpu, increment, start):
        state = self.wait(f"等待 {status}", lambda: (
            s if (s := self.journal()).get("status") == status else None))
        applied = self.plan_id
        self.wait("等待真实 training_recovered", lambda: any(
            e.get("event_type") == "training_recovered" and e.get("plan_id") == applied for e in self.events()))
        pod = self.progress(gpu)
        signature = self.signature(pod)
        ready = float(self.remote("import time; print(time.time())"))
        end = ready + float(self.plan["expected_env"]["DLROVER_SWANLAB_WARMUP_SECONDS"]) + 65
        for index in range(2):
            def ready_to_write():
                now = float(self.remote("import time; print(time.time())"))
                s = self.journal()
                current = self.workers(gpu)
                if s.get("status") != status or not current or self.signature(current) != signature:
                    raise AssertionError("Observation state/worker changed before window submission")
                if s["expires_at"] - now < 30 or now - end > 300:
                    raise AssertionError("Insufficient observation budget or missed window")
                return now >= end
            self.wait("等待候选窗口时间边界", ready_to_write)
            if self.journal()["checkpoint_step"] >= start:
                raise AssertionError("Mock progress does not exceed real checkpoint; redesign trajectory")
            self.generate(start, start + increment, now=end, label=f"{status.lower()}-{index + 1}")
            if index == 0:
                accepted = self.wait("等待第一窗口被 journal 接受", lambda: (
                    s if (s := self.journal()).get("status") == status and s.get("windows") == 1 else None))
                end = accepted["last_window_end"] + 60
                start += increment

    def run(self):
        self.preflight()
        try:
            commit = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True,
                                    capture_output=True, timeout=10, check=True).stdout
            (self.out / "commit.txt").write_text(commit)
        except (OSError, subprocess.SubprocessError):
            (self.out / "commit.txt").write_text("unavailable\n")
        # Namespace creation is optional and only allowed when explicitly selected.
        namespace = self.kubectl("get", "namespace", self.plan["namespace"], "--ignore-not-found", "-o", "name")
        if not namespace.strip():
            found = False
            for filename in self.plan["infrastructure"]:
                docs = list(yaml.safe_load_all((self.out / filename).read_text()))
                for obj in docs:
                    if obj and obj.get("kind") == "Namespace" and obj["metadata"]["name"] == self.plan["namespace"]:
                        self.kubectl("apply", "-f", "-", stdin=yaml.safe_dump(obj))
                        found = True
            if not found:
                raise DependencyMissing("Namespace does not exist and no matching Namespace YAML was selected")
        # Manual ConfigMap is namespace-wide; never mutate a namespace with another job.
        jobs = json.loads(self.kubectl("get", "elasticjobs", "-o", "json"))["items"]
        if jobs or self.pods():
            raise RuntimeError("Namespace contains ElasticJobs or leftover test Pods; archive/clean them first")
        for filename in self.plan["infrastructure"]:
            self.kubectl("apply", "-f", str(self.out / filename))
        self.kubectl("apply", "-f", str(self.out / "idle.yaml"))
        self.kubectl("create", "-f", str(self.out / "job.yaml"))
        self.created = True
        job = json.loads(self.kubectl("get", "elasticjob", self.plan["job_name"], "-o", "json"))
        self.uid = job["metadata"]["uid"]
        dump(self.out / "job.json", job)
        self.hook("prepare_adapter", self.case["id"].startswith("E"))
        self.master = self.wait("等待 Master Running", lambda: next(
            (p["metadata"]["name"] for p in self.live("dlrover-master")), None))
        expected = self.plan["expected_env"]
        actual = json.loads(self.remote("import json,os,sys; print(json.dumps({k:os.getenv(k) for k in sys.argv[1:]}))", *expected))
        dump(self.out / "master-env.json", actual)
        if actual != expected:
            raise AssertionError("Master environment differs from rendered plan; check envFrom/explicit env")
        if not self.case["id"].startswith("E"):
            url = self.config.get("fixture_scenario_url", "http://benefit-fixture:8080/scenario")
            self.wait("等待 fixture ConfigMap 投影生效", lambda: json.loads(self.remote(
                "import sys\nfrom urllib.request import urlopen\ntry: print(urlopen(sys.argv[1],timeout=3).read().decode())\nexcept Exception: print('null')", url)) == self.case["fixture"], 300)
        baseline = self.progress(self.case["initial_gpus"])
        dump(self.out / "worker-before.json", baseline)
        wait = float(expected["DLROVER_SWANLAB_WARMUP_SECONDS"]) + 65
        self.stable_delay(wait, baseline)
        key = self.case["id"]
        if key not in ("S01", "G22"):
            if key.startswith("E"):
                self.generate(100000, 100060, label="baseline")
            elif key == "S05":
                self.generate(150, 160, window=10)
            elif key == "S06":
                # Deterministic pre-generation timestamps: never rely on startup speed.
                # The Pod creation time predates this generation's rendezvous.
                stamp = datetime.fromisoformat(baseline["metadata"]["creationTimestamp"].replace("Z", "+00:00")).timestamp() - 1
                self.generate(now=stamp)
            else:
                self.generate()
        if key == "S04":
            self.stable_delay(65, baseline)
        self.kubectl("apply", "-f", str(self.out / "trigger.yaml"))
        self.triggered = True
        gate = self.wait("等待收益门禁结果", lambda: next(
            (r for r in self.flow() if r.get("event") == "gate_decision"), None))
        self.plan_id = gate["plan_id"]
        dump(self.out / "gate.json", gate)
        for field in ("decision", "status", "reason"):
            if self.case.get(field) is not None and gate.get(field) != self.case[field]:
                raise AssertionError(f"{field}: expected {self.case[field]}, got {gate.get(field)}")
        if gate["decision"] == "reject":
            self.verify_rejection(baseline)
            if key.startswith("E"):
                self.verify_adapter()
        elif gate["decision"] == "explore":
            self.explore()
        else:
            self.wait("等待替换完成并恢复训练", lambda: any(
                e.get("event_type") == "training_recovered" and e.get("plan_id") == self.plan_id for e in self.events()))
            current = self.progress(self.case["target_gpus"])
            if self.signature(current)[0] == self.signature(baseline)[0]:
                raise AssertionError("Expected worker replacement, UID did not change")
            dump(self.out / "worker-after.json", current)
        if key == "G22":
            if any(r.get("plan_id") == self.plan_id and r.get("event") in
                   ("fetch_started", "fetch_failed", "window_selected", "request_started") for r in self.flow()):
                raise AssertionError("Off mode unexpectedly fetched benefit/training evidence")
            fixture = self.kubectl("logs", self.config.get("fixture_log_target", "deployment/benefit-fixture"),
                                   "-c", self.config.get("fixture_container", "server"), "--timestamps")
            (self.out / "fixture.log").write_text(fixture)
            if self.plan_id in fixture:
                raise AssertionError("Off mode sent a request to the benefit fixture")
        self.kubectl("apply", "-f", str(self.out / "idle.yaml"))

    def verify_rejection(self, baseline):
        self.wait("等待 skipped 终态", lambda: any(
            e.get("event_type") == "finished" and e.get("plan_id") == self.plan_id
            and e.get("execution_status") == "skipped" for e in self.events()))
        self.stable_delay(float(self.config.get("reject_observe_seconds", 30)), baseline)
        current = self.progress(self.case["initial_gpus"])
        if self.signature(current) != self.signature(baseline):
            raise AssertionError("Rejected plan restarted/replaced original worker")
        if not any(p["metadata"]["name"] == self.master for p in self.live("dlrover-master")):
            raise AssertionError("Master stopped after rejecting the plan")
        if any(r.get("plan_id") == self.plan_id and r.get("event") == "execution_started" for r in self.flow()):
            raise AssertionError("Rejected plan entered checkpoint/replacement execution")
        if any(e.get("plan_id") == self.plan_id and e.get("phase") in
               ("save_checkpoint", "replacing_workers") for e in self.events()):
            raise AssertionError("Rejected plan entered an execution phase")
        dump(self.out / "worker-after.json", current)

    def archive(self):
        if not self.created:
            return
        errors = []
        for filename, args in (("pods.json", ("get", "pods", "-l", f"{JOB_LABEL}={self.plan['job_name']}", "-o", "json")),
                               ("events.json", ("get", "events", "-o", "json"))):
            try:
                (self.out / filename).write_text(self.kubectl(*args))
            except Exception as error:
                errors.append(str(error))
        try:
            # Stop further manual triggers even after a failed assertion. Keep the job for diagnosis.
            job = json.loads(self.kubectl("get", "elasticjob", self.plan["job_name"], "-o", "json"))
            if job["metadata"]["uid"] == self.uid:
                self.kubectl("apply", "-f", str(self.out / "idle.yaml"))
            if self.master:
                (self.out / "master.log").write_text(self.logs())
            for pod in self.pods("worker"):
                name = pod["metadata"]["name"]
                (self.out / f"{name}.log").write_text(self.logs(name, True))
            if not self.case["id"].startswith("E"):
                fixture_logs = self.kubectl("logs", self.config.get("fixture_log_target", "deployment/benefit-fixture"),
                                            "-c", self.config.get("fixture_container", "server"), "--timestamps")
                (self.out / "fixture.log").write_text(fixture_logs)
        except Exception as error:
            errors.append(str(error))
        if errors:
            dump(self.out / "archive-errors.json", errors)

    def cleanup(self):
        if not self.created or not self.uid:
            return
        job = json.loads(self.kubectl("get", "elasticjob", self.plan["job_name"], "-o", "json"))
        if job["metadata"]["uid"] != self.uid:
            raise RuntimeError("Refusing cleanup: job UID changed")
        self.kubectl("apply", "-f", str(self.out / "idle.yaml"))
        self.kubectl("delete", "elasticjob", self.plan["job_name"], "--wait=true", "--timeout=30s")
        self.wait("等待本用例全部 Pod 清理", lambda: not json.loads(self.kubectl(
            "get", "pods", "-l", f"{JOB_LABEL}={self.plan['job_name']}", "-o", "json"))["items"], 300)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("extract", "list", "render", "run"))
    parser.add_argument("--config", type=Path)
    parser.add_argument("--cases", default="G01", help="Comma-separated IDs, G, S, E, or all")
    parser.add_argument("--output", type=Path, default=Path("/tmp/benefit-k8s-automation"))
    parser.add_argument("--cleanup", action="store_true", help="Delete only this run's job after archiving; retain PVC/journals")
    parser.add_argument("--continue-on-failure", action="store_true")
    args = parser.parse_args(argv)
    if args.command == "extract":
        extract(args.output)
        return 0
    if args.command == "list":
        for key, case in cases().items():
            print(f"{key}  {case['mode']}/{case['objective']}  {case['initial_gpus']}→{case['target_gpus']}  {case['decision']}  {case['reason'] or case['status']}")
        return 0
    if not args.config:
        parser.error("--config is required")
    catalog = cases()
    selected = []
    for token in args.cases.split(","):
        matches = [key for key in catalog if token == "all" or key == token or key.startswith(token) and token in ("G", "S", "E")]
        if not matches:
            parser.error(f"Unknown scenario: {token}")
        selected.extend(key for key in matches if key not in selected)
    if args.command == "run" and len(selected) > 1 and not args.cleanup:
        parser.error("Multiple cases require --cleanup so every case has a fresh job UID")
    results = []
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    for key in selected:
        config = configuration(args.config, key)
        case = cases(int(config.get("total_steps", 3760)))[key]
        out = args.output.resolve() / f"{run_id}-{key.lower()}"
        runner = None
        result = dict(case=key, result="RENDERED", artifacts=str(out))
        try:
            plan = render(config, case, out)
            if args.command == "run":
                runner = Runner(config, plan, out)
                runner.run()
                result["result"] = "PASS"
        except DependencyMissing as error:
            result.update(result="NOT_RUN", error=str(error))
        except (Exception, KeyboardInterrupt) as error:
            result.update(result="FAIL", error=str(error) or "Interrupted")
        finally:
            if runner:
                runner.archive()
                if args.cleanup:
                    try:
                        runner.cleanup()
                    except Exception as error:
                        result.update(result="FAIL", cleanup_error=str(error))
            out.mkdir(parents=True, exist_ok=True)
            dump(out / "result.json", result)
            results.append(result)
            print(json.dumps(result, ensure_ascii=False), flush=True)
        if result["result"] in ("FAIL", "NOT_RUN") and not args.continue_on_failure:
            break
    args.output.mkdir(parents=True, exist_ok=True)
    dump(args.output / f"{run_id}-summary.json", results)
    return int(any(r["result"] in ("FAIL", "NOT_RUN") for r in results))


if __name__ == "__main__":
    sys.exit(main())
