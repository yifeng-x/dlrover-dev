"""Offline tests: document templates, actual benefit evaluator, runner guards."""

import io
import json
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import yaml

import run_k8s_cases as automation
from k8s_cases import cases
from dlrover.python.master.resource.benefit import evaluate
from dlrover.python.master.resource.swanlab import SwanLabTrainingMetrics, TrainingMetricsUnavailable
from dlrover.python.tests.test_vertical_benefit import payload, request_for


@pytest.fixture
def config(tmp_path):
    templates = tmp_path / "templates"
    automation.extract(templates)
    return dict(base_dir=str(tmp_path), manifests_dir="templates", namespace="case-test",
                job_name="test-job", step_regex=r"global_step=(\d+)",
                infrastructure=["00-benefit-env.yaml", "00-fixture-scenario.yaml"])


@pytest.mark.parametrize("case_id", list(cases()))
def test_every_case_renders_without_cluster_access(config, tmp_path, case_id, monkeypatch):
    monkeypatch.setattr(automation.subprocess, "run", Mock(side_effect=AssertionError("cluster access")))
    case = cases()[case_id]
    out = tmp_path / case_id
    plan = automation.render(config, case, out)
    job = yaml.safe_load((out / "job.yaml").read_text())
    assert job["metadata"]["namespace"] == "case-test"
    assert job["metadata"]["name"] == "test-job"
    worker = job["spec"]["replicaSpecs"]["worker"]["template"]["spec"]["containers"][0]
    assert worker["resources"]["limits"]["nvidia.com/gpu"] == case["initial_gpus"]
    assert f"--nproc_per_node={case['initial_gpus']}" in worker["args"][0]
    assert f"/output/benefit-mock/{case_id}" in worker["args"][0]
    assert len(plan["infrastructure"]) == 2
    assert plan["expected_env"]["DLROVER_BENEFIT_OBJECTIVE"] == case["objective"]
    assert ("low_code_platform_task_id" in job["metadata"]["labels"]) is (case_id != "S03")


def test_yaml_selection_override_empty_and_custom_files(config, tmp_path):
    custom = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "extra", "namespace": "custom"}}
    (tmp_path / "extra.yaml").write_text(yaml.safe_dump(custom))
    settings = dict(config, cases={"G01": {"infrastructure": []},
                                 "G02": {"infrastructure": ["extra.yaml", "00-benefit-env.yaml"]}})
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(settings))
    one = automation.configuration(path, "G01")
    assert automation.render(one, cases()["G01"], tmp_path / "one")["infrastructure"] == []
    two = automation.configuration(path, "G02")
    plan = automation.render(two, cases()["G02"], tmp_path / "two")
    assert yaml.safe_load((tmp_path / "two" / plan["infrastructure"][0]).read_text()) == custom
    assert automation.configuration(path, "S01")["infrastructure"] == config["infrastructure"]


def test_template_replacement_is_scenario_aware(config, tmp_path):
    env = yaml.safe_load((tmp_path / "templates/00-benefit-env.yaml").read_text())
    env["data"]["DLROVER_BENEFIT_MIN_GAIN_RATIO"] = "0.1"
    (tmp_path / "custom-env.yaml").write_text(yaml.safe_dump(env))
    config["templates"] = {"00-benefit-env.yaml": "custom-env.yaml"}
    plan = automation.render(config, cases()["G04"], tmp_path / "out")
    assert plan["expected_env"]["DLROVER_BENEFIT_MIN_GAIN_RATIO"] == "0.1"
    assert plan["expected_env"]["DLROVER_BENEFIT_OBJECTIVE"] == "gpu_hours"


def test_extract_never_overwrites_customized_templates(config, tmp_path):
    with pytest.raises(ValueError, match="overwrite"):
        automation.extract(tmp_path / "templates")


@pytest.mark.parametrize("total", [3760, 1044])
@pytest.mark.parametrize("case_id", [key for key in cases() if key.startswith("G") and key not in ("G18", "G19", "G22")])
def test_document_fixture_matches_actual_evaluator(config, tmp_path, total, case_id):
    case = cases(total)[case_id]
    fixture = yaml.safe_load((tmp_path / "templates/00-fixture-code.yaml").read_text())["data"]["server.py"]
    # Load the actual documented request handler without opening any socket.
    namespace = {}
    exec(fixture.rsplit("ThreadingHTTPServer((", 1)[0], namespace)
    namespace["scenario"] = lambda: case["fixture"]
    namespace["time"] = SimpleNamespace(time=lambda: 1000, sleep=lambda _: None)
    request = request_for()
    request["current"]["gpus_per_worker"] = case["initial_gpus"]
    request["target"]["gpus_per_worker"] = case["target_gpus"]
    request["plan_id"] = "plan"
    body = json.dumps(request).encode()
    handler = object.__new__(namespace["Handler"])
    handler.path = "/benefit/snapshot"
    handler.rfile = io.BytesIO(body)
    handler.headers = {"Content-Length": str(len(body))}
    response = []
    handler.send_json = lambda status, data: response.append((status, data))
    handler.do_POST()
    status, snapshot = response[0]
    assert status == 200
    training = payload(request)["training"]
    training["start"].update(work=100, observed_at=940)
    training["end"].update(work=160, observed_at=1000)
    training["target_work"] = total
    snapshot["training"] = {**training, **snapshot["training"]}
    result = evaluate(snapshot, request, 1000, max_age=600, objective=case["objective"])
    assert result.status == case["status"]
    if case["reason"]:
        assert result.reason == case["reason"]


def test_missing_exploration_dependencies_never_calls_kubectl(config, tmp_path, monkeypatch):
    # Remove irrelevant image placeholders to exercise the adapter prerequisite.
    for path in (tmp_path / "templates").glob("*.yaml"):
        path.write_text(path.read_text().replace("REPLACE_", "configured-"))
    plan = automation.render(config, cases()["E01"], tmp_path / "run")
    runner = automation.Runner(config, plan, tmp_path / "run")
    runner.kubectl = Mock(side_effect=AssertionError("Must not mutate cluster"))
    with pytest.raises(automation.DependencyMissing, match="adapter_url"):
        runner.run()
    runner.kubectl.assert_not_called()


def test_records_and_steps_ignore_other_log_content():
    assert automation.records('noise\nINFO BenefitFlow {"event":"gate_decision"}\nINFO BenefitFlow broken', "BenefitFlow ") == [{"event": "gate_decision"}]
    assert automation.step_values("max_steps=100\nglobal_step=1\nglobal_step=2", r"global_step=(\d+)") == [1, 2]


@pytest.fixture
def runner(config, tmp_path):
    out = tmp_path / "run"
    plan = automation.render(config, cases()["G02"], out)
    obj = automation.Runner(config, plan, out)
    obj.plan_id = "plan"
    obj.uid = "job-uid"
    obj.master = "master"
    obj.created = True
    return obj


def pod(uid="worker", restarts=0):
    return dict(metadata=dict(uid=uid), status=dict(containerStatuses=[dict(name="main", restartCount=restarts)]))


@pytest.mark.parametrize("change", ["uid", "restart", "execution", "checkpoint", "none"])
def test_rejection_requires_worker_preservation_and_no_execution(runner, change):
    baseline = pod()
    current = pod("other" if change == "uid" else "worker", int(change == "restart"))
    events = [dict(event_type="finished", plan_id="plan", execution_status="skipped")]
    if change == "checkpoint":
        events.append(dict(plan_id="plan", phase="save_checkpoint"))
    runner.events = lambda: events
    runner.flow = lambda: ([dict(plan_id="plan", event="execution_started")] if change == "execution" else [])
    runner.progress = lambda _: current
    runner.live = lambda _: [dict(metadata=dict(name="master"))]
    runner.stable_delay = lambda *_: None
    if change == "none":
        runner.verify_rejection(baseline)
    else:
        with pytest.raises(AssertionError):
            runner.verify_rejection(baseline)


def test_cleanup_cannot_delete_another_job_uid(runner):
    runner.kubectl = Mock(return_value=json.dumps(dict(metadata=dict(uid="other"))))
    with pytest.raises(RuntimeError, match="UID changed"):
        runner.cleanup()
    assert runner.kubectl.call_count == 1


def test_fault_hook_exit_zero_without_evidence_is_not_pass(runner):
    runner.case = cases()["E09"]
    runner.hook = Mock()
    report = dict(case="E09", job_uid=runner.uid, plan_id=runner.plan_id,
                  checks=dict(fenced=True, no_mutation_replayed=True, checkpoint_resources_checked=True))
    automation.dump(runner.out / "adapter-verification.json", report)
    with pytest.raises(AssertionError, match="evidence"):
        runner.verify_adapter()


def test_multiple_cases_require_explicit_cleanup(config, tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(config))
    with pytest.raises(SystemExit):
        automation.main(["run", "--config", str(path), "--cases", "G01,G02"])


@pytest.mark.parametrize("key", [f"S{i:02}" for i in range(1, 10)])
def test_scalar_faults_use_real_generator_and_swanlab_validator(config, tmp_path, key):
    case = cases()[key]
    out = tmp_path / "run"
    plan = automation.render(config, case, out)
    env = plan["expected_env"]
    env["DLROVER_SWANLAB_MOCK_FILE"] = str(tmp_path / "scalar.json")
    env["LOW_CODE_PLATFORM_TASK_ID"] = "" if key == "S03" else "123"
    runner = automation.Runner(config, plan, out)
    runner.remote = lambda code, *args: subprocess.run(
        [sys.executable, "-", *map(str, args)], input=code, text=True,
        capture_output=True, check=True).stdout
    if key != "S01":
        runner.generate(start=150 if key == "S05" else 100,
                        window=10 if key == "S05" else 60,
                        now=1700 if key == "S06" else 2000)
    client = SwanLabTrainingMetrics(env)
    with pytest.raises(TrainingMetricsUnavailable, match=case["reason"]):
        client.fetch("generation", lambda: 2065 if key == "S04" else 2000,
                     timeout=10, max_age=float(env["DLROVER_BENEFIT_MAX_AGE_SECONDS"]),
                     min_window=30, training_context=dict(generation="generation", started_at=1800,
                                                         recovered_at=0, recovery_ready=True))


def test_full_rejection_runner_orders_apply_and_requires_live_worker(config, tmp_path):
    for path in (tmp_path / "templates").glob("*.yaml"):
        path.write_text(path.read_text().replace("REPLACE_", "configured-"))
    out = tmp_path / "run"
    plan = automation.render(config, cases()["G02"], out)
    runner = automation.Runner(config, plan, out)
    calls = []

    def kubectl(*args, stdin=None):
        calls.append(args)
        if args[:2] == ("get", "namespace"):
            return "namespace/case-test"
        if args[:2] in (("get", "elasticjobs"), ("get", "pods")):
            return '{"items":[]}'
        if args[:2] == ("get", "elasticjob"):
            return '{"metadata":{"uid":"job-uid"}}'
        return ""

    runner.kubectl = kubectl
    runner.live = lambda _: [dict(metadata=dict(name="master"))]
    runner.remote = lambda code, *args: json.dumps(plan["expected_env"] if "os.getenv" in code else plan["case"]["fixture"])
    runner.progress = lambda _: pod()
    runner.stable_delay = lambda *_: None
    runner.generate = Mock()
    runner.flow = lambda: [dict(event="gate_decision", plan_id="plan", decision="reject",
                               status="negative", reason="insufficient_gain")]
    runner.events = lambda: [dict(event_type="finished", plan_id="plan", execution_status="skipped")]
    runner.run()
    runner.generate.assert_called_once()
    mutations = [(args[0], args[-1].split("/")[-1]) for args in calls if args[0] in ("apply", "create")]
    assert mutations == [("apply", "infra-00.yaml"), ("apply", "infra-01.yaml"),
                         ("apply", "idle.yaml"), ("create", "job.yaml"),
                         ("apply", "trigger.yaml"), ("apply", "idle.yaml")]
    assert runner.uid == "job-uid"


def test_inconclusive_waits_for_result_report(runner):
    runner.case = cases()["E04"]
    runner.journal = Mock(side_effect=[
        dict(status="OBSERVING", started_at=100),
        dict(status="INCONCLUSIVE", reason="observation_budget_exhausted", reported=False),
        dict(status="INCONCLUSIVE", reason="observation_budget_exhausted", reported=True),
    ])
    runner.remote = lambda *_: "200"
    runner.verify_adapter = Mock()
    runner.explore()
    assert runner.journal.call_count == 3
    runner.verify_adapter.assert_called_once()


def test_negative_exploration_only_submits_candidate_windows(runner):
    runner.case = cases()["E02"]
    runner.windows = Mock()
    runner.journal = Mock(return_value=dict(status="RETAINED", reported=True))
    runner.verify_adapter = Mock()
    runner.explore()
    runner.windows.assert_called_once_with("OBSERVING", 2, 30, 100060)
    runner.verify_adapter.assert_called_once()
