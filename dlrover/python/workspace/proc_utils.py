# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import os
import signal
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional


@dataclass
class ProcessInfo:
    pid: int # 进程ID（唯一标识）
    ppid: int # 父进程ID
    pgid: int # 进程组ID
    stat: str # 进程状态（如运行、睡眠、僵尸等）
    cmd: str # 进程命令行


def _read_text(path: str) -> str:
    with open(path, "r", errors="replace") as fp:
        return fp.read()


def _read_bytes(path: str) -> bytes:
    with open(path, "rb") as fp:
        return fp.read()


def get_process_info(pid: int) -> Optional[ProcessInfo]:
    try:
        stat_text = _read_text("/proc/%d/stat" % pid)
        _, after_comm = stat_text.rsplit(") ", 1)
        parts = after_comm.split()
        stat = parts[0]
        ppid = int(parts[1])
        pgid = int(parts[2])
        cmdline = _read_bytes("/proc/%d/cmdline" % pid)
        cmd = cmdline.replace(b"\0", b" ").decode("utf-8", "replace").strip()
        if not cmd:
            comm = stat_text.split("(", 1)[1].rsplit(")", 1)[0]
            cmd = "[%s]" % comm
        return ProcessInfo(pid=pid, ppid=ppid, pgid=pgid, stat=stat, cmd=cmd)
    except Exception:
        return None


def is_pid_alive(pid: Optional[int]) -> bool:
    if not pid:
        return False
    info = get_process_info(int(pid))
    return info is not None and info.stat != "Z"


def is_pgid_alive(pgid: Optional[int]) -> bool:
    if not pgid:
        return False
    try:
        pgid = int(pgid)
    except (TypeError, ValueError):
        return False
    for pid in list_pids():
        info = get_process_info(pid)
        if info and info.pgid == pgid and info.stat != "Z":
            return True
    return False


def list_pids() -> List[int]:
    pids = []
    try:
        names = os.listdir("/proc")
    except Exception:
        return pids
    for name in names:
        if name.isdigit():
            pids.append(int(name))
    return pids


def read_environ(pid: int) -> Dict[str, str]:
    try:
        raw = _read_bytes("/proc/%d/environ" % pid)
    except Exception:
        return {}
    env = {}
    for item in raw.split(b"\0"):
        if not item or b"=" not in item:
            continue
        key, value = item.split(b"=", 1)
        env[key.decode("utf-8", "replace")] = value.decode(
            "utf-8", "replace"
        )
    return env


def find_pids_by_launch_id(
    launch_id: str, training_workers_only: bool = False
) -> List[int]:
    if not launch_id:
        return []
    pids = []
    current_pid = os.getpid()
    for pid in list_pids():
        if pid == current_pid:
            continue
        info = get_process_info(pid)
        if not info or info.stat == "Z":
            continue
        env = read_environ(pid)
        if env.get("DLROVER_WORKSPACE_LAUNCH_ID") != launch_id:
            continue
        if training_workers_only and not (
            env.get("TORCHELASTIC_RUN_ID")
            and env.get("MASTER_ADDR")
            and env.get("MASTER_PORT")
        ):
            continue
        pids.append(pid)
    return sorted(pids)


def signal_pids(pids: Iterable[int], sig: signal.Signals):
    current_pid = os.getpid()
    for pid in sorted(set(int(pid) for pid in pids)):
        if pid <= 0 or pid == current_pid:
            continue
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            continue
        except PermissionError:
            continue


def signal_pgid(pgid: Optional[int], sig: signal.Signals) -> bool:
    if not pgid:
        return False
    try:
        pgid = int(pgid)
    except (TypeError, ValueError):
        return False
    if pgid <= 0 or pgid == os.getpgrp():
        return False
    try:
        os.killpg(pgid, sig)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return False
