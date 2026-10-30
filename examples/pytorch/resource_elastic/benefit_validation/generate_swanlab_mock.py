"""Generate a scalar response fixture for local benefit-flow validation."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--now", type=float, default=None)
    parser.add_argument("--window-seconds", type=int, default=60)
    parser.add_argument("--work-start", type=int, default=100)
    parser.add_argument("--work-end", type=int, default=160)
    parser.add_argument("--total-steps", type=int, default=10000)
    parser.add_argument("--global-batch-size", type=int, default=64)
    parser.add_argument("--assume-fixed-batch", action="store_true")
    args = parser.parse_args()
    now = time.time() if args.now is None else args.now
    if (args.window_seconds <= 0
            or not now > args.window_seconds
            or not 0 <= args.work_start < args.work_end <= args.total_steps < 2**53
            or not 0 < args.global_batch_size < 2**53):
        parser.error("Invalid window, counters or batch size")

    times = [now - args.window_seconds, now - args.window_seconds / 2, now]
    work = [args.work_start, (args.work_start + args.work_end) // 2, args.work_end]
    series = {
        "train/global_step": work,
        "train/total_steps": [args.total_steps] * 3,
        "train/global_batch_size": [args.global_batch_size] * 3,
    }
    if args.assume_fixed_batch:
        series = {
            "global_step": work,
            "max_steps": [args.total_steps] * 3,
            "train_speed": [(args.work_end - args.work_start) / args.window_seconds] * 3,
        }

    data = [
        dict(
            taskId=args.task_id, 
            key=key, 
            scalarMetricsList=[
                dict(
                    epoch=index, 
                    createTime=datetime.fromtimestamp(stamp, timezone.utc).isoformat(),
                    data=str(values[index])
                ) for index, stamp in enumerate(times)
            ]
        ) for key, values in series.items()]
    
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=args.output.parent, prefix="swanlab-")
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(data, stream, indent=2, allow_nan=False)
        os.replace(temporary, args.output)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    print(f"Mock scalar response written to {args.output}")


if __name__ == "__main__":
    main()
