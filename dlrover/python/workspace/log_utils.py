# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import os
import sys
import time
from collections import deque


def print_tail(path: str, tail: int, output=None):
    output = output or sys.stdout
    tail = max(0, int(tail))
    if not os.path.exists(path):
        output.write("log file not found: %s\n" % path)
        output.flush()
        return
    lines = deque(maxlen=tail)
    with open(path, "r", errors="replace") as fp:
        for line in fp:
            lines.append(line)
    for line in lines:
        output.write(line)
    output.flush()


def follow_file(path: str, tail: int, output=None, interval: float = 1.0):
    output = output or sys.stdout
    os.makedirs(os.path.dirname(path), exist_ok=True)
    open(path, "a").close()
    print_tail(path, tail, output=output)
    with open(path, "r", errors="replace") as fp:
        fp.seek(0, os.SEEK_END)
        while True:
            line = fp.readline()
            if line:
                output.write(line)
                output.flush()
                continue
            time.sleep(interval)
