# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

"""Explicit framework adapters. Importing workspace never imports a framework."""


def get_adapter(name, commands):
    from .swift import SwiftAdapter

    adapters = {"swift": SwiftAdapter}
    if name == "auto":
        matches = [cls for cls in adapters.values() if cls.detect(commands)]
        if len(matches) != 1:
            raise ValueError(
                "Cannot unambiguously detect a supported framework"
            )
        return matches[0]()
    if name not in adapters:
        raise ValueError("Unsupported workspace framework: %s" % name)
    return adapters[name]()
