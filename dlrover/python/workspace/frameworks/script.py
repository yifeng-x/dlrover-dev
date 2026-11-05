# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

"""A deliberately restricted shell reader; never executes user input."""

import os
import re
import shlex
from dataclasses import dataclass
from pathlib import Path

_NAME = re.compile(r"[A-Za-z_][A-Za-z_0-9]*")
_ASSIGN = re.compile(r"^([A-Za-z_][A-Za-z_0-9]*)=(.*)$", re.S)


@dataclass
class Command:
    argv: list
    environment: dict
    cwd: str
    source: str
    line: int


def _words(text, variables):
    words, word, quote, started = [], "", "", False
    i = 0
    while i < len(text):
        c = text[i]
        if not quote and c.isspace():
            if started:
                words.append(word)
            word, started = "", False
        elif not quote and c == "#" and not started:
            break
        elif c in "\"'" and (not quote or quote == c):
            quote = "" if quote else c
            started = True
        elif c == "\\" and quote != "'":
            i += 1
            if i == len(text):
                raise ValueError("dangling escape")
            if quote == '"' and text[i] not in '$`"\\':
                word += "\\"
            word += text[i]
            started = True
        elif c == "$" and quote != "'":
            j = i + 1
            braced = j < len(text) and text[j] == "{"
            if braced:
                j += 1
            match = _NAME.match(text, j)
            if not match:
                raise ValueError(
                    "only constant variable references are supported"
                )
            name = match.group()
            j = match.end()
            if braced:
                if j >= len(text) or text[j] != "}":
                    raise ValueError(
                        "shell parameter expressions are unsupported"
                    )
                j += 1
            if name not in variables:
                raise ValueError("unresolved variable: " + name)
            value = variables[name]
            if not quote and (
                any(c.isspace() for c in value)
                or any(c in value for c in "*?[")
            ):
                raise ValueError("quote variable reference: " + name)
            word += value
            started, i = True, j - 1
        elif quote != "'" and c == "`":
            raise ValueError("command substitution is unsupported")
        elif not quote and c in ";&|<>()*?[]{}~":
            raise ValueError("unsupported shell operator: " + c)
        else:
            word += c
            started = True
        i += 1
    if quote:
        raise ValueError("unclosed quote")
    if started:
        words.append(word)
    return words


def parse_input(value):
    source = "<framework-cmd>"
    parts = shlex.split(value)
    if parts and parts[0] == "bash":
        if len(parts) != 2:
            raise ValueError("script input must be exactly: bash PATH")
        path = Path(parts[1]).resolve()
        source, value = str(path), path.read_text()
    variables, exported, commands = {}, {}, []
    cwd = ""
    pending, start = "", 1
    for number, raw in enumerate(value.splitlines(), 1):
        if not pending:
            start = number
        # A continuation keeps the original logical command's source line.
        if raw.endswith("\\"):
            pending += raw[:-1]
            continue
        text = pending + raw
        pending = ""
        try:
            if re.search(r"&(?:amp|lt|gt|quot);", text):
                raise ValueError("HTML-escaped shell input")
            words = _words(text, variables)
            if not words:
                continue
            if words[0] == "export":
                for item in words[1:]:
                    match = _ASSIGN.fullmatch(item)
                    if match:
                        variables[match[1]] = match[2]
                        exported[match[1]] = match[2]
                    elif _NAME.fullmatch(item) and item in variables:
                        exported[item] = variables[item]
                    else:
                        raise ValueError("export requires a known constant")
                continue
            if words[0] == "cd":
                if len(words) != 2 or not os.path.isabs(words[1]):
                    raise ValueError("cd requires one absolute worker path")
                cwd = words[1]
                continue
            prefix = {}
            while words and _ASSIGN.fullmatch(words[0]):
                match = _ASSIGN.fullmatch(words.pop(0))
                prefix[match[1]] = match[2]
            if not words:
                variables.update(prefix)
                for key in prefix.keys() & exported.keys():
                    exported[key] = prefix[key]
                continue
            commands.append(
                Command(words, dict(exported, **prefix), cwd, source, start)
            )
        except ValueError as error:
            raise ValueError("%s:%s: %s" % (source, start, error)) from error
    if pending:
        raise ValueError("%s:%s: dangling continuation" % (source, start))
    if not commands:
        raise ValueError("no framework command found")
    return commands
