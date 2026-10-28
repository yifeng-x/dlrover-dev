# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");


class RendezvousParameterUpdateDecision(object):
    def __init__(
        self,
        should_update=False,
        should_reset=False,
        reset_reason="",
        run_id="",
    ):
        self.should_update = should_update
        self.should_reset = should_reset
        self.reset_reason = reset_reason
        self.run_id = run_id


class WorkspaceRendezvousState(object):
    def __init__(self):
        self._run_id = ""

    @property
    def run_id(self):
        return self._run_id

    def evaluate_param_update(
        self,
        initialized: bool,
        old_params,
        new_params,
        run_id="",
        workspace_mode=False,
    ):
        if not workspace_mode:
            return RendezvousParameterUpdateDecision(
                should_update=not initialized
            )

        run_changed = bool(run_id) and run_id != self._run_id
        params_changed = initialized and old_params != new_params
        should_reset = initialized and (run_changed or params_changed)
        should_update = not initialized or run_changed or params_changed
        reason = ""
        if should_reset:
            reason = (
                f"run_id {self._run_id or '<empty>'}->{run_id or '<empty>'}, "
                f"params {old_params}->{new_params}"
            )
        return RendezvousParameterUpdateDecision(
            should_update=should_update,
            should_reset=should_reset,
            reset_reason=reason,
            run_id=run_id if run_id else self._run_id,
        )

    def apply_param_update(self, decision: RendezvousParameterUpdateDecision):
        if decision.run_id:
            self._run_id = decision.run_id
