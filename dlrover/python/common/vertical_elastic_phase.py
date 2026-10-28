import json
from datetime import datetime, timedelta
from threading import Lock
from typing import (
    Dict, Any, List, Optional, Union, TypedDict, ClassVar
)
from dataclasses import dataclass, asdict
from dlrover.python.common.constants import VerticalElasticPhaseType

@dataclass
class PhaseMetadata:
    """阶段元数据"""
    triggered_by: str = ""
    reason: str = ""
    created_at: str = ""
    updated_at: str = ""

    def __post_init__(self):
        now = datetime.now().isoformat()
        self.created_at = self.created_at or now
        self.updated_at = self.updated_at or now

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PhaseMetadata":
        return cls(**data)
    
class StateMachineData(TypedDict):
    current_state: str
    expected_next_phase: Optional[str]

class PhaseInfo(TypedDict):
    ready: bool
    phase: str
    plan_id: Optional[str]
    adjust_gpus: int
    state_machine: StateMachineData
    metadata: Dict[str, Any]

class PlanIdGenerator:
    def __init__(self):
        self.last_date = None
        self.counter = 0
    
    def generate_id(self):
        current_date = datetime.now().strftime("%Y%m%d")
        
        # 如果是新的一天，重置计数器
        if current_date != self.last_date:
            self.last_date = current_date
            self.counter = 0
        
        self.counter += 1
        
        # 格式化序号为3位数，前面补零
        sequence_num = f"{self.counter:03d}"
        
        return f"vertical_elastic_plan_{current_date}_{sequence_num}"

class VerticalElasticPhase:
    """
    vertical elastic phase manager
    """

    # 有效的阶段转换图
    VALID_TRANSITIONS: ClassVar[Dict[str, List[str]]] = {
        VerticalElasticPhaseType.IDLE: [
            VerticalElasticPhaseType.PREPARING
        ],
        VerticalElasticPhaseType.PREPARING: [
            VerticalElasticPhaseType.SAVE_CHECKPOINT,
            VerticalElasticPhaseType.FAILED
        ],
        VerticalElasticPhaseType.SAVE_CHECKPOINT: [
            VerticalElasticPhaseType.REPLACING_WORKERS,
            VerticalElasticPhaseType.FAILED
        ],
        VerticalElasticPhaseType.REPLACING_WORKERS: [
            VerticalElasticPhaseType.TUNE_PARAM,
            VerticalElasticPhaseType.FAILED
        ],
        VerticalElasticPhaseType.TUNE_PARAM: [
            VerticalElasticPhaseType.RENDEZVOUS_READY,
            VerticalElasticPhaseType.FAILED
        ],
        VerticalElasticPhaseType.RENDEZVOUS_READY: [
            VerticalElasticPhaseType.COMPLETED,
            VerticalElasticPhaseType.FAILED
        ],
        VerticalElasticPhaseType.FAILED: [
            VerticalElasticPhaseType.IDLE
        ],
        VerticalElasticPhaseType.COMPLETED: [
            VerticalElasticPhaseType.IDLE
        ]
    }

    # 只读字段常量
    READONLY_FIELDS: ClassVar[List[str]] = ["plan_id", "metadata.created_at"]

    def __init__(self):
        self._lock = Lock()
        self._phase_info: PhaseInfo = self._get_default_phase_info()

    @staticmethod
    def _get_default_phase_info() -> PhaseInfo:
        return PhaseInfo(
            ready=False,
            phase=VerticalElasticPhaseType.IDLE,
            plan_id=None,
            adjust_gpus=0,
            state_machine=StateMachineData(
                current_state=VerticalElasticPhaseType.IDLE,
                expected_next_phase=None,
            ),
            metadata=PhaseMetadata().to_dict()
        )

    def reset(self) -> None:
        """reset the phase info to idle"""
        with self._lock:
            self._phase_info = self._get_default_phase_info()

    def create(
        self,
        plan_id: str,
        adjust_gpus: int = 0,
        reason: str = "vertical_elastic_scaling",
        triggered_by: str = "brain_scheduler",
    ) -> PhaseInfo:
        """
        创建新的垂直弹性调整计划
        线程安全，自动初始化状态机与元数据
        """

        with self._lock:
            # init metadata
            metadata = PhaseMetadata(triggered_by=triggered_by, reason=reason)

            self._phase_info = PhaseInfo(
                ready=True,
                phase=VerticalElasticPhaseType.PREPARING,
                plan_id=plan_id,
                adjust_gpus=adjust_gpus,
                state_machine=StateMachineData(
                    current_state=VerticalElasticPhaseType.PREPARING,
                    expected_next_phase=VerticalElasticPhaseType.SAVE_CHECKPOINT,
                ),
                metadata=metadata.to_dict()
            )
            return self._phase_info.copy()

    
    def get(self) -> PhaseInfo:
        with self._lock:
            return self._phase_info.copy()

    def build_phase_update(
        self,
        adjust_gpus: Optional[int] = None,
        ready: Optional[bool] = None,
        phase: Optional[str] = None,
        expected_next_phase: Optional[str] = None,
        reason: Optional[str] = None,
        triggered_by: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """build phase update dict"""
        updates = {}
        if adjust_gpus is not None:
            updates["adjust_gpus"] = adjust_gpus
        if ready is not None:
            updates["ready"] = ready

        state_machine_updates = {}
        if phase is not None:
            updates["phase"] = phase
            state_machine_updates["current_state"] = phase
        if expected_next_phase is not None:
            state_machine_updates["expected_next_phase"] = expected_next_phase
        if state_machine_updates:
            updates["state_machine"] = state_machine_updates

        metadata_updates = {}
        if reason is not None:
            metadata_updates["reason"] = reason
        if triggered_by is not None:
            metadata_updates["triggered_by"] = triggered_by
        if metadata_updates:
            updates["metadata"] = metadata_updates

        extra = {k: v for k, v in kwargs.items() if k not in ["plan_id"]}
        updates.update(extra)
        return updates

    def is_ready(self) -> bool:
        with self._lock:
            return self._phase_info["ready"]

    def get_phase(self) -> str:
        with self._lock:
            return self._phase_info["phase"]
        
    def get_plan_id(self) -> Optional[str]:
        with self._lock:
            return self._phase_info["plan_id"]

    def update(self, updates: Dict[str, Any], validate: bool = True) -> PhaseInfo:
        with self._lock:
            if validate:
                self._validate_update(updates)
            self._deep_update(self._phase_info, updates)
            # update timestamp
            self._phase_info["metadata"]["updated_at"] = datetime.now().isoformat()
            return self._phase_info.copy()

    def _deep_update(self, base: Dict[str, Any], updates: Dict[str, Any]) -> None:
        """递归深度更新字典，无冗余拷贝"""
        for key, value in updates.items():
            if isinstance(base.get(key), dict) and isinstance(value, dict):
                self._deep_update(base[key], value)
            else:
                base[key] = value

    def _validate_update(self, updates: Dict[str, Any], root_path: str = "") -> None:
        for key, value in updates.items():
            current_path = f"{root_path}.{key}" if root_path else key
            if current_path in self.READONLY_FIELDS:
                raise ValueError(f"Field '{current_path}' is read-only")
            if isinstance(value, dict):
                self._validate_update(value, current_path)
    
    def to_dict(self) -> PhaseInfo:
        return self.get()

    def to_json(self, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "VerticalElasticPhase":
        instance = cls()
        instance._phase_info = data.copy()
        return instance

    @classmethod
    def from_json(cls, json_str: str) -> "VerticalElasticPhase":
        return cls.from_dict(json.loads(json_str))

    def complete(self, success: bool = True) -> None:
        with self._lock:
            target = VerticalElasticPhaseType.COMPLETED if success else VerticalElasticPhaseType.FAILED
            current = self._phase_info["phase"]
            if target not in self.VALID_TRANSITIONS.get(current, []):
                target = VerticalElasticPhaseType.FAILED
            self._phase_info["phase"] = target
            self._phase_info["state_machine"]["current_state"] = target
            self._phase_info["ready"] = False
            self._phase_info["metadata"]["updated_at"] = datetime.now().isoformat()

    def fail(self, reason: str = "") -> None:
        """Tag failed and set reason"""
        with self._lock:
            self._phase_info["phase"] = VerticalElasticPhaseType.FAILED
            self._phase_info["state_machine"]["current_state"] = VerticalElasticPhaseType.FAILED
            if reason:
                self._phase_info["metadata"]["reason"] = f"FAILED: {reason}"
            self._phase_info["ready"] = False
            self._phase_info["metadata"]["updated_at"] = datetime.now().isoformat()

    def __repr__(self) -> str:
        info = self.get()
        return f"VerticalElasticPhase(phase={info['phase']!r}, plan_id={info['plan_id']!r}, ready={info['ready']})"
