import ast

from typing import Dict, Any, Optional, Tuple
from dlrover.python.common.log import default_logger as logger


# 内存单位映射 (二进制优先)
MEMORY_UNITS = {
    'Ki': 1024,
    'Mi': 1024 ** 2,
    'Gi': 1024 ** 3,
    'Ti': 1024 ** 4,
    'K': 1000,
    'M': 1000 ** 2,
    'G': 1000 ** 3,
    'T': 1000 ** 4,
}

# GPU 特征关键词，用于快速匹配键名
GPU_KEYWORDS = ('gpu', 'ascend', 'nvidia', 'amd')
    
   

def safe_parse_resource(data: Any) -> Optional[Dict]:
    """
    安全地将字符串转换为字典。
    支持输入：Dict 对象 或 字符串 "{'key': 'value'}"
    """
    if isinstance(data, dict):
        return data
    
    if isinstance(data, str):
        data = data.strip()
        if not data:
            return None
        try:
            # ast.literal_eval 安全地解析字符串为 Python 对象
            # 它能处理单引号 {'a': 1} 和双引号 {"a": 1}
            parsed = ast.literal_eval(data)
            if isinstance(parsed, dict):
                return parsed
            else:
                logger.warning(f"Parsed resource is not a dict: {type(parsed)}")
                return None
        except (ValueError, SyntaxError) as e:
            logger.error(f"Failed to parse resource string: {data}, Error: {e}")
            return None
    
    return None


def extract_gpu_info_fast(resource_dict: Dict[str, Any]) -> Tuple[int, Optional[str]]:
    """
    Extract gpu infos。eg:'huawei.com/Ascend910B3': 1
    return：(count, type_name)
        eg: {'gpu': 1, 'gpu_type': 'Ascend910B3'}
    """
    for key, value in resource_dict.items():
        key_lower = key.lower()
        # 快速关键词匹配
        if any(k in key_lower for k in GPU_KEYWORDS):
            # 验证值是否为数字
            if isinstance(value, (int, float)):
                count = int(value)
                if count > 0:
                    # 提取类型名：huawei.com/Ascend910B3 -> Ascend910B3
                    gpu_type = key.split('/')[-1] if '/' in key else key
                    return count, gpu_type
            elif isinstance(value, str):
                try:
                    count = int(float(value))
                    if count > 0:
                        gpu_type = key.split('/')[-1] if '/' in key else key
                        return count, gpu_type
                except ValueError:
                    continue
    return 0, None

def fast_parse_memory(value: Any, default: int) -> int:
    """
    直接字符串后缀匹配。
    """
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return int(value)
    if not isinstance(value, str):
        return default
    
    val_str = value.strip()
    if not val_str:
        return default

    # 尝试匹配已知单位 (从长到短匹配，避免 'Mi' 匹配到 'i')
    # 优化：直接检查后缀，比正则快得多
    for unit, multiplier in MEMORY_UNITS.items():
        if val_str.endswith(unit):
            num_part = val_str[:-len(unit)]
            try:
                return int(float(num_part) * multiplier)
            except ValueError:
                break
    
    # 如果没有单位，尝试直接转整数
    try:
        return int(float(val_str))
    except ValueError:
        logger.warning(f"Invalid memory format: {value}")
        return default

def safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default