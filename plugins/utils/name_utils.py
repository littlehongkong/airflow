# plugins/utils/name_utils.py
import re

def normalize_field_names(record_or_dict):
    """
    ✅ CamelCase → snake_case 변환
    ✅ 필드명 전체 소문자 변환
    ✅ 일관된 Warehouse 네이밍 규칙 적용
    """
    def camel_to_snake(name: str) -> str:
        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

    if isinstance(record_or_dict, dict):
        return {camel_to_snake(k): v for k, v in record_or_dict.items()}
    elif hasattr(record_or_dict, "columns"):
        record_or_dict.columns = [camel_to_snake(c) for c in record_or_dict.columns]
        return record_or_dict
    return record_or_dict
