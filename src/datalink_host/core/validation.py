from __future__ import annotations


def parse_choice(value: object, field_name: str, allowed: set[str]) -> str:
    normalized = str(value).strip()
    if normalized not in allowed:
        choices = ", ".join(sorted(allowed))
        raise ValueError(f"{field_name} must be one of: {choices}")
    return normalized


def parse_port(value: object, field_name: str) -> int:
    port = int(value)
    if not 1 <= port <= 65535:
        raise ValueError(f"{field_name} must be between 1 and 65535")
    return port


def parse_positive_float(value: object, field_name: str, *, allow_zero: bool = False) -> float:
    numeric = float(value)
    if allow_zero:
        if numeric < 0:
            raise ValueError(f"{field_name} must be greater than or equal to 0")
        return numeric
    if numeric <= 0:
        raise ValueError(f"{field_name} must be greater than 0")
    return numeric


def parse_positive_int(value: object, field_name: str) -> int:
    numeric = int(value)
    if numeric <= 0:
        raise ValueError(f"{field_name} must be greater than 0")
    return numeric
