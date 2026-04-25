from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np


OUTPUT_DATA_TYPES = {"float32", "int32"}
DEFAULT_INT32_GAIN = 1_000_000.0


@dataclass(frozen=True, slots=True)
class EncodedSamples:
    values: np.ndarray
    miniseed_encoding: str


def normalize_output_data_type(value: object) -> str:
    normalized = str(value).strip().lower()
    if normalized not in OUTPUT_DATA_TYPES:
        choices = ", ".join(sorted(OUTPUT_DATA_TYPES))
        raise ValueError(f"storage.output_data_type must be one of: {choices}")
    return normalized


def normalize_int32_gain(value: object) -> float:
    numeric = float(value)
    if not math.isfinite(numeric) or numeric <= 0:
        raise ValueError("storage.int32_gain must be a finite number greater than 0")
    return numeric


def encode_samples_for_miniseed(
    values: np.ndarray,
    *,
    output_data_type: str,
    int32_gain: float,
) -> EncodedSamples:
    data_type = normalize_output_data_type(output_data_type)
    if data_type == "float32":
        return EncodedSamples(
            values=np.asarray(values, dtype=np.float32),
            miniseed_encoding="FLOAT32",
        )

    gain = normalize_int32_gain(int32_gain)
    scaled = np.asarray(values, dtype=np.float64) * gain
    rounded = np.rint(scaled)
    if not np.all(np.isfinite(rounded)):
        raise ValueError("INT32 output contains non-finite values after applying gain")

    limits = np.iinfo(np.int32)
    if np.any(rounded < limits.min) or np.any(rounded > limits.max):
        raise ValueError(
            "INT32 output is out of range after applying gain; "
            f"allowed_range=[{limits.min}, {limits.max}], gain={gain:g}"
        )

    return EncodedSamples(
        values=rounded.astype(np.int32),
        miniseed_encoding="INT32",
    )
