from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np

from datalink_host.core.config import ProcessingSettings
from datalink_host.models.messages import ChannelFrame, ProcessedFrame


@dataclass(slots=True)
class AverageDownsampler:
    target_rate: float
    _carry: np.ndarray | None = field(default=None, init=False)

    def process(self, channels: np.ndarray, source_rate: float) -> np.ndarray:
        if self.target_rate <= 0 or self.target_rate >= source_rate:
            return channels.copy()

        factor = max(int(round(source_rate / self.target_rate)), 1)
        working = channels if self._carry is None else np.concatenate([self._carry, channels], axis=1)
        usable = (working.shape[1] // factor) * factor
        if usable == 0:
            self._carry = working
            return np.empty((working.shape[0], 0), dtype=working.dtype)

        reduced = working[:, :usable].reshape(working.shape[0], -1, factor).mean(axis=2)
        self._carry = working[:, usable:]
        return reduced


class ProcessingPipeline:
    def __init__(self, settings: ProcessingSettings) -> None:
        self._settings = settings
        self._data1 = AverageDownsampler(settings.data1_rate)
        self._data2 = AverageDownsampler(settings.data2_rate)

    def update_rates(self, data1_rate: float, data2_rate: float) -> None:
        self._settings.data1_rate = data1_rate
        self._settings.data2_rate = data2_rate
        self._data1 = AverageDownsampler(data1_rate)
        self._data2 = AverageDownsampler(data2_rate)

    def process(self, frame: ChannelFrame) -> ProcessedFrame:
        raw = frame.channels
        unwrapped = np.unwrap(raw, axis=1) if self._settings.enable_phase_unwrap else raw.copy()
        data1 = self._data1.process(unwrapped, frame.sample_rate)
        data2 = self._data2.process(unwrapped, frame.sample_rate)
        return ProcessedFrame(
            sample_rate=frame.sample_rate,
            raw=raw,
            unwrapped=unwrapped,
            data1=data1,
            data2=data2,
            received_at=frame.received_at,
        )


def compute_psd(signal: np.ndarray, sample_rate: float) -> tuple[np.ndarray, np.ndarray]:
    if signal.size == 0 or sample_rate <= 0:
        return np.array([]), np.array([])
    centered = signal - np.mean(signal)
    spectrum = np.fft.rfft(centered)
    freqs = np.fft.rfftfreq(signal.size, d=1.0 / sample_rate)
    psd = (np.abs(spectrum) ** 2) / max(signal.size * sample_rate, 1e-9)
    return freqs, psd
