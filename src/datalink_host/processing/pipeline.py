from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np

from datalink_host.core.config import ProcessingSettings
from datalink_host.models.messages import ChannelFrame, ProcessedFrame


@dataclass(slots=True)
class AverageDownsampler:
    target_rate: float
    _carry: np.ndarray | None = field(default=None, init=False)

    def output_rate(self, source_rate: float) -> float:
        if source_rate <= 0:
            return 0.0
        if self.target_rate <= 0 or self.target_rate >= source_rate:
            return source_rate
        factor = max(int(round(source_rate / self.target_rate)), 1)
        return source_rate / factor

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
        self._unwrap_last_samples: np.ndarray | None = None

    def update_rates(self, data1_rate: float, data2_rate: float) -> None:
        self._settings.data1_rate = data1_rate
        self._settings.data2_rate = data2_rate
        self._data1 = AverageDownsampler(data1_rate)
        self._data2 = AverageDownsampler(data2_rate)

    def reset(self) -> None:
        self._data1 = AverageDownsampler(self._settings.data1_rate)
        self._data2 = AverageDownsampler(self._settings.data2_rate)
        self._unwrap_last_samples = None

    def process(self, frame: ChannelFrame) -> ProcessedFrame:
        raw = frame.channels
        if self._settings.enable_phase_unwrap:
            unwrapped = self._unwrap_channels(raw)
        else:
            self._unwrap_last_samples = None
            unwrapped = raw.copy()
        data1 = self._data1.process(unwrapped, frame.sample_rate)
        data2 = self._data2.process(unwrapped, frame.sample_rate)
        return ProcessedFrame(
            sample_rate=frame.sample_rate,
            raw=raw,
            unwrapped=unwrapped,
            data1=data1,
            data1_sample_rate=self._data1.output_rate(frame.sample_rate),
            data2=data2,
            data2_sample_rate=self._data2.output_rate(frame.sample_rate),
            received_at=frame.received_at,
            timestamp_us=frame.timestamp_us,
        )

    def _unwrap_channels(self, channels: np.ndarray) -> np.ndarray:
        if channels.size == 0:
            return channels.copy()
        if self._unwrap_last_samples is None or self._unwrap_last_samples.shape[0] != channels.shape[0]:
            unwrapped = np.unwrap(channels, axis=1)
        else:
            stitched = np.concatenate([self._unwrap_last_samples[:, np.newaxis], channels], axis=1)
            unwrapped = np.unwrap(stitched, axis=1)[:, 1:]
        self._unwrap_last_samples = unwrapped[:, -1].copy()
        return unwrapped


def compute_psd(signal: np.ndarray, sample_rate: float) -> tuple[np.ndarray, np.ndarray]:
    if signal.size == 0 or sample_rate <= 0:
        return np.array([]), np.array([])
    centered = signal - np.mean(signal)
    spectrum = np.fft.rfft(centered)
    freqs = np.fft.rfftfreq(signal.size, d=1.0 / sample_rate)
    psd = (np.abs(spectrum) ** 2) / max(signal.size * sample_rate, 1e-9)
    return freqs, psd
