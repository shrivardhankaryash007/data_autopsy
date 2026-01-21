"""In-memory measurement store for the data autopsy workflow."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable


@dataclass(frozen=True)
class Measurement:
    """Represents a single observed measurement."""

    metric: str
    value: float
    timestamp: datetime
    tags: dict[str, str]


class MeasurementStore:
    """Simple in-memory store for measurements."""

    def __init__(self) -> None:
        self._measurements: list[Measurement] = []

    def add(self, measurement: Measurement) -> None:
        """Add a validated measurement to the store."""
        self._validate_measurement(measurement)
        self._measurements.append(measurement)

    def record(
        self,
        metric: str,
        value: float,
        *,
        timestamp: datetime | None = None,
        tags: dict[str, str] | None = None,
    ) -> Measurement:
        """Create and store a measurement."""
        measurement = Measurement(
            metric=metric,
            value=value,
            timestamp=timestamp or datetime.now(timezone.utc),
            tags=dict(tags or {}),
        )
        self.add(measurement)
        return measurement

    def list(
        self,
        *,
        metric: str | None = None,
        tag_filters: dict[str, str] | None = None,
    ) -> list[Measurement]:
        """List measurements, optionally filtered by metric and tags."""
        if metric is None and not tag_filters:
            return list(self._measurements)

        tag_filters = tag_filters or {}
        return [
            measurement
            for measurement in self._measurements
            if self._matches_metric(measurement, metric)
            and self._matches_tags(measurement, tag_filters)
        ]

    def clear(self) -> None:
        """Remove all stored measurements."""
        self._measurements.clear()

    def _validate_measurement(self, measurement: Measurement) -> None:
        if not measurement.metric.strip():
            raise ValueError("metric must be a non-empty string")
        if not isinstance(measurement.value, (int, float)):
            raise TypeError("value must be a number")
        if not isinstance(measurement.timestamp, datetime):
            raise TypeError("timestamp must be a datetime")
        if measurement.timestamp.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")
        if not isinstance(measurement.tags, dict):
            raise TypeError("tags must be a dict")
        for key, value in measurement.tags.items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise TypeError("tags must map strings to strings")

    @staticmethod
    def _matches_metric(measurement: Measurement, metric: str | None) -> bool:
        return metric is None or measurement.metric == metric

    @staticmethod
    def _matches_tags(measurement: Measurement, tag_filters: dict[str, str]) -> bool:
        return all(measurement.tags.get(key) == value for key, value in tag_filters.items())

    def __iter__(self) -> Iterable[Measurement]:
        return iter(self._measurements)

    def __len__(self) -> int:
        return len(self._measurements)
