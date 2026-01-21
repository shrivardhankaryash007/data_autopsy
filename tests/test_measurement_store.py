from datetime import datetime, timezone

import pytest

from data_autopsy.measurement_store import Measurement, MeasurementStore


def test_record_adds_measurement() -> None:
    store = MeasurementStore()

    measurement = store.record("latency_ms", 123.4, tags={"host": "api-1"})

    assert measurement.metric == "latency_ms"
    assert measurement.value == 123.4
    assert measurement.tags == {"host": "api-1"}
    assert measurement in store.list()
    assert len(store) == 1


def test_list_filters_by_metric_and_tags() -> None:
    store = MeasurementStore()
    store.record("latency_ms", 100, tags={"host": "api-1"})
    store.record("latency_ms", 200, tags={"host": "api-2"})
    store.record("error_rate", 0.1, tags={"host": "api-1"})

    filtered = store.list(metric="latency_ms", tag_filters={"host": "api-1"})

    assert len(filtered) == 1
    assert filtered[0].value == 100


def test_add_validates_measurement() -> None:
    store = MeasurementStore()
    measurement = Measurement(
        metric="",
        value=1.0,
        timestamp=datetime.now(timezone.utc),
        tags={},
    )

    with pytest.raises(ValueError, match="metric must be a non-empty string"):
        store.add(measurement)


def test_timestamp_requires_timezone() -> None:
    store = MeasurementStore()
    measurement = Measurement(
        metric="latency_ms",
        value=1.0,
        timestamp=datetime.now(),
        tags={},
    )

    with pytest.raises(ValueError, match="timestamp must be timezone-aware"):
        store.add(measurement)
