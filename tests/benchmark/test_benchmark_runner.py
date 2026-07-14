import argparse
import json
from pathlib import Path

import pytest

from tests.benchmark import benchmark as bench_module


@pytest.mark.benchmark
def test_benchmark_schema(tmp_path: Path) -> None:
    result = bench_module.PytuckyBenchmark(tmp_path, extended=True).run(5)

    assert isinstance(result, dict)
    assert result.get("engine") == "pytucky"
    assert result.get("record_count") == 5
    assert result.get("success") is True

    for key in (
        "insert",
        "save",
        "query_pk",
        "load",
        "reopen",
        "reopen_first_query",
        "file_size",
        "query_indexed",
        "transaction_begin",
        "transaction_rollback",
        "transaction_peak_memory",
        "small_update_flush",
        "encrypted_reopen",
        "encrypted_reopen_peak_memory",
        "encrypted_file_size",
        "temporary_file_leftover",
    ):
        assert key in result

    assert result["transaction_peak_memory"] >= 0
    assert result["encrypted_reopen_peak_memory"] >= 0
    assert result["temporary_file_leftover"] is False

    assert "lazy_load" not in result
    assert "lazy_query_first" not in result
    assert "lazy_query_batch" not in result


@pytest.mark.benchmark
def test_output_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    out_file = tmp_path / "out.json"
    args = argparse.Namespace(
        count=1,
        extended=False,
        keep=False,
        output_json=str(out_file),
    )

    results = bench_module.main(args)

    assert out_file.exists()
    assert len(results) == 1
    assert results[0]["engine"] == "pytucky"
    assert not (tmp_path / bench_module.TEMP_DIR_NAME).exists()

    payload = json.loads(out_file.read_text(encoding="utf-8"))
    assert payload["record_count"] == 1
    assert isinstance(payload["results"], list)
    assert payload["results"][0]["engine"] == "pytucky"
    assert "timestamp" in payload
    assert "system" in payload
    assert "python_version" in payload


@pytest.mark.benchmark
def test_build_temp_dir_uses_system_temp_outside_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)

    temp_dir = bench_module.build_temp_dir(False)
    try:
        assert temp_dir.exists()
        assert temp_dir != tmp_path / bench_module.TEMP_DIR_NAME
        assert tmp_path not in temp_dir.parents
    finally:
        bench_module.cleanup_temp_dir(temp_dir, keep=False)

    assert not temp_dir.exists()
    assert not (tmp_path / bench_module.TEMP_DIR_NAME).exists()


@pytest.mark.benchmark
@pytest.mark.parametrize("value", ["0", "-1", "invalid"])
def test_positive_record_count_rejects_invalid_values(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        bench_module.positive_record_count(value)


@pytest.mark.benchmark
def test_main_rejects_non_positive_programmatic_count() -> None:
    args = argparse.Namespace(
        count=0,
        extended=False,
        keep=False,
        output_json=None,
    )

    with pytest.raises(argparse.ArgumentTypeError, match="greater than zero"):
        bench_module.main(args)
