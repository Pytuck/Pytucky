#!/usr/bin/env python3
"""Minimal single-engine benchmark runner for pytucky."""
from __future__ import annotations

import argparse
import json
import platform
import shutil
import tempfile
import time
import tracemalloc
from datetime import datetime
from pathlib import Path
from types import TracebackType
from typing import Any

from pytucky import Column, PureBaseModel, Session, Storage, declarative_base, insert, select
from pytucky.common.options import PytuckBackendOptions

DEFAULT_RECORD_COUNT = 100
OUTPUT_DIR = Path(__file__).parent / "benchmark_output"
TEMP_DIR_NAME = ".tmp_bench"
TABLE_NAME = "benchmark_users"


def positive_record_count(value: str) -> int:
    """解析大于零的 benchmark 记录数。"""
    try:
        count = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("record count must be an integer") from exc
    if count <= 0:
        raise argparse.ArgumentTypeError("record count must be greater than zero")
    return count

class Timer:
    def __init__(self) -> None:
        self.elapsed: float = 0.0
        self.start: float = 0.0

    def __enter__(self) -> "Timer":
        self.start = time.perf_counter()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        del exc_type, exc, tb
        self.elapsed = time.perf_counter() - self.start

class PytuckyBenchmark:
    def __init__(self, temp_dir: Path, extended: bool = False) -> None:
        self.temp_dir = temp_dir
        self.extended = extended
        self.file_path = temp_dir / "bench_db.pytuck"
        self.encrypted_file_path = temp_dir / "bench_encrypted.pytuck"

    def setup(self) -> tuple[Storage, Session, type[PureBaseModel]]:
        self._cleanup_storage_files()

        db = Storage(file_path=self.file_path)
        Base: type[PureBaseModel] = declarative_base(db)

        class BenchmarkUser(Base):
            __tablename__ = TABLE_NAME
            id = Column(int, primary_key=True)
            name = Column(str, nullable=False, index=True)
            email = Column(str, nullable=True)
            age = Column(int, nullable=True)
            score = Column(float, nullable=True)
            active = Column(bool, nullable=True)

        session = Session(db)
        return db, session, BenchmarkUser

    def _cleanup_storage_files(self) -> None:
        paths = (
            self.file_path,
            self.file_path.with_suffix(".pytuck.tmp"),
            self.encrypted_file_path,
            self.encrypted_file_path.with_suffix(".pytuck.tmp"),
        )
        for path in paths:
            if path.exists() and path.is_file():
                path.unlink()

    def bench_reopen(self) -> float:
        with Timer() as timer:
            db = Storage(file_path=self.file_path)
        db.close()
        return timer.elapsed

    def bench_insert(self, session: Session, model_class: type[PureBaseModel], count: int) -> float:
        with Timer() as timer:
            for index in range(count):
                statement = insert(model_class).values(
                    name="User_%d" % index,
                    email="user%d@example.com" % index,
                    age=20 + (index % 50),
                    score=float(index % 100) / 10.0,
                    active=(index % 2 == 0),
                )
                session.execute(statement)
            session.commit()
        return timer.elapsed

    def bench_save(self, db: Storage) -> float:
        with Timer() as timer:
            db.flush()
        return timer.elapsed

    def bench_small_update_flush(self, db: Storage, count: int) -> float:
        if count:
            db.update(TABLE_NAME, min(count, max(1, count // 2)), {"score": 99.5})
        with Timer() as timer:
            db.flush()
        return timer.elapsed

    def bench_load(self) -> float:
        with Timer() as timer:
            db = Storage(file_path=self.file_path)
        db.close()
        return timer.elapsed

    def bench_reopen_first_query(self, count: int) -> float:
        sample_id = min(count, max(1, (count + 1) // 2))
        db = Storage(file_path=self.file_path)
        try:
            with Timer() as timer:
                row = db.select(TABLE_NAME, sample_id)
                _ = row.get("name")
            return timer.elapsed
        finally:
            db.close()

    def bench_query_pk(
        self,
        session: Session,
        model_class: type[PureBaseModel],
        count: int,
    ) -> float:
        lookups = min(100, count)
        with Timer() as timer:
            for index in range(lookups):
                statement = select(model_class).filter_by(id=index + 1)
                result = session.execute(statement)
                _ = result.first()
        return timer.elapsed

    def bench_query_indexed(
        self,
        session: Session,
        model_class: type[PureBaseModel],
        count: int,
    ) -> float:
        lookups = min(100, count)
        with Timer() as timer:
            for index in range(lookups):
                statement = select(model_class).filter_by(name="User_%d" % index)
                result = session.execute(statement)
                _ = result.first()
        return timer.elapsed

    def bench_transaction_snapshot(self, count: int) -> dict[str, float | int]:
        """测量当前内存快照事务的固定成本，不改变事务实现。"""
        transaction_db = Storage(in_memory=True)
        transaction_db.create_table(
            "transaction_rows",
            [
                Column(int, name="id", primary_key=True),
                Column(str, name="name", index=True),
            ],
        )
        transaction_db.bulk_insert(
            "transaction_rows",
            [{"name": f"row-{index}"} for index in range(count)],
        )

        with Timer() as begin_timer:
            with transaction_db.transaction():
                pass

        tracemalloc.start()
        try:
            with transaction_db.transaction():
                pass
            _, peak_memory = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()

        with Timer() as rollback_timer:
            try:
                with transaction_db.transaction():
                    if count:
                        transaction_db.update("transaction_rows", 1, {"name": "changed"})
                    raise RuntimeError("benchmark rollback")
            except RuntimeError as exc:
                if str(exc) != "benchmark rollback":
                    raise

        transaction_db.close()
        return {
            "transaction_begin": begin_timer.elapsed,
            "transaction_rollback": rollback_timer.elapsed,
            "transaction_peak_memory": peak_memory,
        }

    def bench_encrypted_reopen(self, count: int) -> dict[str, float | int]:
        """测量单文件加密数据库 reopen 的耗时、峰值内存与文件大小。"""
        options = PytuckBackendOptions(encryption="high", password="benchmark-secret")
        encrypted_db = Storage(file_path=self.encrypted_file_path, backend_options=options)
        encrypted_db.create_table(
            "encrypted_rows",
            [
                Column(int, name="id", primary_key=True),
                Column(str, name="value"),
            ],
        )
        encrypted_db.bulk_insert(
            "encrypted_rows",
            [{"value": f"encrypted-{index}"} for index in range(count)],
        )
        encrypted_db.flush()
        encrypted_db.close()

        reopen_options = PytuckBackendOptions(password="benchmark-secret")
        with Timer() as timer:
            reopened = Storage(
                file_path=self.encrypted_file_path,
                backend_options=reopen_options,
            )
        reopened.close()

        tracemalloc.start()
        try:
            measured_reopen = Storage(
                file_path=self.encrypted_file_path,
                backend_options=reopen_options,
            )
            _, peak_memory = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        measured_reopen.close()
        return {
            "encrypted_reopen": timer.elapsed,
            "encrypted_reopen_peak_memory": peak_memory,
            "encrypted_file_size": self.encrypted_file_path.stat().st_size,
        }

    def run(self, count: int) -> dict[str, Any]:
        results: dict[str, Any] = {
            "engine": "pytucky",
            "record_count": count,
        }
        db: Storage | None = None
        session: Session | None = None
        try:
            db, session, user_model = self.setup()
            results["insert"] = self.bench_insert(session, user_model, count)
            results["save"] = self.bench_save(db)
            results["query_pk"] = self.bench_query_pk(session, user_model, count)
            if self.extended:
                results["query_indexed"] = self.bench_query_indexed(session, user_model, count)
                results["small_update_flush"] = self.bench_small_update_flush(db, count)
                results.update(self.bench_transaction_snapshot(count))
                results.update(self.bench_encrypted_reopen(count))
        except Exception as exc:
            results["success"] = False
            results["error"] = str(exc)
            return results
        finally:
            if session is not None:
                session.close()
            if db is not None:
                db.close()

        try:
            results["load"] = self.bench_load()
            results["reopen"] = self.bench_reopen()
            results["reopen_first_query"] = self.bench_reopen_first_query(count)
            results["file_size"] = self.file_path.stat().st_size if self.file_path.exists() else 0
            results["temporary_file_leftover"] = any(
                path.exists()
                for path in (
                    self.file_path.with_suffix(".pytuck.tmp"),
                    self.encrypted_file_path.with_suffix(".pytuck.tmp"),
                )
            )
            results["success"] = True
        except Exception as exc:
            results["success"] = False
            results["error"] = str(exc)
        return results

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="pytucky minimal benchmark")
    parser.add_argument(
        "-n",
        "--count",
        type=positive_record_count,
        default=DEFAULT_RECORD_COUNT,
        help="record count to benchmark",
    )
    parser.add_argument(
        "--extended",
        action="store_true",
        help="run indexed-query and reopen metrics",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="keep benchmark files under tests/benchmark/benchmark_output/",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default=None,
        metavar="FILE",
        help="write JSON results to the given file path",
    )
    return parser.parse_args()

def build_output_payload(record_count: int, results: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "timestamp": datetime.now().isoformat(),
        "system": platform.system(),
        "python_version": platform.python_version(),
        "record_count": record_count,
        "results": results,
    }

def write_output_json(output_path: Path, payload: dict[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file_obj:
        json.dump(payload, file_obj, indent=2, ensure_ascii=False)

def build_temp_dir(keep: bool) -> Path:
    if keep:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        return OUTPUT_DIR

    return Path(tempfile.mkdtemp(prefix=f"{TEMP_DIR_NAME.lstrip('.')}"))

def cleanup_temp_dir(temp_dir: Path, keep: bool) -> None:
    if keep or not temp_dir.exists():
        return
    shutil.rmtree(str(temp_dir), ignore_errors=True)

def main(args: argparse.Namespace | None = None) -> list[dict[str, Any]]:
    if args is None:
        args = parse_args()

    record_count = positive_record_count(str(args.count))
    temp_dir = build_temp_dir(bool(args.keep))
    try:
        benchmark = PytuckyBenchmark(temp_dir, extended=bool(args.extended))
        results = [benchmark.run(record_count)]
        payload = build_output_payload(record_count, results)
        if args.output_json:
            write_output_json(Path(args.output_json), payload)
        return results
    finally:
        cleanup_temp_dir(temp_dir, bool(args.keep))

if __name__ == "__main__":
    main()
