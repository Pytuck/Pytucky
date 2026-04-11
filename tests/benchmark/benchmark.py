#!/usr/bin/env python3
"""Minimal single-engine benchmark runner for pytucky."""
from __future__ import annotations

import argparse
import json
import platform
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path
from types import TracebackType
from typing import Any, Dict, List, Optional, Tuple, Type

# Ensure project root on path.
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pytucky import Column, PureBaseModel, Session, Storage, declarative_base, insert, select

DEFAULT_RECORD_COUNT = 100
OUTPUT_DIR = Path(__file__).parent / "benchmark_output"
TEMP_DIR_NAME = ".tmp_bench"
TABLE_NAME = "users"


class Timer:
    def __init__(self) -> None:
        self.elapsed: float = 0.0
        self.start: float = 0.0

    def __enter__(self) -> "Timer":
        self.start = time.perf_counter()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        del exc_type, exc, tb
        self.elapsed = time.perf_counter() - self.start


class PytuckyBenchmark:
    def __init__(self, temp_dir: Path, extended: bool = False) -> None:
        self.temp_dir = temp_dir
        self.extended = extended
        self.file_path = temp_dir / "bench_db.pytucky"

    def setup(self) -> Tuple[Storage, Session, Type[PureBaseModel]]:
        self._cleanup_storage_files()

        db = Storage(file_path=self.file_path)
        Base: Type[PureBaseModel] = declarative_base(db)

        class User(Base):
            __tablename__ = TABLE_NAME
            id = Column(int, primary_key=True)
            name = Column(str, nullable=False, index=True)
            value = Column(int, nullable=True)

        session = Session(db)
        return db, session, User

    def _cleanup_storage_files(self) -> None:
        journal_path = self.file_path.with_name(".%s.journal" % self.file_path.name)
        for path in (self.file_path, journal_path):
            if path.exists() and path.is_file():
                path.unlink()

    def bench_reopen(self) -> float:
        with Timer() as timer:
            db = Storage(file_path=self.file_path)
        db.close()
        return timer.elapsed

    def bench_insert(self, session: Session, model_class: Type[PureBaseModel], count: int) -> float:
        with Timer() as timer:
            for index in range(count):
                statement = insert(model_class).values(name="u%d" % index, value=index)
                session.execute(statement)
            session.commit()
        return timer.elapsed

    def bench_save(self, db: Storage) -> float:
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
        model_class: Type[PureBaseModel],
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
        model_class: Type[PureBaseModel],
        count: int,
    ) -> float:
        lookups = min(100, count)
        with Timer() as timer:
            for index in range(lookups):
                statement = select(model_class).filter_by(name="u%d" % index)
                result = session.execute(statement)
                _ = result.first()
        return timer.elapsed

    def run(self, count: int) -> Dict[str, Any]:
        results: Dict[str, Any] = {
            "engine": "pytucky",
            "record_count": count,
        }
        db: Optional[Storage] = None
        session: Optional[Session] = None
        try:
            db, session, user_model = self.setup()
            results["insert"] = self.bench_insert(session, user_model, count)
            results["save"] = self.bench_save(db)
            results["query_pk"] = self.bench_query_pk(session, user_model, count)
            if self.extended:
                results["query_indexed"] = self.bench_query_indexed(session, user_model, count)
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
        type=int,
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


def build_output_payload(record_count: int, results: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "timestamp": datetime.now().isoformat(),
        "system": platform.system(),
        "python_version": platform.python_version(),
        "record_count": record_count,
        "results": results,
    }


def write_output_json(output_path: Path, payload: Dict[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file_obj:
        json.dump(payload, file_obj, indent=2, ensure_ascii=False)


def build_temp_dir(keep: bool) -> Path:
    if keep:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        return OUTPUT_DIR

    temp_dir = Path.cwd() / TEMP_DIR_NAME
    temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir


def cleanup_temp_dir(temp_dir: Path, keep: bool) -> None:
    if keep or not temp_dir.exists():
        return
    shutil.rmtree(str(temp_dir), ignore_errors=True)


def main(args: Optional[argparse.Namespace] = None) -> List[Dict[str, Any]]:
    if args is None:
        args = parse_args()

    temp_dir = build_temp_dir(bool(args.keep))
    try:
        benchmark = PytuckyBenchmark(temp_dir, extended=bool(args.extended))
        results = [benchmark.run(int(args.count))]
        payload = build_output_payload(int(args.count), results)
        if args.output_json:
            write_output_json(Path(args.output_json), payload)
        return results
    finally:
        cleanup_temp_dir(temp_dir, bool(args.keep))


if __name__ == "__main__":
    main()
