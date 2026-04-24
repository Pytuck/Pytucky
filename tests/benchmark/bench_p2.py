#!/usr/bin/env python3
"""P2 优化前后性能对比脚本。

针对 P2 两个优化项的热路径进行定向 benchmark：
- Opt A: Session.flush add_all + commit（批量插入 vs 逐条插入）
- Opt B: _materialize_records 批量读（有改动表的 flush）
- 基础指标: save / reopen / reopen_first_query（确保无退化）
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pytucky import Column, PureBaseModel, Session, Storage, declarative_base

RECORD_COUNT = 10_000
ROUNDS = 3
TABLE_NAME = "users"

def timer(func, *args, **kwargs) -> float:
    start = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - start
    return elapsed, result

def bench_session_add_all_commit(tmp: Path, count: int) -> float:
    """Opt A 目标：Session add_all + commit 的插入时间。"""
    db_path = tmp / "bench_addall.pytucky"
    if db_path.exists():
        db_path.unlink()

    db = Storage(file_path=db_path)
    Base: type[PureBaseModel] = declarative_base(db)

    class User(Base):
        __tablename__ = TABLE_NAME
        id = Column(int, primary_key=True)
        name = Column(str, nullable=False)
        value = Column(int, nullable=True)

    session = Session(db)
    instances = [User(name=f"u{i}", value=i) for i in range(count)]

    start = time.perf_counter()
    session.add_all(instances)
    session.commit()
    elapsed = time.perf_counter() - start

    session.close()
    db.close()
    return elapsed

def bench_materialize_flush(tmp: Path, count: int) -> float:
    """Opt B 目标：对有改动表执行 flush（触发 _materialize_records）。"""
    db_path = tmp / "bench_materialize.pytucky"
    if db_path.exists():
        db_path.unlink()

    # 创建初始数据并 flush 到磁盘
    db = Storage(file_path=db_path)
    db.create_table(TABLE_NAME, [
        Column(int, name="id", primary_key=True),
        Column(str, name="name"),
        Column(int, name="value"),
    ])
    records = [{"id": i, "name": f"u{i}", "value": i} for i in range(count)]
    db.bulk_insert(TABLE_NAME, records)
    db.flush()
    db.close()

    # 重新打开，修改一条记录（使表 dirty），然后 flush
    db2 = Storage(file_path=db_path)
    db2.update(TABLE_NAME, 0, {"value": 999})

    start = time.perf_counter()
    db2.flush()
    elapsed = time.perf_counter() - start

    db2.close()
    return elapsed

def bench_save_reopen_firstquery(tmp: Path, count: int) -> dict[str, float]:
    """基础指标：save / reopen / reopen_first_query。"""
    db_path = tmp / "bench_basic.pytucky"
    if db_path.exists():
        db_path.unlink()

    db = Storage(file_path=db_path)
    db.create_table(TABLE_NAME, [
        Column(int, name="id", primary_key=True),
        Column(str, name="name"),
        Column(int, name="value"),
    ])
    records = [{"id": i, "name": f"u{i}", "value": i} for i in range(count)]
    db.bulk_insert(TABLE_NAME, records)

    # save
    start = time.perf_counter()
    db.flush()
    save_time = time.perf_counter() - start
    db.close()

    # reopen
    start = time.perf_counter()
    db2 = Storage(file_path=db_path)
    reopen_time = time.perf_counter() - start

    # first query
    sample_id = count // 2
    start = time.perf_counter()
    row = db2.select(TABLE_NAME, sample_id)
    _ = row.get("name")
    first_query_time = time.perf_counter() - start
    db2.close()

    return {
        "save": save_time,
        "reopen": reopen_time,
        "reopen_first_query": first_query_time,
    }

def run_all(label: str) -> dict[str, float]:
    import tempfile
    results: dict[str, list] = {}

    for round_idx in range(ROUNDS):
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)

            t = bench_session_add_all_commit(tmp, RECORD_COUNT)
            results.setdefault("session_add_all_commit", []).append(t)

            t = bench_materialize_flush(tmp, RECORD_COUNT)
            results.setdefault("materialize_flush", []).append(t)

            basic = bench_save_reopen_firstquery(tmp, RECORD_COUNT)
            for k, v in basic.items():
                results.setdefault(k, []).append(v)

    # 取均值
    avg: dict[str, float] = {}
    for k, vals in results.items():
        avg[k] = sum(vals) / len(vals)

    print(f"\n{'='*60}")
    print(f"  {label}  ({RECORD_COUNT} 条记录, {ROUNDS} 轮均值)")
    print(f"{'='*60}")
    for k, v in avg.items():
        if v < 0.001:
            print(f"  {k:30s} {v*1_000_000:10.1f} μs")
        elif v < 1.0:
            print(f"  {k:30s} {v*1_000:10.2f} ms")
        else:
            print(f"  {k:30s} {v:10.4f} s")
    print()
    return avg

if __name__ == "__main__":
    run_all("P2 性能测试")
