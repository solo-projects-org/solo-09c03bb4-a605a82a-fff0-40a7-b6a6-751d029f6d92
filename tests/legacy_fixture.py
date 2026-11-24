# -*- coding: utf-8 -*-
"""
旧版数据库夹具：按升级前的表结构与写日志方式生成真实“旧库”。

旧版 block_op 的 CHECK 只接受 base/add/tombstone；补测/清空把合并区域
展开成逐块 tombstone。这里用“全量块集合差”写出同样的逐块日志，供
迁移与混用重放测试使用（真旧库由旧代码生成，日志语义与此一致）。

只用于测试：新代码不导入本模块。
"""

from __future__ import annotations

import os
import sqlite3
from typing import Dict, Optional, Sequence, Tuple

from app import quadtree

LEGACY_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS grid (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    side INTEGER NOT NULL, tol INTEGER NOT NULL,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cells (
    grid_id INTEGER NOT NULL REFERENCES grid(id),
    row_idx INTEGER NOT NULL, col_idx INTEGER NOT NULL, temp INTEGER,
    PRIMARY KEY (grid_id, row_idx, col_idx)
);
CREATE TABLE IF NOT EXISTS nodes (
    grid_id INTEGER NOT NULL REFERENCES grid(id),
    row_idx INTEGER NOT NULL, col_idx INTEGER NOT NULL, side INTEGER NOT NULL,
    measured_cnt INTEGER NOT NULL, unmeasured_cnt INTEGER NOT NULL,
    temp_min INTEGER, temp_max INTEGER, is_valid INTEGER NOT NULL,
    PRIMARY KEY (grid_id, row_idx, col_idx, side)
);
CREATE TABLE IF NOT EXISTS batch (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    grid_id INTEGER NOT NULL REFERENCES grid(id),
    reason TEXT NOT NULL, is_current INTEGER NOT NULL DEFAULT 0,
    tol INTEGER NOT NULL, block_count INTEGER NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS block_op (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL REFERENCES batch(id),
    row_idx INTEGER NOT NULL, col_idx INTEGER NOT NULL, side INTEGER NOT NULL,
    op TEXT NOT NULL CHECK (op IN ('base','add','tombstone')),
    temp_max INTEGER, temp_min INTEGER, cell_count INTEGER
);
CREATE INDEX IF NOT EXISTS idx_block_op_batch ON block_op(batch_id);
CREATE INDEX IF NOT EXISTS idx_block_op_pos ON block_op(row_idx, col_idx, side);
CREATE TABLE IF NOT EXISTS blocks_current (
    row_idx INTEGER NOT NULL, col_idx INTEGER NOT NULL, side INTEGER NOT NULL,
    temp_max INTEGER NOT NULL, temp_min INTEGER NOT NULL,
    cell_count INTEGER NOT NULL,
    PRIMARY KEY (row_idx, col_idx, side)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS event_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL, kind TEXT NOT NULL, detail TEXT NOT NULL
);
"""

TS = "2026-01-01T00:00:00+00:00"


def create_legacy_db(path: str) -> sqlite3.Connection:
    if os.path.exists(path):
        os.unlink(path)
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.executescript(LEGACY_SCHEMA)
    return conn


def _cells(conn) -> Dict[Tuple[int, int], Optional[int]]:
    return {(r["row_idx"], r["col_idx"]): r["temp"]
            for r in conn.execute("SELECT row_idx,col_idx,temp FROM cells")}


def _blocks(conn) -> Dict[Tuple[int, int, int], Tuple[int, int, int]]:
    return {(r["row_idx"], r["col_idx"], r["side"]):
            (r["temp_max"], r["temp_min"], r["cell_count"])
            for r in conn.execute(
                "SELECT row_idx,col_idx,side,temp_max,temp_min,cell_count "
                "FROM blocks_current")}


def _new_batch(conn, reason: str, tol: int, count: int) -> int:
    conn.execute("UPDATE batch SET is_current=0")
    cur = conn.execute(
        "INSERT INTO batch(grid_id,reason,is_current,tol,block_count,created_at) "
        "VALUES(1,?,1,?,?,?)", (reason, tol, count, TS))
    return cur.lastrowid


def _recompute_nodes(conn, n: int, tol: int) -> None:
    cells = _cells(conn)
    stats = quadtree.compute_all_node_stats(cells, n)
    conn.execute("DELETE FROM nodes")
    conn.executemany(
        "INSERT INTO nodes(grid_id,row_idx,col_idx,side,measured_cnt,"
        "unmeasured_cnt,temp_min,temp_max,is_valid) VALUES(1,?,?,?,?,?,?,?,?)",
        [(*coord, *stat, 1 if quadtree.stat_is_valid(stat, tol) else 0)
         for coord, stat in stats.items()])


def legacy_upload(conn, n: int,
                  data: Sequence[Sequence[Optional[int]]], tol: int) -> None:
    conn.execute("DELETE FROM block_op")
    conn.execute("DELETE FROM batch")
    conn.execute("DELETE FROM nodes")
    conn.execute("DELETE FROM cells")
    conn.execute("DELETE FROM grid")
    conn.execute("DELETE FROM blocks_current")
    conn.execute("INSERT INTO grid(id,side,tol,created_at,updated_at) "
                 "VALUES(1,?,?,?,?)", (n, tol, TS, TS))
    conn.executemany(
        "INSERT INTO cells(grid_id,row_idx,col_idx,temp) VALUES(1,?,?,?)",
        [(r, c, data[r][c]) for r in range(n) for c in range(n)])
    _recompute_nodes(conn, n, tol)
    cells = _cells(conn)
    stats = quadtree.compute_all_node_stats(cells, n)
    blocks = quadtree.collect_blocks(stats, cells, n, tol)
    bid = _new_batch(conn, "upload", tol, len(blocks))
    conn.executemany(
        "INSERT INTO block_op(batch_id,row_idx,col_idx,side,op,"
        "temp_max,temp_min,cell_count) VALUES(?,?,?,?,'base',?,?,?)",
        [(bid, b.row, b.col, b.size, b.tmax, b.tmin, b.count) for b in blocks])
    conn.executemany(
        "INSERT INTO blocks_current(row_idx,col_idx,side,temp_max,temp_min,"
        "cell_count) VALUES(?,?,?,?,?,?)",
        [(b.row, b.col, b.size, b.tmax, b.tmin, b.count) for b in blocks])


def legacy_apply_edits(conn, edits: Dict[Tuple[int, int], Optional[int]]) -> None:
    """旧写法：全量算出新旧块集合差，逐块写 add / tombstone。"""
    grid = conn.execute("SELECT * FROM grid WHERE id=1").fetchone()
    n, tol = grid["side"], grid["tol"]
    old = _blocks(conn)
    for (r, c), val in edits.items():
        conn.execute("UPDATE cells SET temp=? WHERE grid_id=1 "
                     "AND row_idx=? AND col_idx=?", (val, r, c))
    _recompute_nodes(conn, n, tol)
    cells = _cells(conn)
    stats = quadtree.compute_all_node_stats(cells, n)
    new_blocks = { (b.row, b.col, b.size): (b.tmax, b.tmin, b.count)
                   for b in quadtree.collect_blocks(stats, cells, n, tol) }
    dead = [p for p in old if p not in new_blocks]
    born = [(p, v) for p, v in new_blocks.items() if p not in old]
    bid = _new_batch(conn, "edit", tol, len(new_blocks))
    conn.executemany(
        "INSERT INTO block_op(batch_id,row_idx,col_idx,side,op) "
        "VALUES(?,?,?,?,'tombstone')", [(bid, *p) for p in dead])
    conn.executemany(
        "INSERT INTO block_op(batch_id,row_idx,col_idx,side,op,"
        "temp_max,temp_min,cell_count) VALUES(?,?,?,?,'add',?,?,?)",
        [(bid, *p, *v) for p, v in born])
    conn.executemany("DELETE FROM blocks_current WHERE row_idx=? AND "
                     "col_idx=? AND side=?", dead)
    conn.executemany(
        "INSERT INTO blocks_current(row_idx,col_idx,side,temp_max,temp_min,"
        "cell_count) VALUES(?,?,?,?,?,?)", [(p[0], p[1], p[2], *v)
                                            for p, v in born])


def legacy_update_tolerance(conn, tol: int) -> None:
    grid = conn.execute("SELECT * FROM grid WHERE id=1").fetchone()
    n = grid["side"]
    conn.execute("UPDATE grid SET tol=?, updated_at=?", (tol, TS))
    _recompute_nodes(conn, n, tol)
    cells = _cells(conn)
    stats = quadtree.compute_all_node_stats(cells, n)
    blocks = quadtree.collect_blocks(stats, cells, n, tol)
    conn.execute("DELETE FROM block_op")
    bid = _new_batch(conn, "tolerance", tol, len(blocks))
    conn.executemany(
        "INSERT INTO block_op(batch_id,row_idx,col_idx,side,op,"
        "temp_max,temp_min,cell_count) VALUES(?,?,?,?,'base',?,?,?)",
        [(bid, b.row, b.col, b.size, b.tmax, b.tmin, b.count) for b in blocks])
    conn.execute("DELETE FROM blocks_current")
    conn.executemany(
        "INSERT INTO blocks_current(row_idx,col_idx,side,temp_max,temp_min,"
        "cell_count) VALUES(?,?,?,?,?,?)",
        [(b.row, b.col, b.size, b.tmax, b.tmin, b.count) for b in blocks])
