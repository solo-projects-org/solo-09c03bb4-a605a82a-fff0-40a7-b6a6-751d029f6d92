# -*- coding: utf-8 -*-
"""
SQLite 持久层与核算服务。

数据模型
--------
grid      : 一张方阵（一次上传一行），记录边长与当前允许温差
cells     : 每个格子一行，温度 NULL 表示未测
nodes     : 四叉树内部节点（size>=2）的统计与合法标志，供增量核算复用
batch     : 批次。上传/改温差/补测清空成功各生成一个批次，
            is_current=1 的批次对应“现行供数”；批次一经生效不可变
block_op  : 块变更日志（delta 模型）
            * 上传、改温差写一整套 op='base' 的现行块；
            * 补测/清空只在受影响子树写 op='add'（新块）、
              op='tombstone'（精确坐标作废）与 op='tombstone_range'
              （整块区域作废，吸收内部旧块时每个区域只记一条）；
            * 旧库遗留的逐块 op='tombstone' 原样保留，重放时与新记录混用；
            * 现行块 = 从最近一次 base 批次起重放：每个坐标取最新精确操作，
              add/base 生效、tombstone 丢弃；若其后有覆盖该坐标（严格位于
              区域内）的 tombstone_range，则该块作废。区域记录不作用于
              同批写在该区域原点的大块，故新大块始终有效。
            * 物化表 blocks_current 与日志在同一事务内用局部核算维护，
              作废旧块只查局部物化表，不再回查历史日志。
event_log : 操作审计

增量核算（补测/清空）
--------------------
1. 取改动格的全部祖先节点；
2. 自底向上（size 升序）只重算这些节点（及其直接孩子）的统计与合法标志；
3. 自根向下收集块增量：未受影响子树整棵跳过（旧块继续生效），
   受影响路径上按新合法性写 add / tombstone / tombstone_range；
   合并时每个区域只写一条范围作废；
4. 全流程在一个 BEGIN IMMEDIATE 事务内，异常回滚，旧批次与旧供数不变。
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple

from . import quadtree
from .quadtree import Block

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS grid (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    side        INTEGER NOT NULL,
    tol         INTEGER NOT NULL,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cells (
    grid_id  INTEGER NOT NULL REFERENCES grid(id),
    row_idx  INTEGER NOT NULL,
    col_idx  INTEGER NOT NULL,
    temp     INTEGER,            -- NULL = 未测
    PRIMARY KEY (grid_id, row_idx, col_idx)
);

CREATE TABLE IF NOT EXISTS nodes (
    grid_id        INTEGER NOT NULL REFERENCES grid(id),
    row_idx        INTEGER NOT NULL,
    col_idx        INTEGER NOT NULL,
    side           INTEGER NOT NULL,
    measured_cnt   INTEGER NOT NULL,
    unmeasured_cnt INTEGER NOT NULL,
    temp_min       INTEGER,
    temp_max       INTEGER,
    is_valid       INTEGER NOT NULL,   -- 1=可作为覆盖方块
    PRIMARY KEY (grid_id, row_idx, col_idx, side)
);

CREATE TABLE IF NOT EXISTS batch (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    grid_id     INTEGER NOT NULL REFERENCES grid(id),
    reason      TEXT NOT NULL,        -- upload / tolerance / edit
    is_current  INTEGER NOT NULL DEFAULT 0,
    tol         INTEGER NOT NULL,
    block_count INTEGER NOT NULL,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS event_log (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         TEXT NOT NULL,
    kind       TEXT NOT NULL,
    detail     TEXT NOT NULL
);
"""


# block_op 单独成段：旧库升级时要按新 CHECK 约束重建该表。
BLOCK_OP_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS block_op (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id   INTEGER NOT NULL REFERENCES batch(id),
    row_idx    INTEGER NOT NULL,
    col_idx    INTEGER NOT NULL,
    side       INTEGER NOT NULL,
    op         TEXT NOT NULL CHECK (op IN ('base','add','tombstone','tombstone_range')),
    temp_max   INTEGER,
    temp_min   INTEGER,
    cell_count INTEGER
)
"""

BLOCK_OP_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_block_op_batch ON block_op(batch_id);
CREATE INDEX IF NOT EXISTS idx_block_op_pos ON block_op(row_idx, col_idx, side);
"""

# 现行块物化表：与 block_op 在同一事务内维护，下载/页面直接顺序读取，
# 无需跨批次还原；表内只保留 is_current 批次对应的块，天然同源。
BLOCKS_CURRENT_SQL = """
CREATE TABLE IF NOT EXISTS blocks_current (
    row_idx    INTEGER NOT NULL,
    col_idx    INTEGER NOT NULL,
    side       INTEGER NOT NULL,
    temp_max   INTEGER NOT NULL,
    temp_min   INTEGER NOT NULL,
    cell_count INTEGER NOT NULL,
    PRIMARY KEY (row_idx, col_idx, side)
) WITHOUT ROWID;
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def get_db_path() -> str:
    return os.environ.get("FURNACE_DB",
                          os.path.join(os.getcwd(), "instance", "furnace.db"))


def connect(db_path: Optional[str] = None) -> sqlite3.Connection:
    path = db_path or get_db_path()
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-65536")   # 64 MiB 页缓存
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn


def init_db(db_path: Optional[str] = None) -> None:
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        _ensure_block_op_schema(conn)
        conn.executescript(BLOCK_OP_INDEX_SQL)
        # 仅在物化表缺失时按日志重放补建；已有的现行块表保持原样，
        # 避免升级动作误伤现场数据。逐块旧记录一律不删不改。
        if _ensure_blocks_current(conn):
            with transaction(conn):
                conn.execute("DELETE FROM blocks_current")
                _rebuild_materialized(conn, replay_current_blocks(conn))
    finally:
        conn.close()


def _table_sql(conn: sqlite3.Connection, name: str) -> Optional[str]:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (name,)).fetchone()
    return row[0] if row else None


def _ensure_block_op_schema(conn: sqlite3.Connection) -> bool:
    """
    旧库的 block_op 只接受 base/add/tombstone 三种 op。若检测到旧 CHECK
    约束，则在保留全部历史行的前提下重建该表并放开 tombstone_range。
    返回是否做了迁移。
    """
    sql = _table_sql(conn, "block_op")
    if sql is None:
        conn.execute(BLOCK_OP_TABLE_SQL)
        return False
    if "tombstone_range" in sql:
        return False

    with transaction(conn):
        # 旧表可能带着索引；改名后索引跟随旧表，先记下名字再删除
        # （sqlite_autoindex_* 由约束自动生成，不能单独 DROP）。
        index_names = [r[1] for r in conn.execute("PRAGMA index_list('block_op')")
                       if not r[1].startswith("sqlite_autoindex")]
        conn.execute("ALTER TABLE block_op RENAME TO block_op_legacy")
        for idx_name in index_names:
            conn.execute(f'DROP INDEX IF EXISTS "{idx_name}"')
        conn.execute(BLOCK_OP_TABLE_SQL)
        conn.execute(
            "INSERT INTO block_op(id,batch_id,row_idx,col_idx,side,op,"
            "temp_max,temp_min,cell_count) "
            "SELECT id,batch_id,row_idx,col_idx,side,op,"
            "temp_max,temp_min,cell_count FROM block_op_legacy ORDER BY id")
        conn.execute("DROP TABLE block_op_legacy")
        # 显式带 id 复制后同步自增序列：确保下一条 id 大于所有历史 id
        seq_row = conn.execute(
            "SELECT seq FROM sqlite_sequence WHERE name='block_op'").fetchone()
        max_id = conn.execute("SELECT MAX(id) FROM block_op").fetchone()[0]
        if max_id is not None:
            if seq_row is None:
                conn.execute("INSERT INTO sqlite_sequence(name,seq) "
                             "VALUES('block_op',?)", (max_id,))
            elif seq_row[0] < max_id:
                conn.execute(
                    "UPDATE sqlite_sequence SET seq=? WHERE name='block_op'",
                    (max_id,))
    return True


def _ensure_blocks_current(conn: sqlite3.Connection) -> bool:
    """旧库可能缺物化表；返回是否需要按日志重放补齐。"""
    if _table_sql(conn, "blocks_current") is not None:
        return False
    conn.executescript(BLOCKS_CURRENT_SQL)
    return True


@contextmanager
def transaction(conn: sqlite3.Connection):
    """显式事务：异常回滚，正常提交。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


# ----------------------------- 读取 --------------------------------

def get_grid(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM grid WHERE id=1").fetchone()


def load_cells(conn: sqlite3.Connection) -> Dict[Tuple[int, int], Optional[int]]:
    rows = conn.execute(
        "SELECT row_idx, col_idx, temp FROM cells WHERE grid_id=1").fetchall()
    return {(r["row_idx"], r["col_idx"]): r["temp"] for r in rows}


def load_node_map(conn: sqlite3.Connection
                  ) -> Dict[Tuple[int, int, int], sqlite3.Row]:
    rows = conn.execute(
        "SELECT row_idx, col_idx, side, measured_cnt, unmeasured_cnt, "
        "temp_min, temp_max, is_valid FROM nodes WHERE grid_id=1"
    ).fetchall()
    return {(r["row_idx"], r["col_idx"], r["side"]): r for r in rows}


def current_batch_id(conn: sqlite3.Connection) -> Optional[int]:
    row = conn.execute(
        "SELECT id FROM batch WHERE grid_id=1 AND is_current=1").fetchone()
    return row["id"] if row else None


# 现行块直接读物化表；block_op 仅作为批次历史与回滚依据保留。
_CURRENT_BLOCK_SQL = """
SELECT row_idx, col_idx, side, temp_max, temp_min, cell_count
FROM blocks_current
ORDER BY row_idx, col_idx, side DESC
"""


def load_current_blocks(conn: sqlite3.Connection) -> List[Block]:
    if current_batch_id(conn) is None:
        return []
    rows = conn.execute(_CURRENT_BLOCK_SQL).fetchall()
    return [Block(r["row_idx"], r["col_idx"], r["side"], r["temp_max"],
                  r["temp_min"], r["cell_count"]) for r in rows]


def iter_current_blocks(conn: sqlite3.Connection):
    """流式迭代现行块，供下载大网格时避免一次性构建 20 万个对象。"""
    if current_batch_id(conn) is None:
        return
    cur = conn.execute(_CURRENT_BLOCK_SQL)
    for r in cur:
        yield Block(r["row_idx"], r["col_idx"], r["side"], r["temp_max"],
                    r["temp_min"], r["cell_count"])


def current_block_count(conn: sqlite3.Connection) -> int:
    if current_batch_id(conn) is None:
        return 0
    return conn.execute("SELECT COUNT(*) FROM blocks_current").fetchone()[0]


# ----------------------------- 重放 --------------------------------
#
# 从最近一次全量出块批次（reason=upload/tolerance）起，按 block_op.id
# 顺序重放，混用旧库逐块 tombstone 与新 tombstone_range：
#
#   base / add        : 覆盖该坐标的现行块（同址后写覆盖先写）
#   tombstone         : 精确作废该坐标（旧库格式，原样兼容）
#   tombstone_range   : 作废严格位于该区域内部的现行块；区域原点上的块
#                       （同批写出的大块）保留。每个合并区域只记一条。
#
# 结果应与 blocks_current 逐块相同，用于自检与旧库补建物化表。
# 以批次 reason 而非 base 行锚定，全未测上传（零块、无 base 行）也能重放。

_BASE_BATCH_SQL = """
SELECT b.id FROM batch b
WHERE b.grid_id=1 AND b.reason IN ('upload','tolerance')
ORDER BY b.id DESC LIMIT 1
"""


def replay_current_blocks(conn: sqlite3.Connection) -> List[Block]:
    """
    从日志重放现行块（自最近一次 base 批次起）。不依赖 blocks_current，
    供初始迁移、自检以及外部核对使用。
    """
    row = conn.execute(_BASE_BATCH_SQL).fetchone()
    if row is None:
        return []

    live: Dict[Tuple[int, int, int], Block] = {}
    for r, c, s, op, tmax, tmin, cnt in conn.execute(
            "SELECT row_idx, col_idx, side, op, temp_max, temp_min, cell_count "
            "FROM block_op WHERE batch_id>=? ORDER BY id", (row[0],)):
        if op in ("base", "add"):
            live[(r, c, s)] = Block(r, c, s, tmax, tmin, cnt)
        elif op == "tombstone":
            live.pop((r, c, s), None)
        elif op == "tombstone_range":
            # 区域坐标上的块是同批新写的大块，保留；只作废其内部旧块。
            victims = [pos for pos in live
                       if pos != (r, c, s)
                       and pos[0] >= r and pos[1] >= c
                       and pos[0] + pos[2] <= r + s and pos[1] + pos[2] <= c + s]
            for pos in victims:
                del live[pos]
    return list(live.values())


def replay_matches_materialized(conn: sqlite3.Connection) -> bool:
    """自检：日志重放结果与物化现行块逐块相同。"""
    if current_batch_id(conn) is None:
        return not conn.execute("SELECT 1 FROM blocks_current LIMIT 1").fetchone()
    materialized = {(b.row, b.col, b.size, b.tmax, b.tmin, b.count)
                    for b in load_current_blocks(conn)}
    replayed = {(b.row, b.col, b.size, b.tmax, b.tmin, b.count)
                for b in replay_current_blocks(conn)}
    return materialized == replayed


# --------------------------- 内部工具 ------------------------------

class _MemRow:
    """与 sqlite3.Row 同接口（按列名取值）的轻量行对象。"""

    def __init__(self, coord: quadtree.Coord, stat: quadtree.NodeStat, tol: int):
        r, c, size = coord
        measured, unmeasured, tmin, tmax = stat
        self._d = {
            "row_idx": r, "col_idx": c, "side": size,
            "measured_cnt": measured, "unmeasured_cnt": unmeasured,
            "temp_min": tmin, "temp_max": tmax,
            "is_valid": 1 if quadtree.stat_is_valid(stat, tol) else 0,
        }

    def __getitem__(self, key):
        return self._d[key]


def _new_batch(conn: sqlite3.Connection, reason: str,
               tol: int, block_count: int) -> int:
    conn.execute("UPDATE batch SET is_current=0 WHERE grid_id=1")
    cur = conn.execute(
        "INSERT INTO batch(grid_id,reason,is_current,tol,block_count,created_at) "
        "VALUES(1,?,1,?,?,?)", (reason, tol, block_count, _now()))
    return cur.lastrowid


def _log(conn: sqlite3.Connection, kind: str, detail: str) -> None:
    conn.execute("INSERT INTO event_log(ts,kind,detail) VALUES(?,?,?)",
                 (_now(), kind, detail))


def _materialize_subtree(node_rows, coord, adds, leaf_need) -> None:
    """
    祖先合法块被拆除后，为未受影响子树按现存结构产出块意图：
    size>=2 的合法节点直接补发；非法节点下钻；到 size=2 仍非法时，
    把四个叶子坐标记入 leaf_need，由调用方统一批量读温度后补发。
    """
    r, c, size = coord
    row = node_rows[coord]
    if row["is_valid"] == 1:
        adds.append(Block(r, c, size, row["temp_max"], row["temp_min"],
                          row["measured_cnt"]))
        return
    if size == 2:
        for dr in (0, 1):
            for dc in (0, 1):
                leaf_need.add((r + dr, c + dc))
        return
    for child in quadtree.children_of(coord):
        _materialize_subtree(node_rows, child, adds, leaf_need)


def _collect_delta(node_rows, coord, affected, changed_leaves,
                   old_valid: Dict[Tuple[int, int, int], bool],
                   adds: List[Block], tombstones: List,
                   leaf_need: set, split: bool = False) -> None:
    """
    自受影响根（恒为整表根）向下收集本次编辑的块增量。

    split=True 表示该节点上方存在“旧合法、新非法”的祖先：
    旧整块已被区域墓碑拆除，本区域保留下来的块都必须显式 add。

    叶子不直接读温度：需要作为块输出的叶子坐标统一记入 leaf_need，
    由调用方在遍历结束后批量加载，保证叶子读取量与实际块输出成正比。

    - 新合法：区域墓碑 + 一个 add（吸收内部全部旧块），结束下钻；
    - 新非法、旧合法（或 split）：本块被拆除，置 split=True 继续下钻；
    - 新非法且一直非法：未受影响孩子旧块仍有效、跳过，仅受影响路径递归；
    - size=2 非法：四个叶子按“改动/拆分补发”记入 leaf_need，改动叶子另记墓碑。
    """
    r, c, size = coord

    # size=2 是最小内部节点，其孩子为叶子，在此终止递归
    if size == 2:
        row = node_rows[coord]
        if row["is_valid"] == 1:
            tombstones.append(("region", r, c, size))
            adds.append(Block(r, c, size, row["temp_max"], row["temp_min"],
                              row["measured_cnt"]))
            return
        now_split = split or bool(old_valid.get(coord, False))
        if now_split and not split:
            tombstones.append((r, c, size))
        for dr in (0, 1):
            for dc in (0, 1):
                pos = (r + dr, c + dc)
                if pos in changed_leaves:
                    tombstones.append((pos[0], pos[1], 1))
                    leaf_need.add(pos)
                elif now_split:
                    leaf_need.add(pos)
        return

    # n==1 退化情形
    if size == 1:
        if (r, c) in changed_leaves:
            tombstones.append((r, c, 1))
            leaf_need.add((r, c))
        elif split:
            leaf_need.add((r, c))
        return

    row = node_rows[coord]
    if row["is_valid"] == 1:
        tombstones.append(("region", r, c, size))
        adds.append(Block(r, c, size, row["temp_max"], row["temp_min"],
                          row["measured_cnt"]))
        return

    now_split = split or bool(old_valid.get(coord, False))
    if now_split and not split:
        tombstones.append((r, c, size))

    for child in quadtree.children_of(coord):
        cr, cc, cs = child
        if cs == 1:
            # 父节点必为 size=2，已在上面的基例处理，理论上不可达
            pos = (cr, cc)
            if pos in changed_leaves:
                tombstones.append((cr, cc, 1))
                leaf_need.add(pos)
            elif now_split:
                leaf_need.add(pos)
            continue
        if child not in affected:
            if now_split:
                _materialize_subtree(node_rows, child, adds, leaf_need)
            continue
        _collect_delta(node_rows, child, affected, changed_leaves,
                       old_valid, adds, tombstones, leaf_need, now_split)


def _write_base_blocks(conn: sqlite3.Connection, bid: int,
                       blocks: List[Block]) -> None:
    conn.executemany(
        "INSERT INTO block_op(batch_id,row_idx,col_idx,side,op,"
        "temp_max,temp_min,cell_count) VALUES(?,?,?,?,'base',?,?,?)",
        [(bid, b.row, b.col, b.size, b.tmax, b.tmin, b.count) for b in blocks],
    )


def _rebuild_materialized(conn: sqlite3.Connection,
                          blocks: List[Block]) -> None:
    """整体重建现行块物化表（用于上传、改温差这类全局重建）。"""
    conn.execute("DELETE FROM blocks_current")
    conn.executemany(
        "INSERT INTO blocks_current(row_idx,col_idx,side,temp_max,temp_min,cell_count) "
        "VALUES(?,?,?,?,?,?)",
        [(b.row, b.col, b.size, b.tmax, b.tmin, b.count) for b in blocks],
    )


def _apply_delta_materialized(conn: sqlite3.Connection,
                              exact_tomb: List[Tuple[int, int, int]],
                              range_tomb: List[Tuple[int, int, int]],
                              adds: List[Block]) -> Dict[str, object]:
    """
    把局部块增量应用到物化表，只用物化表做局部核算，不回查历史日志。

    范围作废先执行：只删除严格位于区域内部的行，区域原点上的块是同批
    新写的大块，保留并由后续 UPSERT 更新。实际删到行的区域才留下一条
    范围作废记录；精确坐标只在物化表中仍存在时才留下逐块记录。
    这样记录数与真实删除量一致，合并成片小块时每个区域最多一条。

    返回 {"removed": 实际删除块数, "ranges": 写出范围记录, "exacts": 写出逐块记录}。
    """
    removed = 0
    used_ranges: List[Tuple[int, int, int]] = []
    for r, c, s in range_tomb:
        # 块都按四叉树对齐，不会与区域部分相交；用起点范围走主键索引，
        # 再保留完整包含条件作兜底。区域原点上的同批大块由 side<s 排除。
        cur = conn.execute(
            "DELETE FROM blocks_current "
            "WHERE row_idx>=? AND row_idx<? AND col_idx>=? AND col_idx<? "
            "AND side<? "
            "AND row_idx+side<=? AND col_idx+side<=?",
            (r, r + s, c, c + s, s, r + s, c + s))
        if cur.rowcount:
            removed += cur.rowcount
            used_ranges.append((r, c, s))

    used_exacts = _live_positions(conn, exact_tomb)
    if used_exacts:
        conn.executemany(
            "DELETE FROM blocks_current "
            "WHERE row_idx=? AND col_idx=? AND side=?",
            used_exacts)
        removed += len(used_exacts)

    conn.executemany(
        "INSERT INTO blocks_current(row_idx,col_idx,side,temp_max,temp_min,cell_count) "
        "VALUES(?,?,?,?,?,?) "
        "ON CONFLICT(row_idx,col_idx,side) DO UPDATE SET "
        "temp_max=excluded.temp_max, temp_min=excluded.temp_min, "
        "cell_count=excluded.cell_count",
        [(b.row, b.col, b.size, b.tmax, b.tmin, b.count) for b in adds],
    )
    return {"removed": removed, "ranges": used_ranges, "exacts": used_exacts}


def _live_positions(conn: sqlite3.Connection,
                    positions: List[Tuple[int, int, int]]
                    ) -> List[Tuple[int, int, int]]:
    """
    从物化表里筛出仍然存在的坐标（局部核算，不查历史日志）。
    用临时表按主键 JOIN，代价与待查坐标数成正比，与物化表总行数无关。
    """
    wanted = list(dict.fromkeys(positions))
    if not wanted:
        return []
    conn.execute("CREATE TEMP TABLE t_want_block(r INT, c INT, s INT)")
    try:
        conn.executemany("INSERT INTO t_want_block VALUES(?,?,?)", wanted)
        rows = conn.execute(
            "SELECT b.row_idx, b.col_idx, b.side FROM blocks_current b "
            "JOIN t_want_block w ON b.row_idx=w.r AND b.col_idx=w.c AND b.side=w.s"
        ).fetchall()
    finally:
        conn.execute("DROP TABLE t_want_block")
    return [(r[0], r[1], r[2]) for r in rows]


# ----------------------------- 服务 --------------------------------

def create_grid(conn: sqlite3.Connection, n: int,
                data: Sequence[Sequence[Optional[int]]],
                tol: int) -> Dict[str, int]:
    """全新上传：清空旧数据，整体建表、建四叉树、建首批次与整套 base 块。"""
    metrics = {"nodes_recomputed": 0, "blocks_added": 0, "blocks_removed": 0}
    with transaction(conn):
        conn.execute("DELETE FROM block_op")
        conn.execute("DELETE FROM batch")
        conn.execute("DELETE FROM nodes")
        conn.execute("DELETE FROM cells")
        conn.execute("DELETE FROM grid")

        now = _now()
        conn.execute(
            "INSERT INTO grid(id,side,tol,created_at,updated_at) "
            "VALUES(1,?,?,?,?)", (n, tol, now, now))
        conn.executemany(
            "INSERT INTO cells(grid_id,row_idx,col_idx,temp) VALUES(1,?,?,?)",
            [(r, c, data[r][c]) for r in range(n) for c in range(n)])
        cells = {(r, c): data[r][c] for r in range(n) for c in range(n)}

        stats = quadtree.compute_all_node_stats(cells, n)
        conn.executemany(
            "INSERT INTO nodes(grid_id,row_idx,col_idx,side,measured_cnt,"
            "unmeasured_cnt,temp_min,temp_max,is_valid) "
            "VALUES(1,?,?,?,?,?,?,?,?)",
            [(*coord, *stat, 1 if quadtree.stat_is_valid(stat, tol) else 0)
             for coord, stat in stats.items()])
        metrics["nodes_recomputed"] = len(stats)

        blocks = quadtree.collect_blocks(stats, cells, n, tol)
        bid = _new_batch(conn, "upload", tol, len(blocks))
        _write_base_blocks(conn, bid, blocks)
        _rebuild_materialized(conn, blocks)
        metrics["blocks_added"] = len(blocks)
        _log(conn, "upload",
             f"新建方阵 {n}x{n}，tol={tol}，块数={len(blocks)}")
    return metrics


def update_tolerance(conn: sqlite3.Connection, tol: int) -> Dict[str, int]:
    """改允许温差：节点统计不变，只重算合法标志，并以整套 base 块重建新批次。"""
    grid = get_grid(conn)
    if grid is None:
        raise RuntimeError("尚未上传方阵")
    n = grid["side"]
    metrics = {"nodes_recomputed": 0, "blocks_added": 0, "blocks_removed": 0}
    with transaction(conn):
        conn.execute("UPDATE grid SET tol=?, updated_at=? WHERE id=1",
                     (tol, _now()))
        conn.execute(
            "UPDATE nodes SET is_valid = CASE WHEN unmeasured_cnt=0 "
            "AND measured_cnt>0 AND temp_max-temp_min<=? THEN 1 ELSE 0 END",
            (tol,))
        metrics["nodes_recomputed"] = conn.execute(
            "SELECT COUNT(*) FROM nodes WHERE grid_id=1").fetchone()[0]

        cells = load_cells(conn)
        node_rows = load_node_map(conn)
        stats = {k: (v["measured_cnt"], v["unmeasured_cnt"],
                     v["temp_min"], v["temp_max"])
                 for k, v in node_rows.items()}
        blocks = quadtree.collect_blocks(stats, cells, n, tol)
        # 温差是全局变化：以整套 base 开新批次，重放从这批发车，
        # 旧 delta 链不再参与；但历史记录原样保留，不做截断。
        bid = _new_batch(conn, "tolerance", tol, len(blocks))
        _write_base_blocks(conn, bid, blocks)
        _rebuild_materialized(conn, blocks)
        metrics["blocks_added"] = len(blocks)
        _log(conn, "tolerance", f"允许温差改为 {tol}，块数={len(blocks)}")
    return metrics


def apply_edits(conn: sqlite3.Connection,
                edits: Dict[Tuple[int, int], Optional[int]]) -> Dict[str, int]:
    """
    批量补测/清空。edits 值为 int（补测）或 None（清空）。

    只重算包含改动格的节点，块变更以 add/tombstone 增量写入新批次，
    写入量与改动子树规模成正比，不再复制全量块。
    """
    grid = get_grid(conn)
    if grid is None:
        raise RuntimeError("尚未上传方阵")
    n, tol = grid["side"], grid["tol"]

    with transaction(conn):
        # 只取改动格的现值做校验与空改剔除，避免全表读取 cells。
        # 用临时表按 cells 主键点查，避免 OR 条件在大表上退化为扫描。
        edit_keys = list(edits.keys())
        for (r, c) in edit_keys:
            if not (0 <= r < n and 0 <= c < n):
                raise ValueError(f"坐标 ({r + 1},{c + 1}) 超出 {n}x{n} 网格")
        conn.execute("CREATE TEMP TABLE t_edit(r INT, c INT)")
        try:
            conn.executemany("INSERT INTO t_edit VALUES(?,?)", edit_keys)
            old_rows = conn.execute(
                "SELECT ce.row_idx, ce.col_idx, ce.temp FROM cells ce "
                "JOIN t_edit e ON ce.row_idx=e.r AND ce.col_idx=e.c "
                "WHERE ce.grid_id=1").fetchall()
        finally:
            conn.execute("DROP TABLE t_edit")
        old_vals = {(r["row_idx"], r["col_idx"]): r["temp"] for r in old_rows}

        real: Dict[Tuple[int, int], Optional[int]] = {}
        for (r, c), val in edits.items():
            if val is not None and not isinstance(val, int):
                raise ValueError(f"坐标 ({r + 1},{c + 1}) 的温度必须是整数")
            if old_vals.get((r, c)) != val:
                real[(r, c)] = val
        if not real:
            _log(conn, "edit", "提交的改动与现值一致，未产生新批次")
            return {"nodes_recomputed": 0, "blocks_added": 0,
                    "blocks_removed": 0, "tombstone_records": 0,
                    "tombstone_ranges": 0, "changed_cells": 0}

        # 1) 批量写格子
        conn.executemany(
            "UPDATE cells SET temp=? WHERE grid_id=1 AND row_idx=? AND col_idx=?",
            [(val, r, c) for (r, c), val in real.items()])
        # 后续叶子统计只需改动格的值；未改动叶子的统计经由 nodes 表获得
        cells = dict(old_vals)
        cells.update(real)

        # 2) 受影响祖先并集，自底向上重算（n==1 无内部节点，单独处理）
        affected: set = set()
        for (r, c) in real:
            affected.update(quadtree.ancestor_nodes(r, c, n))

        if n == 1:
            v = cells.get((0, 0))
            bid = _new_batch(conn, "edit", tol, 1 if v is not None else 0)
            cur = conn.execute(
                "DELETE FROM blocks_current WHERE row_idx=0 AND col_idx=0 AND side=1")
            removed = cur.rowcount
            # 仅有旧块被删时才写作废记录；首次补测不产生空作废
            if removed:
                conn.execute(
                    "INSERT INTO block_op(batch_id,row_idx,col_idx,side,op,"
                    "temp_max,temp_min,cell_count) "
                    "VALUES(?,0,0,1,'tombstone',NULL,NULL,NULL)", (bid,))
            added = 0
            if v is not None:
                conn.execute(
                    "INSERT INTO block_op(batch_id,row_idx,col_idx,side,op,"
                    "temp_max,temp_min,cell_count) "
                    "VALUES(?,0,0,1,'add',?,?,1)", (bid, v, v))
                conn.execute(
                    "INSERT INTO blocks_current(row_idx,col_idx,side,temp_max,temp_min,cell_count) "
                    "VALUES(0,0,1,?,?,1)", (v, v))
                added = 1
            conn.execute("UPDATE grid SET updated_at=? WHERE id=1", (_now(),))
            _log(conn, "edit", f"n=1 单格改动，现行块数 {1 if v is not None else 0}")
            return {"nodes_recomputed": 0, "blocks_added": added,
                    "blocks_removed": removed, "tombstone_records": 1 if removed else 0,
                    "tombstone_ranges": 0, "changed_cells": 1}

        ordered = sorted(affected, key=lambda x: x[2])
        # 重算节点还要读其孩子，加载集合 = 受影响节点 ∪ 其 size>=2 子节点
        load_set = set(affected)
        for coord in ordered:
            for kid in quadtree.children_of(coord):
                if kid[2] >= 2:
                    load_set.add(kid)
        # 用临时表 JOIN 按主键索引取节点，避免大串 OR 触发全表扫描
        conn.execute("CREATE TEMP TABLE t_want_nodes(r INT, c INT, s INT)")
        try:
            conn.executemany("INSERT INTO t_want_nodes VALUES(?,?,?)",
                             sorted(load_set))
            node_rows = {
                (r["row_idx"], r["col_idx"], r["side"]): r
                for r in conn.execute(
                    "SELECT n.row_idx, n.col_idx, n.side, n.measured_cnt, "
                    "n.unmeasured_cnt, n.temp_min, n.temp_max, n.is_valid "
                    "FROM nodes n JOIN t_want_nodes w "
                    "ON n.row_idx=w.r AND n.col_idx=w.c AND n.side=w.s "
                    "WHERE n.grid_id=1")
            }
        finally:
            conn.execute("DROP TABLE t_want_nodes")

        # 重算 size=2 节点要读其叶子真实温度；只加载受影响的 size=2
        # 节点覆盖的叶子，规模 ≤4×改动数。按 cells 主键点查，走索引。
        leaf_quads = sorted(coord for coord in affected if coord[2] == 2)
        if leaf_quads:
            leaf_positions = sorted({
                (rr + dr, cc + dc)
                for (rr, cc, _ss) in leaf_quads
                for dr in (0, 1) for dc in (0, 1)
            })
            conn.execute("CREATE TEMP TABLE t_want_leaf2(r INT, c INT)")
            try:
                conn.executemany("INSERT INTO t_want_leaf2 VALUES(?,?)",
                                 leaf_positions)
                for row in conn.execute(
                        "SELECT ce.row_idx, ce.col_idx, ce.temp FROM cells ce "
                        "JOIN t_want_leaf2 w ON ce.row_idx=w.r AND ce.col_idx=w.c "
                        "WHERE ce.grid_id=1"):
                    cells[(row["row_idx"], row["col_idx"])] = row["temp"]
            finally:
                conn.execute("DROP TABLE t_want_leaf2")

        def stat_of(coord: quadtree.Coord) -> quadtree.NodeStat:
            r2, c2, s2 = coord
            if s2 == 1:
                v = cells.get((r2, c2))
                return (1, 0, v, v) if v is not None else (0, 1, None, None)
            row = node_rows[coord]
            return (row["measured_cnt"], row["unmeasured_cnt"],
                    row["temp_min"], row["temp_max"])

        # 重算前的合法标志，供块增量判断“旧合法块是否被拆除”。
        # 覆盖整个加载集合：未受影响兄弟在补发子树时也要读旧合法性。
        old_valid = {coord: (row["is_valid"] == 1)
                     for coord, row in node_rows.items()}

        node_upserts = []
        for coord in ordered:
            kids = quadtree.children_of(coord)
            stat = stat_of(kids[0])
            for kid in kids[1:]:
                stat = quadtree._merge_stats(stat, stat_of(kid))
            r0, c0, s0 = coord
            measured, unmeasured, tmin, tmax = stat
            valid = 1 if quadtree.stat_is_valid(stat, tol) else 0
            node_upserts.append(
                (r0, c0, s0, measured, unmeasured, tmin, tmax, valid))
            node_rows[coord] = _MemRow(coord, stat, tol)
        conn.executemany(
            "INSERT INTO nodes(grid_id,row_idx,col_idx,side,measured_cnt,"
            "unmeasured_cnt,temp_min,temp_max,is_valid) "
            "VALUES(1,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(grid_id,row_idx,col_idx,side) DO UPDATE SET "
            "measured_cnt=excluded.measured_cnt, "
            "unmeasured_cnt=excluded.unmeasured_cnt, "
            "temp_min=excluded.temp_min, temp_max=excluded.temp_max, "
            "is_valid=excluded.is_valid", node_upserts)

        # 3) 内存收集块增量（tombstones 含精确 3 元组与 ('region',r,c,s) 区域元组）
        adds: List[Block] = []
        tombstones: List = []
        leaf_need: set = set()
        _collect_delta(node_rows, (0, 0, n), affected,
                       set(real.keys()), old_valid, adds, tombstones, leaf_need)

        # 收集器只记录叶子意图；这里按实际需要批量读温度，
        # 实测格产出 size=1 块，未测格不产出。读取量与实际块输出成正比。
        leaf_list = sorted(leaf_need)
        leaf_temp: Dict[Tuple[int, int], Optional[int]] = {}
        if leaf_list:
            conn.execute("CREATE TEMP TABLE t_want_leaf(r INT, c INT)")
            try:
                conn.executemany("INSERT INTO t_want_leaf VALUES(?,?)",
                                 leaf_list)
                for row in conn.execute(
                        "SELECT ce.row_idx, ce.col_idx, ce.temp FROM cells ce "
                        "JOIN t_want_leaf w ON ce.row_idx=w.r AND ce.col_idx=w.c "
                        "WHERE ce.grid_id=1"):
                    leaf_temp[(row["row_idx"], row["col_idx"])] = row["temp"]
            finally:
                conn.execute("DROP TABLE t_want_leaf")
        for pos in leaf_list:
            v = leaf_temp.get(pos)
            if v is not None:
                adds.append(Block(pos[0], pos[1], 1, v, v, 1))

        exact_tomb = [t for t in tombstones if t[0] != "region"]
        region_tomb = [(t[1], t[2], t[3]) for t in tombstones if t[0] == "region"]

        # 同批次同坐标既有精确墓碑又有 add 时，add 本身即覆盖旧状态，取消该墓碑
        add_positions = {(b.row, b.col, b.size) for b in adds}
        exact_tomb = list(dict.fromkeys(
            t for t in exact_tomb if t not in add_positions))
        # 每个合并区域最多一条范围作废记录，按坐标去重
        region_tomb = list(dict.fromkeys(region_tomb))

        # 4) 建新批次，追加局部变更。物化表先做局部删除核算，再据实际
        #    删除结果写记录：没删到块的作废意图不落日志，避免无意义记录。
        bid = _new_batch(conn, "edit", tol, 0)
        applied = _apply_delta_materialized(conn, exact_tomb, region_tomb, adds)
        used_ranges = applied["ranges"]
        used_exacts = applied["exacts"]
        # 先写作废记录、再写 add：重放按 id 顺序进行，
        # 同批新写的大块因此不会被自己的范围记录带走。
        conn.executemany(
            "INSERT INTO block_op(batch_id,row_idx,col_idx,side,op,"
            "temp_max,temp_min,cell_count) "
            "VALUES(?,?,?,?,'tombstone_range',NULL,NULL,NULL)",
            [(bid, r, c, s) for (r, c, s) in used_ranges])
        conn.executemany(
            "INSERT INTO block_op(batch_id,row_idx,col_idx,side,op,"
            "temp_max,temp_min,cell_count) "
            "VALUES(?,?,?,?,'tombstone',NULL,NULL,NULL)",
            [(bid, r, c, s) for (r, c, s) in used_exacts])
        conn.executemany(
            "INSERT INTO block_op(batch_id,row_idx,col_idx,side,op,"
            "temp_max,temp_min,cell_count) "
            "VALUES(?,?,?,?,'add',?,?,?)",
            [(bid, b.row, b.col, b.size, b.tmax, b.tmin, b.count)
             for b in adds])

        count = current_block_count(conn)
        conn.execute("UPDATE batch SET block_count=? WHERE id=?", (count, bid))
        conn.execute("UPDATE grid SET updated_at=? WHERE id=1", (_now(),))
        record_count = len(used_ranges) + len(used_exacts)
        _log(conn, "edit",
             f"改动 {len(real)} 格，重算节点 {len(ordered)} 个，"
             f"add {len(adds)} / 删除块 {applied['removed']} / "
             f"作废记录 {record_count}（区域 {len(used_ranges)}），"
             f"现行块数 {count}")
        return {"nodes_recomputed": len(ordered),
                "blocks_added": len(adds),
                "blocks_removed": applied["removed"],
                "tombstone_records": record_count,
                "tombstone_ranges": len(used_ranges),
                "changed_cells": len(real)}
