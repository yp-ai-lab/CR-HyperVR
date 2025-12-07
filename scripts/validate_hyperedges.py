#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Iterator

import pandas as pd


def _storage_options(path: str | Path) -> dict | None:
    p = str(path)
    return {"token": "cloud"} if p.startswith("gs://") else None


def _iter_parquet_batches(parquet_path: str | Path, batch_size: int = 200_000) -> Iterator[pd.DataFrame]:
    import pyarrow.parquet as pq
    try:
        import fsspec
    except Exception:
        fsspec = None  # type: ignore
    cols = ["src_kind", "src_id", "dst_kind", "dst_id", "weight"]
    path_str = str(parquet_path)
    if path_str.startswith("gs://") and fsspec is not None:
        with fsspec.open(path_str, "rb") as f:  # type: ignore
            pf = pq.ParquetFile(f)
            for batch in pf.iter_batches(columns=cols, batch_size=batch_size):
                yield batch.to_pandas(types_mapper=None)
    else:
        pf = pq.ParquetFile(path_str)
        for batch in pf.iter_batches(columns=cols, batch_size=batch_size):
            yield batch.to_pandas(types_mapper=None)


async def validate(parquet_path: str, database_url: str, weight_tol: float = 1e-6) -> int:
    import asyncpg

    async def _ensure_tmp_table(conn) -> None:
        await conn.execute(
            """
            CREATE TEMP TABLE IF NOT EXISTS tmp_edges (
              src_kind TEXT,
              src_id BIGINT,
              dst_kind TEXT,
              dst_id BIGINT,
              weight REAL
            ) ON COMMIT PRESERVE ROWS
            """
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS tmp_edges_idx ON tmp_edges(src_kind,src_id,dst_kind,dst_id)"
        )

    async def _load_chunk(conn, df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        part = df[["src_kind", "src_id", "dst_kind", "dst_id", "weight"]].copy()
        rows = list(
            zip(
                part["src_kind"].astype(str),
                part["src_id"].astype(int),
                part["dst_kind"].astype(str),
                part["dst_id"].astype(int),
                part["weight"].astype(float),
            )
        )
        if rows:
            await conn.executemany(
                "INSERT INTO tmp_edges (src_kind, src_id, dst_kind, dst_id, weight) VALUES ($1,$2,$3,$4,$5)",
                rows,
            )
        return len(rows)

    conn = await asyncpg.connect(database_url)
    try:
        await _ensure_tmp_table(conn)
        total = 0
        first = True
        for df in _iter_parquet_batches(parquet_path):
            if first:
                first = False
                need = {"src_kind", "src_id", "dst_kind", "dst_id", "weight"}
                if not need.issubset(df.columns):
                    raise RuntimeError(f"Parquet missing columns: {need - set(df.columns)}")
            total += await _load_chunk(conn, df)
        q_matched_exists = (
            "SELECT COUNT(*) FROM tmp_edges t WHERE EXISTS ("
            "  SELECT 1 FROM hyperedges h WHERE h.src_kind=t.src_kind AND h.src_id=t.src_id "
            "  AND h.dst_kind=t.dst_kind AND h.dst_id=t.dst_id AND ABS(h.weight - t.weight) < $1"
            ")"
        )
        q_missing = (
            "SELECT COUNT(*) FROM tmp_edges t "
            " WHERE NOT EXISTS ("
            "   SELECT 1 FROM hyperedges h WHERE h.src_kind=t.src_kind AND h.src_id=t.src_id "
            "   AND h.dst_kind=t.dst_kind AND h.dst_id=t.dst_id AND ABS(h.weight - t.weight) < $1"
            " )"
        )
        q_extra = (
            "SELECT COUNT(*) FROM hyperedges h WHERE NOT EXISTS ("
            "  SELECT 1 FROM tmp_edges t WHERE h.src_kind=t.src_kind AND h.src_id=t.src_id "
            "  AND h.dst_kind=t.dst_kind AND h.dst_id=t.dst_id AND ABS(h.weight - t.weight) < $1"
            ")"
        )
        matched = await conn.fetchval(q_matched_exists, weight_tol)
        missing = await conn.fetchval(q_missing, weight_tol)
        extra = await conn.fetchval(q_extra, weight_tol)
        print({
            "parquet_edges": int(total),
            "db_matched": int(matched or 0),
            "db_missing": int(missing or 0),
            "db_extra": int(extra or 0),
        })
        return 0 if int(missing or 0) == 0 and int(matched or 0) == int(total) else 1
    finally:
        await conn.close()


def main() -> int:
    import asyncio
    p = os.getenv("PROCESSED_PREFIX") or os.getenv("GCS_PROCESSED_PREFIX") or "data/processed"
    parquet_path = f"{p}/hyperedges.parquet"
    db = os.getenv("DATABASE_URL")
    if not db:
        print("Set DATABASE_URL for validation against DB")
        return 2
    return asyncio.run(validate(parquet_path, db))


if __name__ == "__main__":
    raise SystemExit(main())

