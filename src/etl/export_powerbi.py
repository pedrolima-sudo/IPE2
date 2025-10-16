from __future__ import annotations
from pathlib import Path
import polars as pl
from loguru import logger
from ..utils.settings import PARQUET_OUT_DIR


def export_parquet(df: pl.DataFrame, dataset_name: str = "egressos") -> Path:
    out_dir = PARQUET_OUT_DIR / dataset_name
    out_dir.mkdir(parents=True, exist_ok=True)

    # Grava arquivo único + particionado por faixa_etaria (útil para filtros no Power BI)
    file_all = out_dir / f"{dataset_name}.parquet"
    logger.info(f"Gravando Parquet consolidado: {file_all}")
    df.write_parquet(file_all)

    part_dir = out_dir / "partition_faixa_etaria"
    part_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Gravando Parquet particionado por faixa_etaria em: {part_dir}")
    for faixa, pdf in df.partition_by("faixa_etaria", as_dict=True).items():
        safe = str(faixa).replace("/", "-")
        (part_dir / f"faixa={safe}.parquet").write_bytes(pdf.write_parquet())

    return out_dir
