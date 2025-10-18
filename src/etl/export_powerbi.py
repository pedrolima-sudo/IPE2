"""
MÓDULO PARA EXPORTAÇÃO DE DADOS DE EGRESSOS EM FORMATO PARQUET PARA POWER BI
================================================================================

Este módulo exporta os dados de egressos transformados em arquivos Parquet,
particionados por faixa etária, otimizados para consumo no Power BI.

Autor: Pedro Henrique Lima Silva
Data de criação: 15/10/2025
Última modificação: 16/10/2025
"""

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
        safe = "NULL" if faixa is None else str(faixa).replace("/", "-")
        out_file = part_dir / f"faixa={safe}.parquet"
        pdf.write_parquet(out_file)


    return out_dir
