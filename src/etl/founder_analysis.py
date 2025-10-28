"""Identify founder partners by comparing association and company start dates."""

from __future__ import annotations

from pathlib import Path

import polars as pl
from typing import Any, Dict, List

from loguru import logger

MAX_FOUNDER_GAP_DAYS = 7


def _parse_date(expr: pl.Expr) -> pl.Expr:
    """Parse date strings commonly found in Receita datasets."""
    expr_utf8 = expr.cast(pl.Utf8)
    parsed_iso = expr_utf8.str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
    parsed_compact = expr_utf8.str.strptime(pl.Date, format="%Y%m%d", strict=False)
    return parsed_iso.fill_null(parsed_compact)


def label_socios_fundadores(
    egressos_parquet: Path,
    empresas_parquet: Path,
    *,
    max_gap_days: int = MAX_FOUNDER_GAP_DAYS,
) -> Path:
    """Annotate the alumni parquet with founder flags based on temporal heuristics."""
    if max_gap_days < 0:
        raise ValueError("max_gap_days deve ser não negativo.")
    if not egressos_parquet.exists():
        raise FileNotFoundError(f"Parquet de egressos não encontrado: {egressos_parquet}")
    if not empresas_parquet.exists():
        raise FileNotFoundError(f"Parquet de empresas não encontrado: {empresas_parquet}")

    logger.info(
        "Iniciando label de sócios fundadores. Egressos: %s | Empresas: %s",
        egressos_parquet,
        empresas_parquet,
    )

    egressos = pl.read_parquet(egressos_parquet)
    required_cols = {"id_pessoa", "socio", "cnpj_basico", "data_associacao"}
    missing = required_cols - set(egressos.columns)
    if missing:
        raise ValueError(
            f"Colunas necessárias ausentes no parquet de egressos: {', '.join(sorted(missing))}"
        )

    empresas = pl.read_parquet(empresas_parquet, columns=["cnpj_basico", "data_inicio_atividade"])
    if empresas.is_empty():
        logger.warning("Dataset de empresas vazio. Flags de fundador não serão alteradas.")
        empresas = pl.DataFrame({"cnpj_basico": [], "data_inicio_atividade": []})

    empresas = empresas.with_columns(
        _parse_date(pl.col("data_inicio_atividade")).alias("data_inicio_atividade_date")
    )

    socios_cols = ["id_pessoa", "socio", "cnpj_basico", "data_associacao"]
    socios_long = (
        egressos.select(socios_cols)
        .explode(["cnpj_basico", "data_associacao"])
        .filter(
            pl.col("socio")
            & pl.col("cnpj_basico").is_not_null()
            & (pl.col("cnpj_basico") != "")
            & pl.col("data_associacao").is_not_null()
            & (pl.col("data_associacao") != "")
        )
    )

    if socios_long.is_empty():
        logger.info("Nenhum sócio identificado com dados suficientes para análise temporal.")
        fundador_dtype = pl.List(
            pl.Struct(
                {
                    "cnpj_basico": pl.Utf8,
                    "data_associacao": pl.Date,
                    "data_inicio_atividade": pl.Date,
                    "diferenca_dias": pl.Int32,
                }
            )
        )
        egressos = egressos.with_columns([
            pl.lit([], dtype=fundador_dtype).alias("fundador_relacoes"),
            pl.lit([], dtype=pl.List(pl.Utf8)).alias("cnpj_fundador"),
            pl.lit(False).alias("socio_fundador"),
        ])
        egressos.write_parquet(egressos_parquet)
        logger.success("Parquet de egressos atualizado sem fundadores identificados.")
        return egressos_parquet

    socios_long = socios_long.with_columns(
        _parse_date(pl.col("data_associacao")).alias("data_associacao_date")
    )

    joined = socios_long.join(empresas, on="cnpj_basico", how="left")
    joined = joined.with_columns(
        (
            pl.col("data_associacao_date") - pl.col("data_inicio_atividade_date")
        ).dt.days().abs().alias("diferenca_dias")
    )

    founder_condition = (
        pl.col("diferenca_dias").is_not_null()
        & (pl.col("diferenca_dias") <= max_gap_days)
        & pl.col("data_inicio_atividade_date").is_not_null()
        & pl.col("data_associacao_date").is_not_null()
    )

    fundador_relacoes_dtype = pl.List(
        pl.Struct(
            {
                "cnpj_basico": pl.Utf8,
                "data_associacao": pl.Date,
                "data_inicio_atividade": pl.Date,
                "diferenca_dias": pl.Int32,
            }
        )
    )

    founders = (
        joined.filter(founder_condition)
        .group_by("id_pessoa")
        .agg(
            pl.struct(
                [
                    pl.col("cnpj_basico"),
                    pl.col("data_associacao_date").alias("data_associacao"),
                    pl.col("data_inicio_atividade_date").alias("data_inicio_atividade"),
                    pl.col("diferenca_dias"),
                ]
            ).alias("fundador_relacoes")
        )
    )

    if founders.is_empty():
        logger.info("Nenhum sócio atende ao critério de fundador dentro da janela %s dias.", max_gap_days)
        egressos = egressos.with_columns([
            pl.lit([], dtype=fundador_relacoes_dtype).alias("fundador_relacoes"),
            pl.lit([], dtype=pl.List(pl.Utf8)).alias("cnpj_fundador"),
            pl.lit(False).alias("socio_fundador"),
        ])
        egressos.write_parquet(egressos_parquet)
        logger.success("Parquet de egressos atualizado sem fundadores identificados.")
        return egressos_parquet

    def _cnpjs_from_relacoes(relacoes: List[Dict[str, Any]]) -> List[str]:
        if not relacoes:
            return []
        return [rel.get("cnpj_basico") for rel in relacoes if rel.get("cnpj_basico")]

    founders = founders.with_columns([
        pl.col("fundador_relacoes").map_elements(
            _cnpjs_from_relacoes, return_dtype=pl.List(pl.Utf8)
        ).alias("cnpj_fundador"),
        (pl.col("fundador_relacoes").list.len() > 0).alias("socio_fundador"),
    ])

    egressos = egressos.join(founders, on="id_pessoa", how="left")
    egressos = egressos.with_columns([
        pl.when(pl.col("fundador_relacoes").is_null())
        .then(pl.lit([], dtype=fundador_relacoes_dtype))
        .otherwise(pl.col("fundador_relacoes"))
        .alias("fundador_relacoes"),
        pl.when(pl.col("cnpj_fundador").is_null())
        .then(pl.lit([], dtype=pl.List(pl.Utf8)))
        .otherwise(pl.col("cnpj_fundador"))
        .alias("cnpj_fundador"),
        pl.when(pl.col("socio_fundador").is_null())
        .then(pl.lit(False))
        .otherwise(pl.col("socio_fundador"))
        .alias("socio_fundador"),
    ])

    egressos.write_parquet(egressos_parquet)
    logger.success(
        "Parquet de egressos atualizado com flags de sócio fundador. Registros %d.",
        egressos.height,
    )
    return egressos_parquet


__all__ = ["label_socios_fundadores", "MAX_FOUNDER_GAP_DAYS"]
