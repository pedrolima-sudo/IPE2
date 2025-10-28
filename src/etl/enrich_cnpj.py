# -*- coding: utf-8 -*-
"""Identify alumni who are likely partners (sócios) using CPF fragments and name similarity."""

from __future__ import annotations

from typing import Iterable

import polars as pl
from loguru import logger
from rapidfuzz import fuzz

from ..utils.clean import normalize_name
from ..utils.settings import SILVER_DIR


SOCIOS_PARQUET = SILVER_DIR / "socios.parquet"


def _middle_cpf_fragment(value: str | None) -> str:
    """Extract the six middle digits of a CPF string. Returns empty string when unavailable."""
    if not value:
        return ""
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) < 11:
        return ""
    return digits[3:9]


def _load_socios_base() -> pl.DataFrame | None:
    if not SOCIOS_PARQUET.exists():
        logger.warning("Arquivo socios.parquet não encontrado em %s", SOCIOS_PARQUET)
        return None
    socios = pl.read_parquet(SOCIOS_PARQUET, columns=["cpf_fragment", "nome"])
    socios = socios.filter(pl.col("cpf_fragment").is_not_null() & (pl.col("cpf_fragment") != ""))
    if socios.is_empty():
        logger.warning("Base de sócios encontrada, porém sem registros válidos de cpf_fragment.")
        return None
    socios = socios.with_columns([
        pl.col("cpf_fragment").cast(pl.Utf8),
        pl.col("nome").map_elements(normalize_name, return_dtype=pl.Utf8).alias("nome_norm"),
    ])
    grouped = (
        socios.group_by("cpf_fragment")
        .agg(pl.col("nome_norm").drop_nulls().unique().alias("socios_nomes_norm"))
    )
    return grouped


def _has_similar_name(nome: str, candidatos: Iterable[str], *, threshold: int = 90) -> bool:
    if not nome:
        return False
    for candidato in candidatos:
        if fuzz.token_set_ratio(nome, candidato) >= threshold:
            return True
    return False


def mark_founders(df: pl.DataFrame) -> pl.DataFrame:
    """Annotate alumni DataFrame with partner flags based on CPF fragment and name similarity."""
    required_cols = [
        "id_pessoa",
        "matricula",
        "nome",
        "nome_norm",
        "idade",
        "faixa_etaria",
        "data_nascimento",
        "data_ingresso",
        "data_formacao",
        "ultimo_curso",
        "nivel",
        "codigo_curso",
    ]

    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        logger.warning("Colunas esperadas ausentes no DataFrame inicial: %s", ", ".join(missing))
        for col in missing:
            df = df.with_columns(pl.lit(None).alias(col))

    socios = _load_socios_base()
    if socios is None:
        logger.warning("Impossível comparar sócios; retornando flags vazias.")
        return df.with_columns([
            pl.lit(False).alias("socio_cpf"),
            pl.lit(False).alias("socio_nome"),
            pl.lit(False).alias("socio"),
        ]).select(required_cols + ["socio_cpf", "socio_nome", "socio"])

    df = df.with_columns(
        pl.col("cpf_limpo").map_elements(_middle_cpf_fragment, return_dtype=pl.Utf8).alias("cpf_fragment")
    )

    df = df.join(socios, on="cpf_fragment", how="left")
    df = df.with_columns(
        pl.when(pl.col("socios_nomes_norm").is_null())
        .then(pl.lit([], dtype=pl.List(pl.Utf8)))
        .otherwise(pl.col("socios_nomes_norm"))
        .alias("socios_nomes_norm")
    )

    df = df.with_columns(
        (pl.col("socios_nomes_norm").list.len().fill_null(0) > 0).alias("socio_cpf")
    )

    def _map_name_match(row: dict) -> bool:
        nomes = row.get("socios_nomes_norm") or []
        if not nomes:
            return False
        return _has_similar_name(row.get("nome_norm") or "", nomes)

    df = df.with_columns(
        pl.struct(["nome_norm", "socios_nomes_norm"]).map_elements(
            _map_name_match, return_dtype=pl.Boolean
        ).alias("socio_nome")
    )

    df = df.with_columns((pl.col("socio_cpf") & pl.col("socio_nome")).alias("socio"))

    output_cols = [
        "id_pessoa",
        "matricula",
        "nome",
        "nome_norm",
        "idade",
        "faixa_etaria",
        "data_nascimento",
        "data_ingresso",
        "data_formacao",
        "ultimo_curso",
        "nivel",
        "codigo_curso",
        "socio_cpf",
        "socio_nome",
        "socio",
    ]

    return df.select(output_cols)
