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
    schema = pl.scan_parquet(SOCIOS_PARQUET).schema
    columns = ["cpf_fragment", "nome", "cnpj_basico"]
    if "data_entrada_sociedade" in schema:
        columns.append("data_entrada_sociedade")
    socios = pl.read_parquet(SOCIOS_PARQUET, columns=columns)
    socios = socios.filter(pl.col("cpf_fragment").is_not_null() & (pl.col("cpf_fragment") != ""))
    if socios.is_empty():
        logger.warning("Base de sócios encontrada, porém sem registros válidos de cpf_fragment.")
        return None
    socios = socios.with_columns([
        pl.col("cpf_fragment").cast(pl.Utf8),
        pl.col("nome").map_elements(normalize_name, return_dtype=pl.Utf8).alias("nome_norm"),
        pl.col("cnpj_basico").cast(pl.Utf8),
    ])
    if "data_entrada_sociedade" in socios.columns:
        socios = socios.with_columns(
            pl.col("data_entrada_sociedade").cast(pl.Utf8).alias("data_associacao")
        ).drop("data_entrada_sociedade")
    else:
        socios = socios.with_columns(pl.lit(None).cast(pl.Utf8).alias("data_associacao"))
    socios = socios.select(["cpf_fragment", "nome_norm", "cnpj_basico", "data_associacao"]).unique()
    socios = socios.with_columns(
        pl.struct(["nome_norm", "cnpj_basico", "data_associacao"]).alias("socio_pair")
    )
    grouped = (
        socios.group_by("cpf_fragment")
        .agg([
            pl.col("socio_pair").alias("socio_pairs"),
        ])
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
            pl.lit([], dtype=pl.List(pl.Utf8)).alias("cnpj_basico"),
            pl.lit([], dtype=pl.List(pl.Utf8)).alias("data_associacao"),
        ]).select(required_cols + ["socio_cpf", "socio_nome", "socio", "cnpj_basico", "data_associacao"])

    df = df.with_columns(
        pl.col("cpf_limpo").map_elements(_middle_cpf_fragment, return_dtype=pl.Utf8).alias("cpf_fragment")
    )

    df = df.join(socios, on="cpf_fragment", how="left")
    socio_pair_dtype = pl.List(
        pl.Struct({"nome_norm": pl.Utf8, "cnpj_basico": pl.Utf8, "data_associacao": pl.Utf8})
    )
    df = df.with_columns(
        pl.when(pl.col("socio_pairs").is_null())
        .then(pl.lit([], dtype=socio_pair_dtype))
        .otherwise(pl.col("socio_pairs"))
        .alias("socio_pairs")
    )

    df = df.with_columns(
        (pl.col("socio_pairs").list.len().fill_null(0) > 0).alias("socio_cpf")
    )

    def _matched_socios(row: dict) -> dict[str, list[str | None]]:
        nome = row.get("nome_norm") or ""
        pairs = row.get("socio_pairs") or []
        if not pairs or not nome:
            return {"cnpjs": [], "datas": []}
        matched_cnpjs: list[str] = []
        matched_datas: list[str | None] = []
        for pair in pairs:
            candidato = pair.get("nome_norm") or ""
            if _has_similar_name(nome, [candidato]):
                cnpj = pair.get("cnpj_basico") or ""
                if cnpj:
                    matched_cnpjs.append(cnpj)
                    matched_datas.append(pair.get("data_associacao"))
        return {"cnpjs": matched_cnpjs, "datas": matched_datas}

    matched_dtype = pl.Struct(
        {
            "cnpjs": pl.List(pl.Utf8),
            "datas": pl.List(pl.Utf8),
        }
    )
    df = df.with_columns(
        pl.struct(["nome_norm", "socio_pairs"]).map_elements(
            _matched_socios, return_dtype=matched_dtype
        ).alias("matched_socios")
    )

    df = df.with_columns(
        pl.col("matched_socios").struct.field("cnpjs").alias("cnpj_basico"),
        pl.col("matched_socios").struct.field("datas").alias("data_associacao"),
    )
    df = df.drop("matched_socios")

    df = df.with_columns(
        (pl.col("cnpj_basico").list.len().fill_null(0) > 0).alias("socio_nome")
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
        "cnpj_basico",
        "data_associacao",
    ]

    return df.select(output_cols)
