# -*- coding: latin-1 -*-
"""Helpers to enrich the alumni DataFrame with CNPJ information."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import polars as pl
from loguru import logger
from rapidfuzz import fuzz, process

from ..utils.settings import CNPJ_BASE_DIR, SILVER_DIR


def _locate_path(filename: str) -> Path | None:
    for candidate in (
        SILVER_DIR / filename,
        CNPJ_BASE_DIR / filename,
        CNPJ_BASE_DIR.parent / "silver" / filename,
    ):
        if candidate.exists():
            return candidate
    return None


def _load_socios_base() -> pl.DataFrame | None:
    path = _locate_path("socios.parquet")
    if path is None:
        logger.warning("Arquivo socios.parquet não encontrado.")
        return None
    return pl.read_parquet(path)


def _load_socios_by_nome() -> pl.DataFrame | None:
    path = _locate_path("socios_nomes.parquet")
    if path is not None:
        return pl.read_parquet(path)
    path_csv = _locate_path("socios_nomes.csv")
    if path_csv is not None:
        return pl.read_csv(path_csv)
    logger.warning("Arquivo socios_nomes não encontrado.")
    return None


def _load_socios_empresas_map(valid_ids: list[str]) -> pl.DataFrame | None:
    if not valid_ids:
        return None

    socios_path = _locate_path("socios.parquet")
    if socios_path is None:
        logger.warning("socios.parquet ausente; não foi possível mapear empresas.")
        return None

    empresas_path = _locate_path("empresas_basico.parquet")
    if empresas_path is None:
        logger.warning("empresas_basico.parquet ausente; não foi possível enriquecer empresas.")
        return None

    socios_lazy = (
        pl.scan_parquet(socios_path)
        .select(["hash_documento_socio", "cnpj_basico"])
        .filter(pl.col("hash_documento_socio").is_in(valid_ids))
    )
    empresas_lazy = pl.scan_parquet(empresas_path).select(
        [
            "cnpj_basico",
            "cnpj",
            "razao_social",
            "nome_fantasia",
            "uf",
            "municipio_nome",
        ]
    )

    aggregated = (
        socios_lazy
        .join(empresas_lazy, on="cnpj_basico", how="left")
        .group_by("hash_documento_socio")
        .agg([
            pl.col("cnpj").drop_nulls().unique().alias("socios_cnpjs"),
            pl.col("cnpj_basico").drop_nulls().unique().alias("socios_cnpj_basicos"),
            pl.col("razao_social").drop_nulls().unique().alias("socios_empresas_razao"),
            pl.col("nome_fantasia").drop_nulls().unique().alias("socios_empresas_fantasia"),
            pl.col("uf").drop_nulls().unique().alias("socios_empresas_ufs"),
            pl.col("municipio_nome").drop_nulls().unique().alias("socios_empresas_municipios"),
        ])
        .collect()
    )

    if aggregated.is_empty():
        return None
    return aggregated


def _ensure_socios_data(max_files: int = 3) -> None:
    alvo_cpf = SILVER_DIR / "socios.parquet"
    alvo_nome = SILVER_DIR / "socios_nomes.parquet"
    if alvo_cpf.exists() and alvo_nome.exists():
        return
    try:
        logger.warning(
            "Bases de sócios ausentes. Iniciando preparação mínima (--max-files=%d)...",
            max_files,
        )
        subprocess.run(
            [sys.executable, "-m", "src.cnpj.prepare_socios", "--max-files", str(max_files)],
            check=True,
        )
        logger.success("Preparo de sócios concluído.")
    except Exception as exc:
        logger.exception(exc)


def mark_founders(df: pl.DataFrame) -> pl.DataFrame:
    logger.info("Marcando sócios/fundadores a partir das bases locais...")
    _ensure_socios_data(max_files=-1)

    def _fragment_from_cpf(cpf: str | None) -> str:
        if not cpf:
            return ""
        digits = ''.join(ch for ch in str(cpf) if ch.isdigit())
        if len(digits) >= 11:
            return digits[3:9]
        if len(digits) >= 6:
            return digits[-6:]
        return ""

    df = df.with_columns(
        pl.col("cpf_limpo").map_elements(_fragment_from_cpf, return_dtype=pl.Utf8).alias("cpf_fragment")
    )

    socio_ids = (
        df.select("id_pessoa")
        .filter(pl.col("id_pessoa") != "")
        .unique()
        .to_series()
        .to_list()
    )

    mapping = _load_socios_empresas_map(socio_ids)
    list_columns = [
        "socios_cnpjs",
        "socios_cnpj_basicos",
        "socios_empresas_razao",
        "socios_empresas_fantasia",
        "socios_empresas_ufs",
        "socios_empresas_municipios",
    ]
    if mapping is not None:
        df = df.join(mapping, left_on="id_pessoa", right_on="hash_documento_socio", how="left")
        if "hash_documento_socio" in df.columns:
            df = df.drop("hash_documento_socio")
    for col in list_columns:
        if col not in df.columns:
            df = df.with_columns(pl.lit([], dtype=pl.List(pl.Utf8)).alias(col))
        else:
            df = df.with_columns(
                pl.when(pl.col(col).is_null())
                .then(pl.lit([], dtype=pl.List(pl.Utf8)))
                .otherwise(pl.col(col))
                .alias(col)
            )

    df = df.with_columns(
        (pl.col("socios_cnpjs").list.len().fill_null(0) > 0).alias("eh_socio_por_documento")
    )

    socios_nome = _load_socios_by_nome()
    if socios_nome is not None and {"nome", "cnpj_basicos"}.issubset(socios_nome.columns):
        socios_nome = socios_nome.with_columns(pl.col("nome").cast(pl.Utf8))
        df = df.join(
            socios_nome.select([
                pl.col("nome").alias("lookup_nome"),
                pl.col("cnpj_basicos").alias("socios_nome_cnpj_basicos"),
            ]),
            left_on="nome_norm",
            right_on="lookup_nome",
            how="left",
        )
        if "lookup_nome" in df.columns:
            df = df.drop("lookup_nome")
        df = df.with_columns(
            pl.when(pl.col("socios_nome_cnpj_basicos").is_null())
            .then(pl.lit([], dtype=pl.List(pl.Utf8)))
            .otherwise(pl.col("socios_nome_cnpj_basicos"))
            .alias("socios_nome_cnpj_basicos")
        )

        candidatos_nome = socios_nome.select("nome").to_series().to_list()

        def approx_flag(row: dict) -> bool:
            nome = row.get("nome_norm") or ""
            fragment = row.get("cpf_fragment") or ""
            if fragment or not nome:
                return False
            for other in candidatos_nome[:5000]:
                if fuzz.token_set_ratio(nome, other) >= 92:
                    return True
            return False

        df = df.with_columns(
            pl.struct(["nome_norm", "cpf_fragment"]).map_elements(approx_flag, return_dtype=pl.Boolean).alias("eh_socio_por_nome")
        )
    else:
        df = df.with_columns([
            pl.lit([], dtype=pl.List(pl.Utf8)).alias("socios_nome_cnpj_basicos"),
            pl.lit(False).alias("eh_socio_por_nome"),
        ])

    df = df.with_columns(
        pl.when(pl.col("socios_cnpj_basicos").list.len().fill_null(0) == 0)
        .then(pl.col("socios_nome_cnpj_basicos"))
        .otherwise(pl.col("socios_cnpj_basicos"))
        .alias("socios_cnpj_basicos")
    )

    df = df.with_columns(
        (pl.col("eh_socio_por_documento") | pl.col("eh_socio_por_nome")).alias("socio")
    )

    df = df.drop([c for c in ["eh_socio_por_documento", "eh_socio_por_nome"] if c in df.columns])
    return df