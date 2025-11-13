"""
MÓDULO PARA TRANSFORMAÇÃO DE DADOS DE EGRESSOS
=================================================

Este módulo aplica transformações básicas nos dados de egressos, como limpeza de nomes, CPF, 
cálculo de idade e faixa etária, e geração de IDs hash.

Autor: Pedro Henrique Lima Silva
Data de criação: 15/10/2025
Última modificação: 16/10/2025
"""

from __future__ import annotations
from datetime import date
from dateutil.relativedelta import relativedelta
import polars as pl
from validate_docbr import CPF
from loguru import logger
from ..utils.clean import normalize_name
from ..utils.security import hash_identifier
from ..utils.settings import CPF_SALT


_cpf_validator = CPF()


def _clean_cpf(s: str) -> str:
    if s is None:
        return ""
    s = "".join(ch for ch in str(s) if ch.isdigit())
    if len(s) == 11 and _cpf_validator.validate(s):
        return s
    return ""


def _calc_idade(nasc: date | None, ref: date | None = None) -> int | None:
    if not nasc:
        return None
    try:
        ref_date = ref or date.today()
        if not ref_date:
            return None
        return relativedelta(ref_date, nasc).years
    except Exception:
        return None


def _calc_idade_ingresso(row: dict) -> int | None:
    return _calc_idade(row.get("dn_date"), row.get("di_date"))


def _calc_idade_conclusao(row: dict) -> int | None:
    return _calc_idade(row.get("dn_date"), row.get("df_date"))


def _faixa_etaria(idade: int | None) -> str:
    if idade is None:
        return "IGNORADO"
    bins = [0, 18, 25, 35, 45, 55, 65, 200]
    labels = ["<18", "18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
    for i in range(len(labels)):
        if bins[i] <= idade < bins[i+1]:
            return labels[i]
    return "IGNORADO"


def basic_transform(df: pl.DataFrame) -> pl.DataFrame:
    """Limpeza e features básicas: nomes, CPF, idade/faixa, IDs hash.
    Requer colunas: nome, cpf, data_nascimento.
    """
    logger.info("Transformando dados básicos de egressos…")

    # nomes normalizados
    df = df.with_columns([
        pl.col("nome").map_elements(normalize_name, return_dtype=pl.Utf8).alias("nome_norm"),
        pl.col("nome_pai").map_elements(normalize_name, return_dtype=pl.Utf8).alias("nome_pai_norm"),
        pl.col("nome_mae").map_elements(normalize_name, return_dtype=pl.Utf8).alias("nome_mae_norm"),
    ])

    # CPF limpo e id hash
    df = df.with_columns([
        pl.col("cpf").map_elements(_clean_cpf, return_dtype=pl.Utf8).alias("cpf_limpo"),
    ])
    df = df.with_columns([
        pl.col("cpf_limpo").map_elements(lambda x: hash_identifier(x, CPF_SALT), return_dtype=pl.Utf8).alias("id_pessoa"),
    ])

    # data de nascimento (cast seguro) → idade e faixa
    date_cols = [
        pl.col("data_nascimento").cast(pl.Date, strict=False).alias("dn_date"),
    ]
    if "data_ingresso" in df.columns:
        date_cols.append(pl.col("data_ingresso").cast(pl.Date, strict=False).alias("di_date"))
    else:
        date_cols.append(pl.lit(None).cast(pl.Date).alias("di_date"))
    if "data_formacao" in df.columns:
        date_cols.append(pl.col("data_formacao").cast(pl.Date, strict=False).alias("df_date"))
    else:
        date_cols.append(pl.lit(None).cast(pl.Date).alias("df_date"))

    df = df.with_columns(date_cols)
    df = df.with_columns([
        pl.col("dn_date").map_elements(_calc_idade, return_dtype=pl.Int64).alias("idade"),
    ])
    df = df.with_columns([
        pl.struct(["dn_date", "di_date"]).map_elements(
            _calc_idade_ingresso, return_dtype=pl.Int64
        ).alias("idade_ingresso"),
        pl.struct(["dn_date", "df_date"]).map_elements(
            _calc_idade_conclusao, return_dtype=pl.Int64
        ).alias("idade_conclusao"),
    ])
    df = df.with_columns([
        pl.col("idade").map_elements(_faixa_etaria, return_dtype=pl.Utf8).alias("faixa_etaria"),
    ])

    # último curso (heurística simples: usa 'curso' se existir)
    if "curso" in df.columns:
        df = df.with_columns(pl.col("curso").fill_null("").alias("ultimo_curso"))
    else:
        df = df.with_columns(pl.lit("").alias("ultimo_curso"))

    return df
