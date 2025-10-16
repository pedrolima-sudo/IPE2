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


def _calc_idade(nasc: date | None) -> int | None:
    if not nasc:
        return None
    try:
        today = date.today()
        return relativedelta(today, nasc).years
    except Exception:
        return None


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
    Requer colunas: nome, cpf, data_nascimento (se existirem no input).
    """
    logger.info("Transformando dados básicos de egressos…")

    # aplica limpeza de strings
    df = df.with_columns([
        pl.col("nome").map_elements(normalize_name).alias("nome_norm"),
        pl.col("nome_pai").map_elements(normalize_name).alias("nome_pai_norm"),
        pl.col("nome_mae").map_elements(normalize_name).alias("nome_mae_norm"),
    ])

    # limpa CPF e cria ID hash
    df = df.with_columns([
        pl.col("cpf").map_elements(_clean_cpf).alias("cpf_limpo"),
    ])

    df = df.with_columns([
        pl.col("cpf_limpo").map_elements(lambda x: hash_identifier(x, CPF_SALT)).alias("id_pessoa"),
    ])

    # idade & faixa
    df = df.with_columns([
        pl.col("data_nascimento").dt.date().alias("dn_date"),
    ])

    df = df.with_columns([
        pl.col("dn_date").map_elements(_calc_idade).alias("idade"),
        pl.col("idade").map_elements(_faixa_etaria).alias("faixa_etaria"),
    ])

    # último curso (heurística simples: usa 'curso' se existir)
    if "curso" in df.columns:
        df = df.with_columns(pl.col("curso").fill_null("").alias("ultimo_curso"))
    else:
        df = df.with_columns(pl.lit("").alias("ultimo_curso"))

    return df