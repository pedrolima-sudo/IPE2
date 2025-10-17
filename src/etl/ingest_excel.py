"""
MÓDULO PARA INGESTÃO DE DADOS DE EGRESSOS A PARTIR DE ARQUIVOS EXCEL
=======================================================================

Este módulo pega o Excel bruto de egressos e devolve um DataFrame Polars limpo e padronizado para as próximas etapas.

Autor: Pedro Henrique Lima Silva
Data de criação: 15/10/2025
Última modificação: 17/10/2025
"""

from __future__ import annotations
from pathlib import Path
import pandas as pd
import polars as pl
from loguru import logger

EXPECTED_COLS = {
    "matricula": str,
    "nome": str,
    "cpf": str,
    "nome_pai": str,
    "nome_mae": str,
    "data_nascimento": "datetime64[ns]",
    "data_ingresso": "datetime64[ns]",
    "data_formacao": "datetime64[ns]",
    # colunas opcionais
    "curso": str,
    "codigo_curso": str,
    "nivel": str,
    "situacao_curso": str,
}

# mapeia variáveis comuns (após normalização) para o nome canônico usado no pipeline
COLUMN_ALIASES = {
    "matriculadre": "matricula",
    "datanascimento": "data_nascimento",
    "dataingresso": "data_ingresso",
    "dataconclusao": "data_formacao",
    "pai": "nome_pai",
    "mae": "nome_mae",
    "codigo": "codigo_curso",
    "situacaocurso": "situacao_curso",
}


def read_egressos_excel(path: Path) -> pl.DataFrame:
    """Lê o Excel de egressos usando pandas + converte para Polars.
    Aceita variações mínimas de nome de coluna (case-insensitive / underscores).
    """
    logger.info(f"Lendo Excel de egressos: {path}")
    if not Path(path).exists():
        raise FileNotFoundError(f"Excel não encontrado: {path}")

    # lê com pandas para suportar .xlsx via openpyxl
    df_pd = pd.read_excel(path, dtype=str)
    # normaliza nomes de colunas
    df_pd.columns = (
        df_pd.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )
    df_pd.columns = [COLUMN_ALIASES.get(col, col) for col in df_pd.columns]

    # tenta converter datas se existirem
    for col in ("data_nascimento", "data_ingresso", "data_formacao"):
        if col in df_pd.columns:
            df_pd[col] = pd.to_datetime(df_pd[col], errors="coerce", dayfirst=True)

    # converte para polars
    df = pl.from_pandas(df_pd)

    # garante presença de colunas básicas, criando vazias se faltarem
    for col in EXPECTED_COLS.keys():
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    return df
