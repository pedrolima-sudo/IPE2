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
    "situacaoCurso": str,
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

    # tenta converter datas se existirem
    for col in ("data_nascimento", "data_ingresso", "data_formacao"):
        if col in df_pd.columns:
            df_pd[col] = pd.to_datetime(df_pd[col], errors="coerce")

    # converte para polars
    df = pl.from_pandas(df_pd)

    # garante presença de colunas básicas, criando vazias se faltarem
    for col in EXPECTED_COLS.keys():
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    return df