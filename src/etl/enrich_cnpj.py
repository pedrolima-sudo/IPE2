from __future__ import annotations
from pathlib import Path
import polars as pl
from loguru import logger
from rapidfuzz import fuzz
from ..utils.settings import CNPJ_BASE_DIR


# Estratégia mínima: 
# 1) Se houver um arquivo de sócios já preparado (ex.: silver/socios.parquet) com coluna 'cpf_socio'
#    marcamos fundador/sócio por CPF.
# 2) Se não houver CPF, fazemos fallback por nome aproximado (menos confiável) 
#    usando uma lista local 'socios_nomes.parquet/csv' (opcional).


def _load_socios_by_cpf() -> pl.DataFrame | None:
    # Procura por um parquet pronto (você pode preparar isso depois em outro script)
    for path in [CNPJ_BASE_DIR.parent / "silver" / "socios.parquet",
                 CNPJ_BASE_DIR / "socios.parquet"]:
        if Path(path).exists():
            logger.info(f"Carregando sócios por CPF: {path}")
            return pl.read_parquet(path)
    logger.warning("Base de sócios por CPF não encontrada. Pulei enriquecimento por CPF.")
    return None


def _load_socios_by_nome() -> pl.DataFrame | None:
    for path in [CNPJ_BASE_DIR.parent / "silver" / "socios_nomes.parquet",
                 CNPJ_BASE_DIR / "socios_nomes.parquet",
                 CNPJ_BASE_DIR / "socios_nomes.csv"]:
        if Path(path).exists():
            logger.info(f"Carregando base de sócios por nome: {path}")
            if str(path).endswith(".csv"):
                return pl.read_csv(path)
            return pl.read_parquet(path)
    logger.warning("Base de sócios por nome não encontrada. Pulei fallback por nome.")
    return None


def mark_founders(df: pl.DataFrame) -> pl.DataFrame:
    logger.info("Marcando fundadores/sócios a partir das bases disponíveis…")

    # 1) Join por CPF (mais robusto)
    socios_cpf = _load_socios_by_cpf()
    if socios_cpf is not None and "cpf" in socios_cpf.columns:
        socios_cpf = socios_cpf.with_columns(
            pl.col("cpf").cast(pl.Utf8)
        )
        df = df.join(socios_cpf.select(["cpf"]).unique(), left_on="cpf_limpo", right_on="cpf", how="left")
        df = df.with_columns(pl.col("cpf").is_not_null().alias("eh_socio_por_cpf"))
        df = df.drop("cpf")
    else:
        df = df.with_columns(pl.lit(False).alias("eh_socio_por_cpf"))

    # 2) Fallback por nome (menos confiável)
    socios_nome = _load_socios_by_nome()
    if socios_nome is not None and "nome" in socios_nome.columns:
        # normaliza nomes na base externa
        socios_nome = socios_nome.with_columns(pl.col("nome").str.to_uppercase())
        # cria uma marcação por similaridade > limiar (ex.: 92)
        def approx_flag(nome):
            if not nome:
                return False
            # Atenção: este loop é O(n). Para bases grandes, use estrategias mais eficientes (token sets, índices, etc.)
            # Aqui é propositalmente simples para funcionar em ambientes pequenos.
            top = socios_nome.select("nome").to_series().to_list()
            for other in top[:5000]:  # limita a 5000 para não explodir
                if fuzz.token_set_ratio(nome, other) >= 92:
                    return True
            return False
        df = df.with_columns(pl.col("nome_norm").map_elements(approx_flag).alias("eh_socio_por_nome"))
    else:
        df = df.with_columns(pl.lit(False).alias("eh_socio_por_nome"))

    return df.with_columns((pl.col("eh_socio_por_cpf") | pl.col("eh_socio_por_nome")).alias("eh_socio_fundador"))
