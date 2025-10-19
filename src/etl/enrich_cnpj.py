"""
MÓDULO PARA ENRIQUECIMENTO DE DADOS DE EGRESSOS COM INFORMAÇÕES DE CNPJ
========================================================================   

Este módulo enriquece o DataFrame de egressos com indicadores de sócio/fundador
a partir de bases externas de CNPJ.

Autor:Pedro Henrique Lima Silva
Data de criação: 15/10/2025
Última modificação: 16/10/2025
"""

from __future__ import annotations
from pathlib import Path
import polars as pl
from loguru import logger
from rapidfuzz import fuzz, process
from ..utils.settings import CNPJ_BASE_DIR, SILVER_DIR
import subprocess, sys

# Estratégia mínima: 
# 1) Se houver um arquivo de sócios já preparado (ex.: silver/socios.parquet) com coluna 'cpf_fragment'
#    combinamos os 6 últimos dígitos do CPF + nome aproximado para marcar sócios.
# 2) Se não houver CPF, fazemos fallback por nome aproximado (menos confiável) 
#    usando uma lista local 'socios_nomes.parquet/csv' (opcional).


def _load_socios_by_cpf() -> pl.DataFrame | None:
    """Busca uma base de sócios contendo coluna CPF nas localizações conhecidas e retorna como DataFrame."""
    for path in [
        SILVER_DIR / "socios.parquet",
        CNPJ_BASE_DIR / "socios.parquet",
        CNPJ_BASE_DIR.parent / "silver" / "socios.parquet",
    ]:
        if Path(path).exists():
            logger.info(f"Carregando sócios por CPF: {path}")
            return pl.read_parquet(path)
    logger.warning("Base de sócios por CPF não encontrada. Pulei enriquecimento por CPF.")
    return None

def _load_socios_by_nome() -> pl.DataFrame | None:
    """Retorna a base de sócios indexada por nome (parquet ou CSV), se algum arquivo esperado estiver presente."""
    for path in [
        SILVER_DIR / "socios_nomes.parquet",
        SILVER_DIR / "socios_nomes.csv",
        CNPJ_BASE_DIR / "socios_nomes.parquet",
        CNPJ_BASE_DIR / "socios_nomes.csv",
        CNPJ_BASE_DIR.parent / "silver" / "socios_nomes.parquet",
        CNPJ_BASE_DIR.parent / "silver" / "socios_nomes.csv",
    ]:
        if Path(path).exists():
            logger.info(f"Carregando base de sócios por nome: {path}")
            return pl.read_parquet(path) if str(path).endswith(".parquet") else pl.read_csv(path)
    logger.warning("Base de sócios por nome não encontrada. Pulei fallback por nome.")
    return None



def mark_founders(df: pl.DataFrame) -> pl.DataFrame:
    """Enriquece o DataFrame de egressos com indicadores booleanos de sócio/fundador via CPF e aproximação por nome."""
    logger.info("Marcando fundadores/sócios a partir das bases disponíveis…")
    _ensure_socios_data(max_files=-1)  # pode ajustar para -1 quando quiser baixar tudo

    def _fragment_from_cpf(cpf: str | None) -> str:
        if not cpf:
            return ""
        digits = str(cpf)
        if len(digits) >= 11:
            return digits[3:9]
        if len(digits) >= 6:
            return digits[-6:]
        return ""

    df = df.with_columns(
        pl.col("cpf_limpo").map_elements(
            _fragment_from_cpf, return_dtype=pl.Utf8
        ).alias("cpf_fragment")
    )

    # 1) Join por fragmento de CPF + nome aproximado
    socios_cpf = _load_socios_by_cpf()
    if socios_cpf is not None and {"cpf_fragment", "nome"}.issubset(socios_cpf.columns):
        socios_cpf = socios_cpf.with_columns([
            pl.col("cpf_fragment").cast(pl.Utf8),
            pl.col("nome").cast(pl.Utf8),
        ])
        socios_idx = (
            socios_cpf
            .filter(pl.col("cpf_fragment") != "")
            .group_by("cpf_fragment")
            .agg(pl.col("nome").unique().alias("nomes_socios"))
        )

        df = df.join(socios_idx, on="cpf_fragment", how="left")

        def _match_fragment_name(row: dict) -> bool:
            nome = row.get("nome_norm") or ""
            candidatos = row.get("nomes_socios")
            if not nome or not candidatos:
                return False
            # Limpa candidatos vazios
            nomes_validos = [c for c in candidatos if c]
            if not nomes_validos:
                return False
            match = process.extractOne(
                nome,
                nomes_validos,
                scorer=fuzz.token_set_ratio,
                score_cutoff=90,
            )
            return match is not None

        df = df.with_columns(
            pl.struct(["nome_norm", "nomes_socios"])
              .map_elements(_match_fragment_name, return_dtype=pl.Boolean)
              .alias("eh_socio_por_cpf")
        )
        df = df.drop("nomes_socios")
    else:
        df = df.with_columns(pl.lit(False).alias("eh_socio_por_cpf"))

    # 2) Fallback por nome (menos confiável)
    socios_nome = _load_socios_by_nome()
    if socios_nome is not None and "nome" in socios_nome.columns:
        # normaliza nomes na base externa
        socios_nome = socios_nome.with_columns(pl.col("nome").str.to_uppercase())
        candidatos_nome = socios_nome.select("nome").to_series().to_list()

        # cria uma marcação por similaridade > limiar (ex.: 92)
        def approx_flag(row: dict) -> bool:
            nome = row.get("nome_norm") or ""
            fragment = row.get("cpf_fragment") or ""
            if fragment or not nome:
                return False
            # Atenção: este loop é O(n). Para bases grandes, use estratégias mais eficientes (token sets, índices, etc.)
            for other in candidatos_nome[:5000]:  # limita a 5000 para não explodir
                if fuzz.token_set_ratio(nome, other) >= 92:
                    return True
            return False

        df = df.with_columns(
            pl.struct(["nome_norm", "cpf_fragment"])
              .map_elements(approx_flag, return_dtype=pl.Boolean)
              .alias("eh_socio_por_nome")
        )
    else:
        df = df.with_columns(pl.lit(False).alias("eh_socio_por_nome"))

    return df.with_columns((pl.col("eh_socio_por_cpf") | pl.col("eh_socio_por_nome")).alias("eh_socio_fundador"))

def _ensure_socios_data(max_files: int = 3) -> None:
    """Garante a presença das bases de sócios em data/silver, disparando o download/preparo quando necessário.

    max_files controla quantos arquivos compactados "Socios" baixar (3 = teste rápido; -1 = todos).
    """
    alvo_cpf = SILVER_DIR / "socios.parquet"
    alvo_nome = SILVER_DIR / "socios_nomes.parquet"
    if alvo_cpf.exists() and alvo_nome.exists():
        return
    try:
        from loguru import logger
        logger.warning("Base de sócios ausente. Iniciando download/preparo mínimo (--max-files=%d)...", max_files)
        subprocess.run(
            [sys.executable, "-m", "src.cnpj.prepare_socios", "--max-files", str(max_files)],
            check=True
        )
        logger.success("Preparo de sócios concluído.")
    except Exception as e:
        logger.exception(e)
        # segue sem enriquecer, mas sem quebrar o pipeline
