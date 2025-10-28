"""
MóDULO PARA PIPELINE ETL DE DADOS DE EGRESSOS
=================================================

Este módulo orquestra a execução do pipeline ETL completo para dados de egressos,
incluindo ingestão, transformação, enriquecimento e exportação.

Autor: Pedro Henrique Lima Silva
Data de criação: 15/10/2025
Última modificação: 16/10/2025
"""

from __future__ import annotations
import argparse
from pathlib import Path
from loguru import logger
import polars as pl

from .ingest_excel import read_egressos_excel
from .transform import basic_transform
from .enrich_cnpj import mark_founders
from .export_powerbi import export_parquet
from .build_company_dataset import build_company_dataset
from .founder_analysis import label_socios_fundadores
from ..utils.settings import EGRESSO_EXCEL_FILE


def run_pipeline(excel_path: str | None) -> Path:
    if not excel_path:
        if not EGRESSO_EXCEL_FILE:
            raise SystemExit("Informe o caminho do Excel via --excel OU defina EGRESSO_EXCEL_FILE no .env")
        excel_path = EGRESSO_EXCEL_FILE

    df = read_egressos_excel(Path(excel_path))
    df = basic_transform(df)
    df = mark_founders(df)

    decade_exprs = []
    if "data_ingresso" in df.columns:
        decade_exprs.append(
            (
                pl.col("data_ingresso")
                .cast(pl.Date, strict=False)
                .dt.year()
                .floordiv(10)
                .mul(10)
                .cast(pl.Int32)
                .alias("decada_inicio_curso")
            )
        )
    else:
        decade_exprs.append(pl.lit(None).cast(pl.Int32).alias("decada_inicio_curso"))

    if "data_formacao" in df.columns:
        decade_exprs.append(
            (
                pl.col("data_formacao")
                .cast(pl.Date, strict=False)
                .dt.year()
                .floordiv(10)
                .mul(10)
                .cast(pl.Int32)
                .alias("decada_conclusao_curso")
            )
        )
    else:
        decade_exprs.append(pl.lit(None).cast(pl.Int32).alias("decada_conclusao_curso"))

    df = df.with_columns(decade_exprs)

    # selecione apenas colunas úteis ao BI (ajuste conforme necessário)
    cols = [
        "id_pessoa", "matricula", "nome", "nome_norm",
        "idade", "faixa_etaria",
        "data_nascimento", "data_ingresso", "data_formacao",
        "ultimo_curso", "nivel", "codigo_curso",
        "socio_cpf", "socio_nome", "socio", "cnpj_basico", "data_associacao",
        "decada_inicio_curso", "decada_conclusao_curso",
    ]
    cols = [c for c in cols if c in df.columns]
    df_out = df.select(cols)

    out_dir = export_parquet(df_out)

    egressos_parquet = out_dir / "egressos.parquet"
    empresas_parquet = build_company_dataset(egressos_parquet=egressos_parquet)
    founders_parquet = label_socios_fundadores(
        egressos_parquet=egressos_parquet, empresas_parquet=empresas_parquet
    )

    logger.success(
        f"Pipeline concluído. Saída egressos: {out_dir} | Parquet empresas: {empresas_parquet} | Fundadores: {founders_parquet}"
    )
    return out_dir


def main():
    parser = argparse.ArgumentParser(description="Portal Egressos – Pipeline ETL")
    parser.add_argument("--excel", help="Caminho do Excel de egressos", default=None)
    args = parser.parse_args()
    run_pipeline(args.excel)


if __name__ == "__main__":
    main()
