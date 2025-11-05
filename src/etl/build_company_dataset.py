# -*- coding: utf-8 -*-
"""Build a dataset with company information for partners found in the alumni base."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import polars as pl
from loguru import logger

from ..utils.settings import CNPJ_BASE_DIR, PARQUET_OUT_DIR


EMPRESAS_COLUMNS: Sequence[str] = (
    "cnpj_basico",
    "razao_social",
    "natureza_juridica",
    "qualificacao_responsavel",
    "capital_social",
    "porte_empresa",
    "ente_federativo",
)

ESTABELECIMENTOS_COLUMNS: Sequence[str] = (
    "cnpj_basico",
    "cnpj_ordem",
    "cnpj_dv",
    "identificador_matriz_filial",
    "nome_fantasia",
    "situacao_cadastral",
    "data_situacao_cadastral",
    "motivo_situacao_cadastral",
    "nome_cidade_exterior",
    "pais",
    "data_inicio_atividade",
    "cnae_fiscal_principal",
    "cnae_fiscal_secundaria",
    "tipo_logradouro",
    "logradouro",
    "numero",
    "complemento",
    "bairro",
    "cep",
    "uf",
    "municipio",
    "ddd1",
    "telefone1",
    "ddd2",
    "telefone2",
    "ddd_fax",
    "fax",
    "email",
    "situacao_especial",
    "data_situacao_especial",
)

SIMPLES_COLUMNS: Sequence[str] = (
    "cnpj_basico",
    "optante_simples",
    "data_opcao_simples",
    "data_exclusao_simples",
    "optante_mei",
    "data_opcao_mei",
    "data_exclusao_mei",
)

MATRIZ_COLUMNS_TO_EXPORT: Sequence[str] = (
    "situacao_cadastral",
    "data_inicio_atividade",
    "uf",
    "cnae_fiscal_principal",
    "cnae_fiscal_secundaria",
)


@dataclass(slots=True)
class ExtractPaths:
    empresas: list[Path]
    estabelecimentos: list[Path]
    simples: list[Path]


def _ensure_targets(cnpjs: Iterable[str]) -> pl.DataFrame:
    targets = sorted({cnpj for cnpj in cnpjs if cnpj})
    if not targets:
        raise ValueError("Nenhum CNPJ encontrado para processar.")
    return pl.DataFrame({"cnpj_basico": targets}, schema={"cnpj_basico": pl.Utf8})


def _find_latest_extract_dir() -> Path:
    candidates = sorted(
        (p / "extracted_all" for p in CNPJ_BASE_DIR.iterdir() if p.is_dir()),
        key=lambda path: path.parent.name,
        reverse=True,
    )
    for directory in candidates:
        if directory.exists():
            return directory
    raise FileNotFoundError(
        f"Não foi possível localizar diretórios 'extracted_all' em {CNPJ_BASE_DIR}"
    )


def _collect_extract_paths(base_dir: Path) -> ExtractPaths:
    empresas = sorted(base_dir.glob("Empresas*.csv"))
    estabelecimentos = sorted(base_dir.glob("Estabelecimentos*.csv"))
    simples = sorted(base_dir.glob("Simples*.csv"))
    if not empresas:
        logger.warning("Nenhum arquivo Empresas*.csv encontrado em %s", base_dir)
    if not estabelecimentos:
        logger.warning("Nenhum arquivo Estabelecimentos*.csv encontrado em %s", base_dir)
    if not simples:
        logger.warning("Nenhum arquivo Simples*.csv encontrado em %s", base_dir)
    return ExtractPaths(empresas=empresas, estabelecimentos=estabelecimentos, simples=simples)


def _read_filtered_csv(
    paths: Sequence[Path],
    columns: Sequence[str],
    targets_lazy: pl.LazyFrame,
) -> pl.DataFrame:
    if not paths:
        return pl.DataFrame({col: [] for col in columns}, schema={col: pl.Utf8 for col in columns})
    frames: list[pl.LazyFrame] = []
    for path in paths:
        scan = (
            pl.scan_csv(
                path,
                separator=";",
                has_header=False,
                new_columns=list(columns),
                schema_overrides={col: pl.Utf8 for col in columns},
                ignore_errors=True,
                encoding="utf8-lossy",
            )
            .join(targets_lazy, on="cnpj_basico", how="inner")
        )
        frames.append(scan)
    if not frames:
        return pl.DataFrame()
    # Use streaming execution to avoid keeping every arquivo residente em memória.
    return pl.concat(frames).collect(streaming=True)


def _load_empresas(paths: Sequence[Path], targets_lazy: pl.LazyFrame) -> pl.DataFrame:
    empresas = _read_filtered_csv(paths, EMPRESAS_COLUMNS, targets_lazy)
    if empresas.is_empty():
        return empresas
    return empresas.with_columns(pl.col("cnpj_basico").cast(pl.Utf8)).unique("cnpj_basico")


def _load_simples(paths: Sequence[Path], targets_lazy: pl.LazyFrame) -> pl.DataFrame:
    simples = _read_filtered_csv(paths, SIMPLES_COLUMNS, targets_lazy)
    if simples.is_empty():
        return simples
    return simples.with_columns(pl.col("cnpj_basico").cast(pl.Utf8)).unique("cnpj_basico")


def _load_estabelecimentos(
    paths: Sequence[Path],
    targets_lazy: pl.LazyFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    estabelecimentos = _read_filtered_csv(paths, ESTABELECIMENTOS_COLUMNS, targets_lazy)
    if estabelecimentos.is_empty():
        schema = {col: pl.Utf8 for col in ESTABELECIMENTOS_COLUMNS}
        vazio = pl.DataFrame({col: [] for col in ESTABELECIMENTOS_COLUMNS}, schema=schema)
        return vazio, vazio

    estabelecimentos = estabelecimentos.with_columns([
        pl.col("cnpj_basico").cast(pl.Utf8),
        (pl.col("cnpj_basico") + pl.col("cnpj_ordem") + pl.col("cnpj_dv")).alias("cnpj"),
    ])

    struct_cols = list(ESTABELECIMENTOS_COLUMNS) + ["cnpj"]
    estabelecimentos = estabelecimentos.with_columns(
        pl.struct(struct_cols).alias("_estab_struct")
    )

    aggregated = (
        estabelecimentos.group_by("cnpj_basico")
        .agg(pl.col("_estab_struct").alias("estabelecimentos"))
    )

    matriz = (
        estabelecimentos.filter(pl.col("identificador_matriz_filial") == "1")
        .group_by("cnpj_basico")
        .agg(pl.col("_estab_struct").first().alias("estabelecimento_matriz"))
    )

    return aggregated, matriz


def _collect_cnpjs_from_parquet(egressos_path: Path) -> set[str]:
    if not egressos_path.exists():
        raise FileNotFoundError(f"Arquivo de egressos não encontrado: {egressos_path}")
    scan = pl.scan_parquet(egressos_path)
    if "cnpj_basico" not in scan.schema:
        raise ValueError(
            "Coluna 'cnpj_basico' ausente no parquet de egressos. "
            "Execute novamente o pipeline ETL para gerar as informações de sócios (socio_cpf, socio_nome, cnpj_basico)."
        )
    flattened = (
        scan.select(pl.col("cnpj_basico").list.explode().cast(pl.Utf8))
        .drop_nulls()
        .filter(pl.col("cnpj_basico") != "")
        .unique()
        .collect()
    )
    return set(flattened["cnpj_basico"])


def build_company_dataset(
    egressos_parquet: Path | None = None,
    output_path: Path | None = None,
) -> Path:
    """Collect company information for partner CNPJs and persist as parquet."""
    if egressos_parquet is None:
        egressos_parquet = PARQUET_OUT_DIR / "egressos" / "egressos.parquet"
    if output_path is None:
        output_path = PARQUET_OUT_DIR / "empresas" / "socios_empresas.parquet"

    logger.info("Carregando CNPJs alvo a partir de %s", egressos_parquet)
    target_cnpjs = _collect_cnpjs_from_parquet(egressos_parquet)
    targets_df = _ensure_targets(target_cnpjs)
    targets_lazy = targets_df.lazy()

    extract_dir = _find_latest_extract_dir()
    logger.info("Usando arquivos extraídos em %s", extract_dir)
    paths = _collect_extract_paths(extract_dir)

    logger.info("Lendo informações de Empresas.")
    empresas = _load_empresas(paths.empresas, targets_lazy)

    logger.info("Lendo informações de Estabelecimentos.")
    estabelecimentos_all, estabelecimentos_matriz = _load_estabelecimentos(
        paths.estabelecimentos, targets_lazy
    )

    logger.info("Lendo informações de Simples.")
    simples = _load_simples(paths.simples, targets_lazy)

    result = targets_df.lazy()
    if not empresas.is_empty():
        result = result.join(empresas.lazy(), on="cnpj_basico", how="left")
    if not simples.is_empty():
        result = result.join(simples.lazy(), on="cnpj_basico", how="left", suffix="_simples")
    if not estabelecimentos_all.is_empty():
        result = result.join(estabelecimentos_all.lazy(), on="cnpj_basico", how="left")
    if not estabelecimentos_matriz.is_empty():
        result = result.join(estabelecimentos_matriz.lazy(), on="cnpj_basico", how="left")

    if "estabelecimento_matriz" in result.schema:
        result = result.with_columns(
            [
                pl.col("estabelecimento_matriz")
                .struct.field(column)
                .alias(column)
                for column in MATRIZ_COLUMNS_TO_EXPORT
            ]
        )
    else:
        result = result.with_columns(
            [pl.lit(None).cast(pl.Utf8).alias(column) for column in MATRIZ_COLUMNS_TO_EXPORT]
        )

    if "data_inicio_atividade" in result.schema:
        inicio_atividade_expr = pl.coalesce(
            [
                pl.col("data_inicio_atividade").str.strptime(pl.Date, format="%Y%m%d", strict=False),
                pl.col("data_inicio_atividade").str.strptime(pl.Date, format="%Y-%m-%d", strict=False),
            ]
        )
        result = result.with_columns(
            inicio_atividade_expr
            .dt.year()
            .floordiv(10)
            .mul(10)
            .cast(pl.Int32)
            .alias("decada_inicio_atividade")
        )
    else:
        result = result.with_columns(pl.lit(None).cast(pl.Int32).alias("decada_inicio_atividade"))

    final_df = result.collect()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    final_df.write_parquet(output_path)
    logger.success("Parquet gerado com %d empresas em %s", final_df.height, output_path)
    return output_path


def main() -> None:
    build_company_dataset()


if __name__ == "__main__":
    main()
