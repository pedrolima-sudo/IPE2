# -*- coding: latin-1 -*-
"""
MÓDULO DE PREPARAÇÃO DOS DADOS DE SÓCIOS
========================================================

Prepara os dados de sócios extraídos dos arquivos .zip do CNPJ,
gerando tabelas deduplicadas por CPF e por nome.

Autor: Pedro Henrique Lima Silva (essa parte é toda mérito do gpt)
Data de criação: 15/10/2025
Última modificação: 18/10/2025
"""
from __future__ import annotations
import argparse
import hashlib
import json
import re
from collections.abc import Iterable
from pathlib import Path
import shutil  # para copyfileobj
import zipfile

import polars as pl
from loguru import logger
from unidecode import unidecode


try:
    from .download_cnpj import list_months, download_many
    from ..utils.settings import (
        CNPJ_BASE_DIR,
        SILVER_DIR,
        SOCIOS_DOWNLOAD_PREFIXES,
        CPF_SALT,
    )
    from ..utils.security import hash_identifier
except ImportError:  # Execu��o direta (python src/cnpj/prepare_socios.py)
    import sys
    from pathlib import Path as _Path

    sys.path.append(str(_Path(__file__).resolve().parents[2]))
    from src.cnpj.download_cnpj import list_months, download_many  # type: ignore
    from src.utils.settings import (
        CNPJ_BASE_DIR,
        SILVER_DIR,
        SOCIOS_DOWNLOAD_PREFIXES,
        CPF_SALT,
    )
    from src.utils.security import hash_identifier


def _digits_only(s: str | None) -> str:
    if not s:
        return ""
    return "".join(ch for ch in str(s) if ch.isdigit())


def _normalize_name(s: str | None) -> str:
    if not s:
        return ""
    return " ".join(unidecode(str(s)).upper().split())


def _cpf_fragment_from_digits(d: str | None) -> str:
    if not d:
        return ""
    digits = str(d)
    if len(digits) >= 11:
        return digits[3:9]
    if len(digits) >= 6:
        return digits[-6:]
    return ""


_MONTH_DIR_PATTERN = re.compile(r"^\d{4}-\d{2}$")
_PREFIX_SPLIT_PATTERN = re.compile(r"[;,]")


def _normalize_prefix_tokens(prefixes: str | Iterable[str] | None) -> tuple[str, ...] | None:
    """
    Replica a normaliza��o usada no downloader para reutiliza��o offline.
    Converte para lowercase e trata 'all'/'todos' como None.
    """
    if prefixes is None:
        return None

    raw_parts: list[str] = []
    if isinstance(prefixes, str):
        raw_parts = _PREFIX_SPLIT_PATTERN.split(prefixes)
    else:
        for item in prefixes:
            if not item:
                continue
            if isinstance(item, str):
                raw_parts.extend(_PREFIX_SPLIT_PATTERN.split(item))
            else:
                raw_parts.append(str(item))

    cleaned: list[str] = []
    for part in raw_parts:
        token = part.strip()
        if not token:
            continue
        lowered = token.lower()
        if lowered in {"all", "todos", "*"}:
            return None
        cleaned.append(lowered)

    return tuple(cleaned) or None


def _list_local_months(base_dir: Path) -> list[str]:
    """Lista diret��rios AAAA-MM jǭ baixados localmente."""
    if not base_dir.exists():
        return []
    try:
        entries = base_dir.iterdir()
    except OSError as err:
        logger.debug("Falha ao listar diret��rio local %s: %s", base_dir, err)
        return []
    months = sorted(p.name for p in entries if p.is_dir() and _MONTH_DIR_PATTERN.match(p.name))
    return months


def _extract_all(zips: list[Path], out_dir: Path, *, preserve_dirs: bool = False) -> list[Path]:
    """
    Extrai todos os arquivos de cada zip. Se já houver CSVs extraídos previamente,
    reaproveita-os e evita trabalho repetido. Quando extrai algo novo, renomeia para
    <nome_do_zip>.csv, adicionando sufixos __2, __3... quando necessário.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    extracted: list[Path] = []

    def _existing_csvs() -> list[Path]:
        return list(out_dir.rglob("*.csv")) if preserve_dirs else list(out_dir.glob("*.csv"))

    for zip_path in zips:
        logger.info(f"Extraindo: {zip_path}")
        base = zip_path.stem
        emitted = 0

        existing = [p for p in _existing_csvs() if p.name.lower().startswith(base.lower())]
        if existing:
            logger.info("Pulando extração (já encontrado): %s", ", ".join(str(p) for p in existing))
            extracted.extend(existing)
            continue

        with zipfile.ZipFile(zip_path, "r") as zf:
            for info in zf.infolist():
                if info.is_dir():
                    continue
                emitted += 1
                target_name = base if emitted == 1 else f"{base}__{emitted}"
                if preserve_dirs:
                    raw_target = out_dir / info.filename
                    raw_target.parent.mkdir(parents=True, exist_ok=True)
                    target = raw_target.with_name(f"{target_name}.csv")
                else:
                    target = out_dir / f"{target_name}.csv"

                out_root = out_dir.resolve()
                tgt_parent = target.resolve().parent
                if tgt_parent != out_root and out_root not in tgt_parent.parents:
                    raise ValueError("caminho inseguro dentro do zip")

                if target.exists():
                    target.unlink()

                with zf.open(info) as src, target.open("wb") as dst:
                    shutil.copyfileobj(src, dst)

                extracted.append(target)

    if not extracted:
        fallback = _existing_csvs()
        if fallback:
            logger.info("Reutilizando %d CSVs já presentes em %s", len(fallback), out_dir)
            extracted.extend(fallback)

    logger.info(f"Arquivos extraídos: {len(extracted)}")
    return extracted



def build_socios_tables(csvs: list[Path]) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Retorna (socios_por_cpf, socios_por_nome).
    - socios_por_cpf: colunas ['cpf_fragment','nome'] com 6 dígitos finais e nome normalizado.
    - socios_por_nome: colunas ['nome'] (deduplicado, útil para fallback por nome)
    """
    if not csvs:
        raise SystemExit("Nenhum CSV de sócios encontrado após extração.")

    dfs = []
    for p in csvs:
        try:
            dfp = pl.read_csv(
                p,
                separator=";",
                has_header=False,
                ignore_errors=True,
                encoding="latin1",
                infer_schema_length=0,
                quote_char='"'
            )
            if dfp.shape[1] >= 11:
                rename_map = {
                    dfp.columns[0]: "cnpj_basico",
                    dfp.columns[1]: "identificador_socio",
                    dfp.columns[2]: "nome_socio",
                    dfp.columns[3]: "cpf_cnpj_socio",
                    dfp.columns[4]: "qualificacao_socio",
                    dfp.columns[5]: "data_entrada_sociedade",
                    dfp.columns[6]: "pais",
                    dfp.columns[7]: "representante_legal",
                    dfp.columns[8]: "nome_representante",
                    dfp.columns[9]: "qualificacao_representante",
                    dfp.columns[10]: "faixa_etaria",
                }
                dfp = dfp.rename(rename_map)
            dfs.append(dfp)
        except Exception as e:
            logger.warning(f"Falha ao ler {p}: {e}")

    if not dfs:
        raise SystemExit("Falha ao ler todos os CSVs de sócios.")

    df = pl.concat(dfs, how="vertical_relaxed")
    df = df.rename({c: c.lower() for c in df.columns})

    # Mapeia colunas mais comuns conforme layout RFB
    col_nome = next((c for c in df.columns if c in {"nome_socio", "nome_socio_razao_social", "nome_do_socio", "nome"}), None)
    col_id   = next((c for c in df.columns if c in {"identificador_de_socio", "identificador_socio", "identificador do socio"}), None)
    col_doc  = next((c for c in df.columns if c in {"cpf_cnpj_socio", "cnpj_cpf_do_socio", "documento_socio"}), None)

    if not (col_nome and col_id and col_doc):
        logger.error(f"Colunas esperadas não encontradas. Existem: {df.columns}")
        raise SystemExit("Layout de sócios inesperado — ajuste os aliases no script.")

    col_cnpj = next((c for c in df.columns if c in {"cnpj_basico", "cnpj"}), None)

    selected_exprs = [
        pl.col(col_nome).alias("nome"),
        pl.col(col_id).alias("identificador"),
        pl.col(col_doc).alias("doc"),
    ]
    if col_cnpj:
        selected_exprs.insert(0, pl.col(col_cnpj).alias("cnpj_basico"))
    else:
        selected_exprs.insert(0, pl.lit(None).alias("cnpj_basico"))

    df = df.select(selected_exprs)

    df = df.with_columns([
        pl.col("identificador").cast(pl.Utf8),
        pl.col("doc").map_elements(_digits_only).alias("doc_digits"),
        pl.col("nome").map_elements(_normalize_name).alias("nome_normalizado"),
        pl.col("cnpj_basico").cast(pl.Utf8).alias("cnpj_basico"),
    ])

    df = df.with_columns([
        pl.col("doc_digits").map_elements(
            lambda v: hash_identifier(v, CPF_SALT) if v else "",
            return_dtype=pl.Utf8,
        ).alias("hash_documento_socio"),
    ])

    df_pf = df.filter(
        (pl.col("identificador") == "2")
        & (pl.col("doc_digits").str.len_bytes() >= 6)
        & (pl.col("doc_digits") != "")
    )

    df_pf = df_pf.with_columns([
        pl.col("doc_digits").map_elements(
            _cpf_fragment_from_digits, return_dtype=pl.Utf8
        ).alias("cpf_fragment"),
    ])

    socios_por_cpf = (
        df_pf.select([
            pl.col("cpf_fragment"),
            pl.col("nome_normalizado").alias("nome"),
            pl.col("cnpj_basico"),
            pl.col("hash_documento_socio"),
        ])
        .filter(pl.col("cpf_fragment") != "")
        .unique(maintain_order=False)
        .sort(["cpf_fragment", "cnpj_basico", "nome"])
    )

    socios_por_nome = (
        df.select([
            pl.col("nome_normalizado").alias("nome"),
            pl.col("cnpj_basico"),
        ])
        .filter(pl.col("nome") != "")
        .group_by("nome")
        .agg(pl.col("cnpj_basico").drop_nulls().unique().alias("cnpj_basicos"))
        .with_columns(
            pl.when(pl.col("cnpj_basicos").list.len() == 1)
            .then(pl.col("cnpj_basicos").list.first())
            .otherwise(None)
            .alias("cnpj_basico")
        )
        .sort("nome")
    )

    return socios_por_cpf, socios_por_nome


def run_prepare_socios(
    month: str | None = None,
    max_files: int = 3,
    index_url: str | None = None,
    download_prefix: str | Iterable[str] | None = None,
) -> tuple[Path | None, Path | None]:
    """
    Baixa, extrai e gera os Parquets de socios. Retorna caminhos (socios.parquet, socios_nomes.parquet).
    `download_prefix` segue a semantica de `download_many`: None/"all" -> todos os .zip.
    """
    target_month = month
    online_error: Exception | None = None
    if not target_month:
        months: list[str] = []
        try:
            months = list_months(index_url)
        except Exception as err:
            online_error = err
            logger.warning(
                f"Falha ao consultar o indice do CNPJ ({err}). Tentando meses ja baixados em {CNPJ_BASE_DIR}."
            )
        if months:
            target_month = months[-1]
            logger.info(f"Usando mes mais recente do indice: {target_month}")
        else:
            local_months = _list_local_months(CNPJ_BASE_DIR)
            if not local_months:
                msg = "Nenhum mes disponivel (indice indisponivel e cache local vazio)."
                if online_error:
                    raise RuntimeError(msg) from online_error
                raise SystemExit(msg)
            target_month = local_months[-1]
            logger.info(f"Sem acesso ao indice; usando mes local mais recente: {target_month}")

    if not target_month:
        raise SystemExit("Mes alvo nao definido.")

    effective_prefix = download_prefix if download_prefix is not None else SOCIOS_DOWNLOAD_PREFIXES
    normalized_prefix = _normalize_prefix_tokens(effective_prefix)
    prefix_desc = "todos" if normalized_prefix is None else ", ".join(normalized_prefix)

    CNPJ_BASE_DIR.mkdir(parents=True, exist_ok=True)
    month_dir = CNPJ_BASE_DIR / target_month
    month_dir.mkdir(parents=True, exist_ok=True)

    download_error: Exception | None = None
    if online_error is None:
        try:
            month_dir = download_many(
                index_url,
                target_month,
                CNPJ_BASE_DIR,
                prefix=effective_prefix,
                max_files=max_files,
            )
        except Exception as err:
            download_error = err
            logger.warning(
                f"Falha ao baixar arquivos de {target_month} ({err}). Reutilizando arquivos locais em {month_dir}."
            )
    else:
        logger.info(
            f"Pulando download online de {target_month} (indice indisponivel). Reutilizando cache local."
        )

    all_zips = sorted(p for p in month_dir.glob('*.zip') if p.is_file())
    if normalized_prefix:
        all_zips = [p for p in all_zips if any(p.name.lower().startswith(pref) for pref in normalized_prefix)]

    if not all_zips:
        if max_files == 0:
            logger.info("max-files=0 -> nada a baixar ou preparar.")
            return None, None
        base_msg = f"Nenhum arquivo .zip compativel (prefixo: {prefix_desc}) encontrado em {month_dir}."
        if download_error:
            raise SystemExit(f"{base_msg} Falha no download: {download_error}") from download_error
        if online_error:
            raise SystemExit(f"{base_msg} Indice indisponivel.") from online_error
        raise SystemExit(base_msg)

    if max_files is not None and max_files >= 0:
        all_zips = all_zips[:max_files]

    if download_error or online_error:
        logger.info(f"Reutilizando {len(all_zips)} arquivo(s) ja presente(s) em {month_dir}.")

    all_extract_dir = month_dir / 'extracted_all'
    logger.info(f"Extraindo todos os CSVs disponiveis ({len(all_zips)} .zip) para {all_extract_dir}")
    all_csvs = _extract_all(all_zips, all_extract_dir)
    logger.info(f"Total de CSVs extraidos: {len(all_csvs)}")

    socios_csvs = sorted((p for p in all_csvs if p.name.lower().startswith('socios')), key=lambda p: p.name)
    logger.info(f"CSVs de socios disponiveis apos extracao: {len(socios_csvs)}")
    if max_files >= 0:
        socios_csvs = socios_csvs[:max_files]

    if not socios_csvs:
        if max_files == 0:
            logger.info("max-files=0 -> nada a baixar ou preparar.")
            return None, None
        raise SystemExit("Nenhum CSV de socios encontrado apos a extracao dos arquivos.")

    # Apenas os CSVs de socios sao utilizados para montar as tabelas consumidas pelo pipeline.
    socios_por_cpf, socios_por_nome = build_socios_tables(socios_csvs)

    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    out_cpf = SILVER_DIR / 'socios.parquet'
    socios_por_cpf.write_parquet(out_cpf)
    logger.success(f"Gerado: {out_cpf}  (CPFs unicos: {socios_por_cpf.height})")

    out_nome = SILVER_DIR / 'socios_nomes.parquet'
    socios_por_nome.write_parquet(out_nome)
    logger.success(f"Gerado: {out_nome} (nomes unicos: {socios_por_nome.height})")

    meta = {
        'cpf_salt_sha256': hashlib.sha256(CPF_SALT.encode('utf-8')).hexdigest(),
        'source_month': target_month,
        'max_files': max_files,
    }
    meta_path = SILVER_DIR / 'socios_meta.json'
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')
    logger.info(f"Metadados registrados em: {meta_path}")

    return out_cpf, out_nome


def main():
    parser = argparse.ArgumentParser(description="Baixa e prepara base de Sócios (QSA) para enriquecimento")
    parser.add_argument("--month", help="Mês AAAA-MM (ex.: 2024-09). Se não informado, pega o mais recente.", default=None)
    parser.add_argument("--max-files", type=int, default=3, help="Limita quantos zips de Sócios baixar (p/ teste rápido). Use -1 para todos.")
    parser.add_argument("--index-url", default=None, help="URL base do índice (deixe vazio para autodetectar)")
    parser.add_argument(
        "--download-prefix",
        default=SOCIOS_DOWNLOAD_PREFIXES,
        help="Prefixo(s) opcionais para download (ex.: 'Socios' ou 'Socios,Empresas'). Deixe vazio/'all' para todos.",
    )
    args = parser.parse_args()

    run_prepare_socios(
        month=args.month,
        max_files=args.max_files,
        index_url=args.index_url,
        download_prefix=args.download_prefix,
    )

if __name__ == "__main__":
    main()
