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
import zipfile
import argparse
from collections.abc import Iterable
from pathlib import Path

import polars as pl
from loguru import logger
from unidecode import unidecode

try:
    from .download_cnpj import list_months, download_many
    from ..utils.settings import (
        CNPJ_BASE_DIR,
        SILVER_DIR,
        SOCIOS_DOWNLOAD_PREFIXES,
    )
except ImportError:  # Execução direta (python src/cnpj/prepare_socios.py)
    import sys
    from pathlib import Path as _Path

    sys.path.append(str(_Path(__file__).resolve().parents[2]))
    from src.cnpj.download_cnpj import list_months, download_many  # type: ignore
    from src.utils.settings import (  # type: ignore
        CNPJ_BASE_DIR,
        SILVER_DIR,
        SOCIOS_DOWNLOAD_PREFIXES,
    )


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


from pathlib import Path
import zipfile
import shutil  # para copyfileobj
from loguru import logger

def _extract_all(zips: list[Path], out_dir: Path, *, preserve_dirs: bool = False) -> list[Path]:
    """
    Extrai todos os arquivos de cada zip, renomeando os CSVs para <nome_do_zip>.csv.
    Se um zip contiver múltiplos arquivos de dados, recebe sufixos __2, __3, ...
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    extraidos: list[Path] = []

    for z in zips:
        logger.info(f"Extraindo: {z}")
        zip_stem = z.stem
        emitted = 0
        with zipfile.ZipFile(z, "r") as zf:
            if preserve_dirs:
                zf.extractall(out_dir)
                for info in zf.infolist():
                    if info.is_dir():
                        continue
                    emitted += 1
                    base_name = zip_stem if emitted == 1 else f"{zip_stem}__{emitted}"
                    target = out_dir / info.filename
                    target.parent.mkdir(parents=True, exist_ok=True)
                    desired = target.with_name(f"{base_name}.csv")
                    if desired != target:
                        if desired.exists():
                            desired.unlink()
                        target.rename(desired)
                        target = desired
                    extraidos.append(target)
            else:
                # achata: coloca tudo direto em out_dir
                for info in zf.infolist():
                    if info.is_dir():
                        continue
                    emitted += 1
                    base_name = zip_stem if emitted == 1 else f"{zip_stem}__{emitted}"
                    target = out_dir / f"{base_name}.csv"

                    # evita path traversal (zip slip)
                    out_res = out_dir.resolve()
                    tgt_parent = target.resolve().parent
                    if tgt_parent != out_res and out_res not in tgt_parent.parents:
                        raise ValueError("caminho inseguro dentro do zip")

                    with zf.open(info) as src, open(target, "wb") as dst:
                        shutil.copyfileobj(src, dst)

                    extraidos.append(target)

    logger.info(f"Arquivos extraidos: {len(extraidos)}")
    return extraidos



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

    df = df.select([
        pl.col(col_nome).alias("nome"),
        pl.col(col_id).alias("identificador"),
        pl.col(col_doc).alias("doc")
    ])

    df = df.with_columns([
        pl.col("identificador").cast(pl.Utf8),
        pl.col("doc").map_elements(_digits_only).alias("doc_digits"),
        pl.col("nome").map_elements(_normalize_name).alias("nome_normalizado"),
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
        ])
             .unique(maintain_order=False)
             .sort(["cpf_fragment", "nome"])
    )

    socios_por_nome = (
        df.select([pl.col("nome_normalizado").alias("nome")])
          .filter(pl.col("nome") != "")
          .unique(maintain_order=False)
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
    Baixa, extrai e gera os Parquets de sócios. Retorna caminhos (socios.parquet, socios_nomes.parquet).
    `download_prefix` segue a semântica de `download_many`: None/"all" → todos os .zip.
    """
    target_month = month
    if not target_month:
        months = list_months(index_url)
        if not months:
            raise SystemExit("Nenhum mês encontrado.")
        target_month = months[-1]
        logger.info(f"Usando mês mais recente: {target_month}")

    effective_prefix = download_prefix if download_prefix is not None else SOCIOS_DOWNLOAD_PREFIXES

    CNPJ_BASE_DIR.mkdir(parents=True, exist_ok=True)
    month_dir = download_many(
        index_url,
        target_month,
        CNPJ_BASE_DIR,
        prefix=effective_prefix,
        max_files=max_files,
    )

    all_zips = sorted(p for p in month_dir.glob("*.zip") if p.is_file())
    if not all_zips:
        raise SystemExit("Nenhum arquivo .zip encontrado após download.")

    all_extract_dir = month_dir / "extracted_all"
    logger.info(f"Extraindo todos os CSVs disponíveis ({len(all_zips)} .zip) para {all_extract_dir}")
    all_csvs = _extract_all(all_zips, all_extract_dir)
    logger.info(f"Total de CSVs extraídos: {len(all_csvs)}")

    socios_csvs = sorted((p for p in all_csvs if p.name.lower().startswith("socios")), key=lambda p: p.name)
    logger.info(f"CSVs de sócios disponíveis após extração: {len(socios_csvs)}")
    if max_files >= 0:
        socios_csvs = socios_csvs[:max_files]

    if not socios_csvs:
        if max_files == 0:
            logger.info("max-files=0 -> nada a baixar ou preparar.")
            return None, None
        raise SystemExit("Nenhum CSV de socios encontrado apos a extração dos arquivos.")

    # Apenas os CSVs de Sócios são utilizados para montar as tabelas consumidas pelo pipeline.
    socios_por_cpf, socios_por_nome = build_socios_tables(socios_csvs)

    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    out_cpf = SILVER_DIR / "socios.parquet"
    socios_por_cpf.write_parquet(out_cpf)
    logger.success(f"Gerado: {out_cpf}  (CPFs únicos: {socios_por_cpf.height})")

    out_nome = SILVER_DIR / "socios_nomes.parquet"
    socios_por_nome.write_parquet(out_nome)
    logger.success(f"Gerado: {out_nome} (nomes únicos: {socios_por_nome.height})")

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
