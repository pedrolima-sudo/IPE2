"""
MÓDULO DE PREPARAÇÃO DOS DADOS DE SÓCIOS
========================================================

Prepara os dados de sócios extraídos dos arquivos .zip do CNPJ,
gerando tabelas deduplicadas por CPF e por nome.

Data de criação: 15/10/2025
Última modificação: 18/10/2025
"""
from __future__ import annotations
import zipfile
import argparse
from pathlib import Path

import polars as pl
from loguru import logger
from unidecode import unidecode

try:
    from .download_cnpj import list_months, download_many
    from ..utils.settings import CNPJ_BASE_DIR, SILVER_DIR
except ImportError:  # Execução direta (python src/cnpj/prepare_socios.py)
    import sys
    from pathlib import Path as _Path

    sys.path.append(str(_Path(__file__).resolve().parents[2]))
    from src.cnpj.download_cnpj import list_months, download_many  # type: ignore
    from src.utils.settings import CNPJ_BASE_DIR, SILVER_DIR  # type: ignore


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


def _extract_all(zips: list[Path], out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    csvs: list[Path] = []
    for z in zips:
        logger.info(f"Extraindo: {z}")
        with zipfile.ZipFile(z, "r") as zf:
            for member in zf.namelist():
                lower = member.lower()
                if lower.endswith(".csv") or lower.endswith(".sociocsv"):
                    name = Path(member).name
                    if lower.endswith(".sociocsv"):
                        name = f"{Path(name).stem}.csv"
                    target = out_dir / name
                    with zf.open(member) as src, open(target, "wb") as dst:
                        dst.write(src.read())
                    csvs.append(target)
    logger.info(f"CSVs extraídos: {len(csvs)}")
    return csvs


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


def main():
    parser = argparse.ArgumentParser(description="Baixa e prepara base de Sócios (QSA) para enriquecimento")
    parser.add_argument("--month", help="Mês AAAA-MM (ex.: 2024-09). Se não informado, pega o mais recente.", default=None)
    parser.add_argument("--max-files", type=int, default=3, help="Limita quantos zips de Sócios baixar (p/ teste rápido). Use -1 para todos.")
    parser.add_argument("--index-url", default=None, help="URL base do índice (deixe vazio para autodetectar)")
    args = parser.parse_args()

    month = args.month
    if not month:
        months = list_months(args.index_url)
        if not months:
            raise SystemExit("Nenhum mês encontrado no índice da RFB.")
        month = months[-1]
        logger.info(f"Usando mês mais recente: {month}")

    CNPJ_BASE_DIR.mkdir(parents=True, exist_ok=True)
    month_dir = download_many(args.index_url, month, CNPJ_BASE_DIR, prefix="Socios", max_files=args.max_files)

    zips = sorted(p for p in month_dir.glob("Socios*.zip") if p.is_file())
    if args.max_files >= 0:
        zips = zips[:args.max_files]

    if not zips:
        if args.max_files == 0:
            logger.info("max-files=0 -> nada a baixar ou preparar.")
            return
        raise SystemExit("Nenhum arquivo .zip de socios encontrado apos download.")

    extract_dir = month_dir / "extracted"
    csvs = _extract_all(zips, extract_dir)

    socios_por_cpf, socios_por_nome = build_socios_tables(csvs)

    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    out_cpf = SILVER_DIR / "socios.parquet"
    socios_por_cpf.write_parquet(out_cpf)
    logger.success(f"Gerado: {out_cpf}  (CPFs únicos: {socios_por_cpf.height})")

    out_nome = SILVER_DIR / "socios_nomes.parquet"
    socios_por_nome.write_parquet(out_nome)
    logger.success(f"Gerado: {out_nome} (nomes únicos: {socios_por_nome.height})")


if __name__ == "__main__":
    main()
