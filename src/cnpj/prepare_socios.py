from __future__ import annotations
from pathlib import Path
import zipfile
import argparse
import polars as pl
from loguru import logger
from unidecode import unidecode

from .download_cnpj import list_months, list_files, download_files, DEFAULT_INDEX_URL
from ..utils.settings import CNPJ_BASE_DIR, SILVER_DIR


def _digits_only(s: str | None) -> str:
    if not s:
        return ""
    return "".join(ch for ch in str(s) if ch.isdigit())


def _normalize_name(s: str | None) -> str:
    if not s:
        return ""
    return " ".join(unidecode(str(s)).upper().split())


def _extract_all(zips: list[Path], out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    csvs: list[Path] = []
    for z in zips:
        logger.info(f"Extraindo: {z}")
        with zipfile.ZipFile(z, "r") as zf:
            for member in zf.namelist():
                if member.lower().endswith(".csv"):
                    target = out_dir / Path(member).name
                    with zf.open(member) as src, open(target, "wb") as dst:
                        dst.write(src.read())
                    csvs.append(target)
    logger.info(f"CSVs extraídos: {len(csvs)}")
    return csvs


def build_socios_tables(csvs: list[Path]) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Retorna (socios_por_cpf, socios_por_nome).
    - socios_por_cpf: colunas ['cpf'] (deduplicado, somente pessoa física)
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
                has_header=True,
                ignore_errors=True,
                encoding="latin1",
                infer_schema_length=0,
                quote_char='"'
            )
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

    # Filtra pessoa física (identificador = 2) e CPF com 11 dígitos
    df = df.with_columns([
        pl.col("identificador").cast(pl.Utf8),
        pl.col("doc").map_elements(_digits_only).alias("doc_digits"),
    ])

    df_pf = df.filter((pl.col("identificador") == "2") & (pl.col("doc_digits").str.len_bytes() == 11))

    socios_por_cpf = (
        df_pf.select([pl.col("doc_digits").alias("cpf")])
             .unique(maintain_order=False)
             .sort("cpf")
    )

    socios_por_nome = (
        df.select([pl.col("nome")])
          .with_columns(pl.col("nome").map_elements(_normalize_name))
          .filter(pl.col("nome") != "")
          .unique(maintain_order=False)
          .sort("nome")
    )

    return socios_por_cpf, socios_por_nome


def main():
    parser = argparse.ArgumentParser(description="Baixa e prepara base de Sócios (QSA) para enriquecimento")
    parser.add_argument("--month", help="Mês AAAA-MM (ex.: 2024-09). Se não informado, pega o mais recente.", default=None)
    parser.add_argument("--max-files", type=int, default=3, help="Limita quantos zips de Sócios baixar (p/ teste rápido). Use -1 para todos.")
    parser.add_argument("--index-url", default=DEFAULT_INDEX_URL, help="URL de índice oficial da RFB")
    args = parser.parse_args()

    month = args.month
    if not month:
        months = list_months(args.index_url)
        if not months:
            raise SystemExit("Nenhum mês encontrado no índice da RFB.")
        month = months[-1]
        logger.info(f"Usando mês mais recente: {month}")

    month_dir = CNPJ_BASE_DIR / month
    month_dir.mkdir(parents=True, exist_ok=True)

    files = list_files(month, args.index_url, prefix_filter="Socios")
    if args.max_files >= 0:
        files = files[:args.max_files]

    zips = download_files(files, month_dir)

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
