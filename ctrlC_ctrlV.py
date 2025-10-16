# =============================
# File: src/utils/settings.py
# =============================
from __future__ import annotations
from pathlib import Path
import os
from dotenv import load_dotenv

# Descobre raiz do projeto (2 níveis acima deste arquivo)
ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

# Diretórios principais
CNPJ_BASE_DIR   = Path(os.getenv("CNPJ_BASE_DIR", ROOT / "data/raw/cnpj")).resolve()
PARQUET_OUT_DIR = Path(os.getenv("PARQUET_OUT_DIR", ROOT / "data/gold")).resolve()
SILVER_DIR      = Path(os.getenv("SILVER_DIR", ROOT / "data/silver")).resolve()
LOG_DIR         = Path(os.getenv("LOG_DIR", ROOT / "logs")).resolve()

# Flags e configs
ENV           = os.getenv("ENV", "dev")
USE_DUCKDB    = os.getenv("USE_DUCKDB", "1") in ("1", "true", "True")
SCHEDULE_CRON = os.getenv("SCHEDULE_CRON", "0 3 * * *")
CPF_SALT      = os.getenv("CPF_SALT")

# (Opcional) caminho direto para o Excel de egressos (pode vir via CLI também)
EGRESSO_EXCEL_FILE = os.getenv("EGRESSO_EXCEL_FILE")

# Cria pastas se não existirem
for p in (CNPJ_BASE_DIR, PARQUET_OUT_DIR, SILVER_DIR, LOG_DIR):
    p.mkdir(parents=True, exist_ok=True)

# Valida obrigatórios
_missing = [k for k,v in {"CPF_SALT": CPF_SALT}.items() if not v]
if _missing:
    raise RuntimeError(f"Variáveis obrigatórias ausentes no .env: {_missing}")

# =============================
# File: src/utils/security.py
# =============================
import hmac
import hashlib


def hash_identifier(value: str, salt: str) -> str:
    """Gera um hash HMAC-SHA256 estável para identificar a pessoa sem expor CPF.
    - value: string original (ex.: CPF sem máscara)
    - salt: segredo do .env (CPF_SALT)
    """
    if value is None:
        return ""
    value = str(value).strip()
    if not value:
        return ""
    return hmac.new(salt.encode("utf-8"), value.encode("utf-8"), hashlib.sha256).hexdigest()

# =============================
# File: src/utils/clean.py
# =============================
import re
from unidecode import unidecode


def normalize_name(name: str) -> str:
    if name is None:
        return ""
    s = unidecode(str(name)).strip()
    s = re.sub(r"\s+", " ", s)
    return s.upper()

# =============================
# File: src/etl/ingest_excel.py
# =============================
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

# =============================
# File: src/etl/transform.py
# =============================
from __future__ import annotations
from datetime import date
from dateutil.relativedelta import relativedelta
import polars as pl
from validate_docbr import CPF
from loguru import logger
from ..utils.clean import normalize_name
from ..utils.security import hash_identifier
from ..utils.settings import CPF_SALT


_cpf_validator = CPF()


def _clean_cpf(s: str) -> str:
    if s is None:
        return ""
    s = "".join(ch for ch in str(s) if ch.isdigit())
    if len(s) == 11 and _cpf_validator.validate(s):
        return s
    return ""


def _calc_idade(nasc: date | None) -> int | None:
    if not nasc:
        return None
    try:
        today = date.today()
        return relativedelta(today, nasc).years
    except Exception:
        return None


def _faixa_etaria(idade: int | None) -> str:
    if idade is None:
        return "IGNORADO"
    bins = [0, 18, 25, 35, 45, 55, 65, 200]
    labels = ["<18", "18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
    for i in range(len(labels)):
        if bins[i] <= idade < bins[i+1]:
            return labels[i]
    return "IGNORADO"


def basic_transform(df: pl.DataFrame) -> pl.DataFrame:
    """Limpeza e features básicas: nomes, CPF, idade/faixa, IDs hash.
    Requer colunas: nome, cpf, data_nascimento (se existirem no input).
    """
    logger.info("Transformando dados básicos de egressos…")

    # aplica limpeza de strings
    df = df.with_columns([
        pl.col("nome").map_elements(normalize_name, return_dtype=pl.Utf8).alias("nome_norm"),
        pl.col("nome_pai").map_elements(normalize_name, return_dtype=pl.Utf8).alias("nome_pai_norm"),
        pl.col("nome_mae").map_elements(normalize_name, return_dtype=pl.Utf8).alias("nome_mae_norm"),
    ])

    # limpa CPF e cria ID hash
    df = df.with_columns([
        pl.col("cpf").map_elements(_clean_cpf, return_dtype=pl.Utf8).alias("cpf_limpo"),
    ])

    df = df.with_columns([
        pl.col("cpf_limpo").map_elements(lambda x: hash_identifier(x, CPF_SALT), return_dtype=pl.Utf8).alias("id_pessoa"),
    ])

    # idade & faixa
    df = df.with_columns([
        pl.col("data_nascimento").cast(pl.Date, strict=False).alias("dn_date"),
    ])

    df = df.with_columns([
        pl.col("dn_date").map_elements(_calc_idade, return_dtype=pl.Int64).alias("idade"),
        pl.col("idade").map_elements(_faixa_etaria, return_dtype=pl.Utf8).alias("faixa_etaria"),
    ])

    # último curso (heurística simples: usa 'curso' se existir)
    if "curso" in df.columns:
        df = df.with_columns(pl.col("curso").fill_null("").alias("ultimo_curso"))
    else:
        df = df.with_columns(pl.lit("").alias("ultimo_curso"))

    return df

# =============================
# File: src/etl/enrich_cnpj.py
# =============================
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

# =============================
# File: src/etl/export_powerbi.py
# =============================
from __future__ import annotations
from pathlib import Path
import polars as pl
from loguru import logger
from ..utils.settings import PARQUET_OUT_DIR


def export_parquet(df: pl.DataFrame, dataset_name: str = "egressos") -> Path:
    out_dir = PARQUET_OUT_DIR / dataset_name
    out_dir.mkdir(parents=True, exist_ok=True)

    # Grava arquivo único + particionado por faixa_etaria (útil para filtros no Power BI)
    file_all = out_dir / f"{dataset_name}.parquet"
    logger.info(f"Gravando Parquet consolidado: {file_all}")
    df.write_parquet(file_all)

    part_dir = out_dir / "partition_faixa_etaria"
    part_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Gravando Parquet particionado por faixa_etaria em: {part_dir}")
    for faixa, pdf in df.partition_by("faixa_etaria", as_dict=True).items():
        safe = "NULL" if faixa is None else str(faixa).replace("/", "-")
        out_file = part_dir / f"faixa={safe}.parquet"
        pdf.write_parquet(out_file)

    return out_dir

# =============================
# File: src/etl/pipeline.py
# =============================
from __future__ import annotations
import argparse
from pathlib import Path
from loguru import logger
import polars as pl

from .ingest_excel import read_egressos_excel
from .transform import basic_transform
from .enrich_cnpj import mark_founders
from .export_powerbi import export_parquet
from ..utils.settings import EGRESSO_EXCEL_FILE


def run_pipeline(excel_path: str | None) -> Path:
    if not excel_path:
        if not EGRESSO_EXCEL_FILE:
            raise SystemExit("Informe o caminho do Excel via --excel OU defina EGRESSO_EXCEL_FILE no .env")
        excel_path = EGRESSO_EXCEL_FILE

    df = read_egressos_excel(Path(excel_path))
    df = basic_transform(df)
    df = mark_founders(df)

    # selecione apenas colunas úteis ao BI (ajuste conforme necessário)
    cols = [
        "id_pessoa", "matricula", "nome", "nome_norm",
        "idade", "faixa_etaria",
        "data_nascimento", "data_ingresso", "data_formacao",
        "ultimo_curso", "nivel", "codigo_curso",
        "eh_socio_fundador", "eh_socio_por_cpf", "eh_socio_por_nome",
    ]
    cols = [c for c in cols if c in df.columns]
    df_out = df.select(cols)

    out_dir = export_parquet(df_out)
    logger.success(f"Pipeline concluído. Saída em: {out_dir}")
    return out_dir


def main():
    parser = argparse.ArgumentParser(description="Portal Egressos – Pipeline ETL")
    parser.add_argument("--excel", help="Caminho do Excel de egressos", default=None)
    args = parser.parse_args()
    run_pipeline(args.excel)


if __name__ == "__main__":
    main()

# =============================
# File: src/etl/scheduler.py
# =============================
from __future__ import annotations
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger
from .pipeline import run_pipeline
from ..utils.settings import SCHEDULE_CRON, EGRESSO_EXCEL_FILE


def job():
    try:
        logger.info("[scheduler] Iniciando job ETL…")
        run_pipeline(EGRESSO_EXCEL_FILE)
        logger.success("[scheduler] Job ETL finalizado com sucesso.")
    except Exception as e:
        logger.exception(e)


def main():
    if not EGRESSO_EXCEL_FILE:
        raise SystemExit("Defina EGRESSO_EXCEL_FILE no .env para o agendador.")

    sched = BlockingScheduler(timezone="America/Sao_Paulo")
    sched.add_job(job, CronTrigger.from_crontab(SCHEDULE_CRON))
    logger.info(f"Scheduler iniciado com CRON='{SCHEDULE_CRON}' – Ctrl+C para parar")
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler finalizado.")


if __name__ == "__main__":
    main()

