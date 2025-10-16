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
