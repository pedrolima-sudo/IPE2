"""
MÓDULO DE CONFIGURAÇÕES
==========================

O módulo settings gerencia configurações e diretórios do projeto.

Autor: Pedro Henrique Lima Silva
Data de criação: 15/10/2025
Última modificação: 16/10/2025
"""

from __future__ import annotations
from pathlib import Path
import os
from dotenv import load_dotenv

# Define a raiz do projeto (2 níveis acima deste arquivo)
ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

# Diretórios principais (os torna absolutos e normalizados)
CNPJ_BASE_DIR   = Path(os.getenv("CNPJ_BASE_DIR", ROOT / "data/raw/cnpj")).resolve()
PARQUET_OUT_DIR = Path(os.getenv("PARQUET_OUT_DIR", ROOT / "data/gold")).resolve()
SILVER_DIR      = Path(os.getenv("SILVER_DIR", ROOT / "data/silver")).resolve()
LOG_DIR         = Path(os.getenv("LOG_DIR", ROOT / "logs")).resolve()

# Flags e configs
ENV           = os.getenv("ENV", "dev")
USE_DUCKDB    = os.getenv("USE_DUCKDB", "1") in ("1", "true", "True")
SCHEDULE_CRON = os.getenv("SCHEDULE_CRON", "0 3 * * *")
SOCIOS_MAX_FILES = int(os.getenv("SOCIOS_MAX_FILES", "-1"))
_SOCIOS_DOWNLOAD_PREFIXES_RAW = os.getenv("SOCIOS_DOWNLOAD_PREFIXES")
SOCIOS_DOWNLOAD_PREFIXES = _SOCIOS_DOWNLOAD_PREFIXES_RAW.strip() if _SOCIOS_DOWNLOAD_PREFIXES_RAW else None
CPF_SALT      = os.getenv("CPF_SALT")

# Caminho direto para o Excel de egressos
EGRESSO_EXCEL_FILE = os.getenv("EGRESSO_EXCEL_FILE")

# Cria pastas se não existirem
for p in (CNPJ_BASE_DIR, PARQUET_OUT_DIR, SILVER_DIR, LOG_DIR):
    p.mkdir(parents=True, exist_ok=True)

# Valida existência de variáveis obrigatórias
_missing = [k for k,v in {"CPF_SALT": CPF_SALT}.items() if not v]
if _missing:
    raise RuntimeError(f"Variáveis obrigatórias ausentes no .env: {_missing}")
