# Powershell helper para rodar o pipeline manualmente
param(
  [string]$ExcelPath = ""
)

$ErrorActionPreference = "Stop"

# Ativa venv se estiver na raiz
if (Test-Path .\.venv\Scripts\Activate.ps1) { . .\.venv\Scripts\Activate.ps1 }

if ($ExcelPath -eq "") {
  python -m src.etl.pipeline
} else {
  python -m src.etl.pipeline --excel $ExcelPath
}