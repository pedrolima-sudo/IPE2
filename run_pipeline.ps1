"""Para executar o pipeline ETL completo só colocar no powershell:
.\run_pipeline.ps1"""
param(
    [string]$ExcelPath = "C:\ipe2_archives\excel\egressos_ime_db_fake.xlsx",
    [int]$SociosMaxFiles = -1
)

$ErrorActionPreference = "Stop"

$repoRoot    = Split-Path -Parent $MyInvocation.MyCommand.Path
$venvScript  = Join-Path $repoRoot ".venv\Scripts\Activate.ps1"

if (-not (Test-Path $venvScript)) {
    throw "Virtualenv não encontrado em $venvScript"
}

. $venvScript

python -m src.cnpj.prepare_socios --max-files $SociosMaxFiles
python -m src.etl.pipeline --excel $ExcelPath
