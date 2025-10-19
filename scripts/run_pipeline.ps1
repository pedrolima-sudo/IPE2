# Para executar o pipeline ETL completo:
#   .\run_pipeline.ps1
# Parâmetros opcionais:
#   -SociosMaxFiles 3 -ExcelPath "C:\caminho\egressos.xlsx"

param(
    [string]$ExcelPath = "C:\ipe2_archives\excel\egressos_ime_db_fake.xlsx",
    [int]$SociosMaxFiles = -1 #-1 para usar todos os arquivos disponíveis
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot  = Split-Path -Parent $scriptDir

if (-not (Test-Path (Join-Path $repoRoot ".venv"))) {
    # fallback: talvez a estrutura seja diretamente na mesma pasta
    $repoRoot = $scriptDir
}

$venvScript = Join-Path $repoRoot ".venv\Scripts\Activate.ps1"

if (-not (Test-Path $venvScript)) {
    throw "Virtualenv não encontrado em $venvScript"
}

. $venvScript

python -m src.cnpj.prepare_socios --max-files $SociosMaxFiles
python -m src.etl.pipeline --excel $ExcelPath
