# Powershell helper para iniciar o agendador (APScheduler)
$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot  = Split-Path -Parent $scriptDir

if (-not (Test-Path (Join-Path $repoRoot ".venv"))) {
    $repoRoot = $scriptDir
}

$venvScript = Join-Path $repoRoot ".venv\Scripts\Activate.ps1"
if (-not (Test-Path $venvScript)) {
    throw "Virtualenv não encontrada em $venvScript"
}

. $venvScript

python -m src.etl.scheduler
