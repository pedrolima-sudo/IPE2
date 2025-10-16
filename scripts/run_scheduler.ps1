# Powershell helper para iniciar o agendador (APScheduler)
$ErrorActionPreference = "Stop"
if (Test-Path .\.venv\Scripts\Activate.ps1) { . .\.venv\Scripts\Activate.ps1 }
python -m src.etl.scheduler