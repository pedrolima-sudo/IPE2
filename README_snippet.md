# Portal de Egressos – ETL (Skeleton)

## Rodar manualmente
```powershell
# na raiz do projeto
$env:EGRESSO_EXCEL_FILE = "C:\\caminho\\egressos.xlsx"  # (ou configure no .env)
python -m src.etl.pipeline                      # usa EGRESSO_EXCEL_FILE do .env
python -m src.etl.pipeline --excel C:\\...xlsx  # ou passe via CLI
```

## Scheduler (todo dia 03:00 via .env SCHEDULE_CRON)
```powershell
python -m src.etl.scheduler
```

## Saída para Power BI
- Parquet em `data/gold/egressos/`
- Arquivo único: `egressos.parquet`
- Particionado por faixa etária: `partition_faixa_etaria/`

> Observação: o enriquecimento de fundadores usa bases opcionais em `data/silver/socios*.parquet/csv`. Sem elas, o campo `eh_socio_fundador` ficará **False** por padrão.
