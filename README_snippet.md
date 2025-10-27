# Portal de Egressos – ETL (Skeleton)

## Rodar manualmente
```powershell
# na raiz do projeto
$env:EGRESSO_EXCEL_FILE = "C:\ipe2_archives\excel\egressos_ime_db_fake.xlsx"  
python -m src.etl.pipeline                      
python -m src.etl.pipeline --excel "C:\ipe2_archives\excel\egressos_ime_db_fake.xlsx"
```

## Scheduler (todo dia 03:00 via .env SCHEDULE_CRON)
```powershell
python -m src.etl.scheduler
```
> O scheduler executa `prepare_socios` antes do pipeline. Controle a quantidade de zips com a variavel `.env` `SOCIOS_MAX_FILES` (padrao -1 para todos).

## Saída para Power BI
- Parquet em `data/gold/egressos/`
- Arquivo único: `egressos.parquet`
- Particionado por faixa etária: `partition_faixa_etaria/`

> Observação: o enriquecimento de fundadores usa bases opcionais em `data/silver/socios*.parquet/csv`. Sem elas, o campo `socio` ficará **False** por padrão.


```powershell
# 1) Baixar e preparar os arquivos mais recentes (limita 3 zips p/ teste)
python -m src.cnpj.prepare_socios --max-files 3

# 2) Ou fixar um mês específico (ex.: 2024-09) e baixar mais zips
python -m src.cnpj.prepare_socios --month 2024-09 --max-files 10

# 3) Depois rode o pipeline principal
python -m src.etl.pipeline --excel "C:\\caminho\\egressos.xlsx"
```

## Scripts de apoio
```powershell
# Executa prepare_socios e o pipeline em sequencia, com parametros opcionais
.\scripts\run_pipeline.ps1 -SociosMaxFiles 3 -ExcelPath "C:\ipe2_archives\excel\egressos_ime_db_fake.xlsx"

# Ativa o scheduler APScheduler usando as variaveis definidas no .env
.\scripts\run_scheduler.ps1
```

**Notas**
- Os dados oficiais estao em `https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/` (mirror mantido pela RFB). O script faz fallback para `https://dadosabertos.rfb.gov.br/CNPJ/dados_abertos_cnpj/` se o host novo ficar fora e, por padrão, baixa todos os arquivos `.zip` do mês (use `--prefix` ou a variável `.env` `SOCIOS_DOWNLOAD_PREFIXES` para restringir, ex.: `Socios`).
- O Parquet de saída vai para `data/silver/socios.parquet` (CPF deduplicado) e `data/silver/socios_nomes.parquet` (nomes normalizados).
- **LGPD**: mantenha `data/silver` fora do Git (já previsto no `.gitignore`). O join por CPF acontece só localmente; o **Power BI** recebe apenas o `id_pessoa` (hash do CPF) e a flag agregada `socio`.
