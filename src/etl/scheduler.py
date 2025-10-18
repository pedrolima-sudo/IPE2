"""
MÓDULO PARA AGENDAMENTO AUTOMÁTICO DO PIPELINE ETL DE EGRESSOS
================================================================

Este módulo agenda a execução periódica do pipeline ETL de egressos usando APScheduler.

Autor: Pedro Henrique Lima Silva
Data de criação: 15/10/2025
Última modificação: 16/10/2025
"""

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
