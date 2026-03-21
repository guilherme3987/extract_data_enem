import logging

# Instanciando logger
logger = logging.getLogger("airflow.task")


def log_extraction_stats(years_processed, total_questions, duration):
    logger.info("=" * 60)
    logger.info("RESUMO DA EXTRAÇÃO")
    logger.info("=" * 60)
    logger.info(f"Anos processados : {len(years_processed)} {years_processed}")
    logger.info(f"Total de questões: {total_questions}")
    logger.info(f"Duração total    : {duration:.2f} segundos")

    if total_questions == 0:
        logger.warning("Nenhuma questão foi extraída. Verifique o repositório.")

    logger.info("=" * 60)