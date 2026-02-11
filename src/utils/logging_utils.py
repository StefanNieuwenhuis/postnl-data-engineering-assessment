import logging

logger = logging.getLogger(__name__)


def log_header(title: str, separator: str = "=") -> None:
    logger.info("")
    logger.info(separator * 80)
    logger.info(title.center(80))
    logger.info(separator * 80)


def log_footer(separator: str = "=") -> None:
    logger.info(separator * 80)
