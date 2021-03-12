import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logger(logfile_name: str, log_dir: str = ""):
    if "." not in logfile_name:
        logfile_name += ".log"

    if log_dir:
        log_path = os.path.join(log_dir, "logs", logfile_name)
    else:
        log_path = os.path.join(LOG_DIR, logfile_name)

    file_logger = RotatingFileHandler(
        log_path,
        maxBytes=LOG_SIZE,
        backupCount=LOG_FILE_BACKUP_CNT,
    )
    console_logger = logging.StreamHandler()
    console_logger.setFormatter(logging.Formatter(LOG_FORMAT))

    logging.basicConfig(
        level=logging.INFO, format=LOG_FORMAT,
        handlers=[file_logger, console_logger]
    )


LOG_FILE_BACKUP_CNT = 10
LOG_FORMAT = "%(asctime)s - %(name)s:%(lineno)s - %(levelname)s - %(message)s"
LOG_DIR = os.environ.get("LOG_DIR") or os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "logs"
)
LOG_SIZE = 64 * 1024 * 1024
