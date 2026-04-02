import logging
import sys

def get_logger(file_name: str) -> logging.Logger:


    logger = logging.getLogger(file_name)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    console_handler = logging.StreamHandler(sys.stdout)
    console_error_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(formatter)
    console_error_handler.setFormatter(formatter)

    console_handler.setLevel(logging.INFO)
    console_error_handler.setLevel(logging.ERROR)

    if not logger.handlers:
        logger.addHandler(console_handler)
        logger.addHandler(console_error_handler)

    logger.setLevel(logging.INFO)

    return logger