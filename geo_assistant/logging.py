import logging
from logging.config import dictConfig

LOGGING_CONFIG = {
    "version": 1,
    "formatters": {
        "default": {
            "format": "[%(asctime)s] %(levelname)s - %(name)s - %(message)s",
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "level": "DEBUG",
        }
    },
    "root": {
        "handlers": ["console"],
        "level": "DEBUG",
    },
}

_configured = False

def configure_logging() -> None:
    global _configured
    if not _configured:
        dictConfig(LOGGING_CONFIG)
        _configured = True


def get_logger(name: str) -> logging.Logger:
    configure_logging()
    return logging.getLogger(name)
