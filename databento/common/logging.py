import logging
import os
import sys
from typing import Optional

import databento


DATABENTO_LOG = os.environ.get("DATABENTO_LOG", "").upper()

logger = logging.getLogger("databento")

_DEBUG = "DEBUG"
_INFO = "INFO"
_ERROR = "ERROR"


def _console_log_level() -> Optional[str]:
    if databento.log:
        databento.log = databento.log.upper()

    if databento.log in [_DEBUG, _INFO, _ERROR]:
        return databento.log
    elif DATABENTO_LOG in [_DEBUG, _INFO, _ERROR]:
        return DATABENTO_LOG
    else:
        return None


def log_debug(msg: str) -> None:
    """Log the given message with DEBUG level."""
    log_level = _console_log_level()
    if log_level == [_DEBUG, _INFO, _ERROR]:
        print(f"DEBUG: {msg}", file=sys.stderr)
    logger.debug(msg)


def log_info(msg: str) -> None:
    """Log the given message with INFO level."""
    log_level = _console_log_level()
    if log_level in [_INFO, _ERROR]:
        print(f"INFO: {msg}", file=sys.stderr)
    logger.info(msg)


def log_error(msg: str) -> None:
    """Log the given message with ERROR level."""
    log_level = _console_log_level()
    if log_level in [_ERROR]:
        print(f"ERROR: {msg}", file=sys.stderr)
    logger.error(msg)
