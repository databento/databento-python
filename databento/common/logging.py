import logging
import os
import sys

import databento


DATABENTO_LOG = os.environ.get("DATABENTO_LOG", "").upper()

logger = logging.getLogger("databento")

_DEBUG = "DEBUG"
_INFO = "INFO"


def _console_log_level():
    if databento.log:
        databento.log = databento.log.upper()

    if databento.log in [_DEBUG, _INFO]:
        return databento.log
    elif DATABENTO_LOG in [_DEBUG, _INFO]:
        return DATABENTO_LOG
    else:
        return None


def log_debug(msg: str):
    """Log the given message with DEBUG level."""
    log_level = _console_log_level()
    if log_level == _DEBUG:
        print(f"DEBUG: {msg}", file=sys.stderr)
    logger.debug(msg)


def log_info(msg: str):
    """Log the given message with INFO level."""
    log_level = _console_log_level()
    if log_level in [_DEBUG, _INFO]:
        print(f"INFO: {msg}", file=sys.stderr)
    logger.info(msg)
