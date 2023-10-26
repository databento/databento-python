from __future__ import annotations

import logging


def enable_logging(level: int | str = logging.INFO) -> None:
    """
    Enable logging for the Databento module. This function should be used for
    simple applications and examples. It is advisable to configure your own
    logging for serious applications.

    Parameters
    ----------
    level : str or int, default 'INFO'
        The log level to configure.

    See Also
    --------
    logging

    """
    # Create a basic formatter
    formatter = logging.Formatter(
        fmt=logging.BASIC_FORMAT,
    )

    # Construct a stream handler for stderr
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    handler.setLevel(level=level)

    # Add the handler to the databento logger
    databento_logger = logging.getLogger("databento")
    databento_logger.setLevel(level=level)
    databento_logger.addHandler(handler)
