import os
from typing import Optional

from databento.historical.client import Historical
from databento.historical.load import from_disk


# Single source version from VERSION file
here = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

with open(os.path.join(here, "VERSION"), encoding="utf-8") as f:
    __version__ = f.read()


__all__ = ["Historical", "from_disk"]


# Set to either 'DEBUG' or 'INFO', controls console logging
log: Optional[str] = None
