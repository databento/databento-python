import sys

from databento.version import __version__


PYTHON_VERSION = f"{sys.version_info.major}.{sys.version_info.minor}"
USER_AGENT = f"Databento/{__version__} Python/{PYTHON_VERSION}"
