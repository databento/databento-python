import platform
import re
from typing import Final

from databento.version import __version__


TOKEN_PATTERN: Final = re.compile(r"[^a-zA-Z0-9\.]")

PLATFORM_NAME: Final = TOKEN_PATTERN.sub("-", platform.system())
PLATFORM_VERSION: Final = TOKEN_PATTERN.sub("-", platform.release())
PYTHON_VERSION: Final = TOKEN_PATTERN.sub("-", platform.python_version())

USER_AGENT: Final = (
    f"Databento/{__version__} Python/{PYTHON_VERSION} {PLATFORM_NAME}/{PLATFORM_VERSION}"
)
