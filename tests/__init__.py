import os
import pathlib
import sys


sys.path.append(os.path.join(os.path.dirname(__file__), "."))

TESTS_ROOT = pathlib.Path(__file__).absolute().parent
PROJECT_ROOT = pathlib.Path(__file__).absolute().parent.parent
