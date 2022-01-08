#!/usr/bin/env python3
import os

import databento
from setuptools import find_packages, setup


here = os.path.abspath(os.path.dirname(__file__))
os.chdir(here)

with open("README.md") as f:
    readme = f.read()

with open("requirements.txt", "r") as f:
    install_requires = f.read().splitlines(keepends=False)

with open("requirements_dev.txt", "r") as f:
    tests_require = f.read().splitlines(keepends=False)


setup(
    name="databento",
    version=databento.__version__,
    description="Python client library for the Databento API",
    long_description=readme,
    long_description_content_type="text/x-rst",
    author="Databento",
    author_email="support@databento.com",
    url="https://github.com/databento/databento-python",
    license="MIT",
    keywords="databento financial data API",
    packages=find_packages(exclude=["tests"]),
    zip_safe=False,
    # https://docs.anaconda.com/anaconda/packages/py3.6_linux-64/
    install_requires=install_requires,
    tests_require=tests_require,
    python_requires=">=3.6.*",
    project_urls={
        "Bug Tracker": "https://github.com/databento/databento-python/issues",
        "Documentation": "https://docs.databento.com/",
        "Source Code": "https://github.com/databento/databento-python",
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
