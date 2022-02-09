# Databento Python Library #

[![test](https://github.com/databento/databento-python/actions/workflows/test.yml/badge.svg?branch=dev)](https://github.com/databento/databento-python/actions/workflows/test.yml)
![python](https://img.shields.io/badge/python-3.6+-blue.svg)
![pypi-version](https://img.shields.io/pypi/v/databento)
[![code-style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

The Databento Python client library provides access to the Databento API for
both live and historical data, from applications written in the Python language.

## Documentation
The best place to start is with the [Getting started guide](https://docs.databento.com/getting-started?historical=python&live=python).
See the [Python API docs](https://docs.databento.com/reference-historical?historical=python&live=python) for further details.

## Requirements
The library is fully compatible with the latest distribution of Anaconda 3.6 and above.
The minimum dependencies as found in the `requirements.txt` are also listed below:
- Python 3.6+
- aiohttp>=3.6.2
- numpy>=1.18.5
- pandas>=1.0.5
- requests>=2.24.0
- zstandard>=0.17.0

## Installation
To install the latest stable version of the package from PyPI:

    pip install -U databento

## API access setup
You need an access key to request for data through Databento. Sign up and you
will be automatically assigned your first pair of default access keys. Each
access key is a 32 character string that can be found from the web platform.

One of your default keys is a `test key`. Test keys have the prefix `db-test-`
and let you access our test environment, which provides free, simulated data for
validating your integration. All of our services can be tested with test keys.

Your other default key is a production key. Production keys have the prefix
`db-prod-` and are used to access real data. When you sign up, you are also
automatically enrolled in our Free Tier, which lets you try some real data for
free.

## License
The `databento` library is offered under the [MIT License](https://mit-license.org/).
