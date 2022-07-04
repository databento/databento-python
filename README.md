# Library #

[![test](https://github.com/databento/databento-python/actions/workflows/test.yml/badge.svg?branch=dev)](https://github.com/databento/databento-python/actions/workflows/test.yml)
![python](https://img.shields.io/badge/python-3.7+-blue.svg)
![pypi-version](https://img.shields.io/pypi/v/databento)
[![code-style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Official Python client library for [Databento](https://databento.com).

Key features include:
- Fast, lightweight access to both live and historical data from [multiple markets]().
- [Multiple schemas]() such as MBO, MBP, top of book, OHLCV, last sale, and more.
- [Fully normalized](), i.e. identical message schemas for both live and historical data, and across multiple asset classes.
- Provides mappings between different symbology systems, including [smart symbology]() resolution to handle rollovers.
- [Point-in-time]() instrument definitions, free of look-ahead bias and retroactive adjustments.
- Reads and stores market data in an extremely efficient file format using [Databento Binary Encoding]().
- Event-driven [market replay](), including at high-frequency order book granularity.
- Support for [batch download]() of flat files.
- Support for [pandas](), CSV, and JSON.

## Documentation
The best place to begin is with our [Getting started](https://docs.databento.com/getting-started?historical=python&live=python) guide.

The full client API reference can be found on the [Historical Reference](https://docs.databento.com/reference-historical?historical=python&live=python) and
[Live Reference](https://docs.databento.com/reference-live?historical=python&live=python) sections of our documentation. See also the
[Examples]() section for various tutorials and code samples.

## Requirements
The library is fully compatible with the latest distribution of Anaconda 3.7 and above.
The minimum dependencies as found in the `requirements.txt` are also listed below:
- Python (>=3.7)
- aiohttp (>=3.7.2)
- numpy (>=1.17.0)
- pandas (>=1.1.3)
- requests (>=2.24.0)
- zstandard (>=0.18.0)

## Installation
To install the latest stable version of the package from PyPI:

    pip install -U databento

## Usage
The library needs to be configured with your account's access key, which can
be found on your [Databento user portal](https://databento.com/platform/keys).
[Sign up](https://app0.databento.com/signup) for free and you will
automatically receive a set of access keys to start with.

A simple Databento application looks like this:

```
import databento as db

client = db.Historical('YOUR_ACCESS_KEY')
bento = client.timeseries.stream(
    dataset='GLBX.MDP3',
    start='2020-11-02T14:30',
    end='2020-11-02T14:40')

bento.replay(callback=print)
```

Replace `YOUR_ACCESS_KEY` with your actual access key, then run this program.

Notice that you've used `.replay()` to access the entire block of data
and dispatch each data event to a callback function. You can also use
`.to_df()` or `.to_list()` to cast the data into a Pandas DataFrame or list
respectively.

The access key was also passed as a parameter, which is [not recommended for production applications](https://docs0.databento.com/knowledge-base/new-users/securing-your-access-keys?historical=python&live=python).
Instead, you can leave out this parameter to pass your access key via the `DATABENTO_ACCESS_KEY` environment variable:

```
import databento as db

client = db.Historical('YOUR_ACCESS_KEY')   # pass key via parameter
client = db.Historical()                    # pass key via `DATABENTO_ACCESS_KEY` environment variable
```

## License
Distributed under the [MIT License](https://mit-license.org/).
