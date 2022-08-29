# databento-python

[![test](https://github.com/databento/databento-python/actions/workflows/test.yml/badge.svg?branch=dev)](https://github.com/databento/databento-python/actions/workflows/test.yml)
![python](https://img.shields.io/badge/python-3.7+-blue.svg)
![pypi-version](https://img.shields.io/pypi/v/databento)
![license](https://img.shields.io/github/license/databento/databento-python?color=blue)
[![code-style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

The official Python client library for [Databento](https://databento.com).

Key features include:
- Fast, lightweight access to both live and historical data from [multiple markets]().
- [Multiple schemas]() such as MBO, MBP, top of book, OHLCV, last sale, and more.
- [Fully normalized](), i.e. identical message schemas for both live and historical data, across multiple asset classes.
- Provides mappings between different symbology systems, including [smart symbology]() for futures rollovers.
- [Point-in-time]() instrument definitions, free of look-ahead bias and retroactive adjustments.
- Reads and stores market data in an extremely efficient file format using [Databento Binary Encoding]().
- Event-driven [market replay](), including at high-frequency order book granularity.
- Support for [batch download]() of flat files.
- Support for [pandas](), CSV, and JSON.

## Documentation
The best place to begin is with our [Getting started](https://docs.databento.com/getting-started?historical=python&live=python) guide.

You can find our full client API reference on the [Historical Reference](https://docs.databento.com/reference-historical?historical=python&live=python) and
[Live Reference](https://docs.databento.com/reference-live?historical=python&live=python) sections of our documentation. See also the
[Examples]() section for various tutorials and code samples.

## Requirements
The library is fully compatible with the latest distribution of Anaconda 3.7 and above.
The minimum dependencies as found in the `requirements.txt` are also listed below:
- Python (>=3.7)
- aiohttp (>=3.7.2)
- dbz-lib (>=0.1.1)
- numpy (>=1.17.0)
- pandas (>=1.1.3)
- requests (>=2.24.0)
- zstandard (>=0.18.0)

## Installation
To install the latest stable version of the package from PyPI:

    pip install -U databento

## Usage
The library needs to be configured with an API key from your account.
[Sign up](https://databento.com/signup) for free and you will automatically
receive a set of API keys to start with. Each API key is a 28-character
string that can be found on the API Keys page of your [Databento user portal](https://databento.com/platform/keys).

A simple Databento application looks like this:

```python
import databento as db

client = db.Historical('YOUR_API_KEY')
data = client.timeseries.stream(
    dataset='GLBX.MDP3',
    start='2020-11-02T14:30',
    end='2020-11-02T14:40')

data.replay(callback=print)    # market replay, with `print` as event handler
```

Replace `YOUR_API_KEY` with an actual API key, then run this program.

This uses `.replay()` to access the entire block of data
and dispatch each data event to an event handler. You can also use
`.to_df()` or `.to_ndarray()` to cast the data into a Pandas `DataFrame` or numpy `ndarray`:

```python
df = data.to_df(pretty_ts=True, pretty_px=True)  # to DataFrame, with pretty formatting
array = data.to_ndarray()                        # to ndarray
```

Note that the API key was also passed as a parameter, which is
[not recommended for production applications](https://docs0.databento.com/knowledge-base/new-users/securing-your-api-keys?historical=python&live=python).
Instead, you can leave out this parameter to pass your API key via the `DATABENTO_API_KEY` environment variable:

```python
import databento as db

client = db.Historical('YOUR_API_KEY')  # pass as parameter
client = db.Historical()                # pass as `DATABENTO_API_KEY` environment variable
```

## License
Distributed under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0.html).
