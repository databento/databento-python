# databento-python

[![test](https://github.com/databento/databento-python/actions/workflows/test.yml/badge.svg?branch=dev)](https://github.com/databento/databento-python/actions/workflows/test.yml)
![python](https://img.shields.io/pypi/pyversions/databento.svg)
[![pypi-version](https://img.shields.io/pypi/v/databento)](https://pypi.org/project/databento)
[![license](https://img.shields.io/github/license/databento/databento-python?color=blue)](./LICENSE)
[![code-style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Slack](https://img.shields.io/badge/join_Slack-community-darkblue.svg?logo=slack)](https://to.dbn.to/slack)

The official Python client library for [Databento](https://databento.com).

Key features include:
- Fast, lightweight access to both live and historical data from [multiple markets](https://databento.com/docs/faqs/venues-and-publishers).
- [Multiple schemas](https://databento.com/docs/schemas-and-data-formats/whats-a-schema?historical=python&live=python) such as MBO, MBP, top of book, OHLCV, last sale, and more.
- [Fully normalized](https://databento.com/docs/standards-and-conventions/normalization?historical=python&live=python), i.e. identical message schemas for both live and historical data, across multiple asset classes.
- Provides mappings between different symbology systems, including [smart symbology](https://databento.com/docs/api-reference-historical/basics/symbology?historical=python&live=python) for futures rollovers.
- [Point-in-time]() instrument definitions, free of look-ahead bias and retroactive adjustments.
- Reads and stores market data in an extremely efficient file format using [Databento Binary Encoding](https://databento.com/docs/standards-and-conventions/databento-binary-encoding?historical=python&live=python).
- Event-driven [market replay](https://databento.com/docs/api-reference-historical/helpers/bento-replay?historical=python&live=python), including at high-frequency order book granularity.
- Support for [batch download](https://databento.com/docs/faqs/streaming-vs-batch-download?historical=python&live=python) of flat files.
- Support for [pandas](https://pandas.pydata.org/docs/), CSV, and JSON.

## Documentation
The best place to begin is with our [Getting started](https://databento.com/docs/quickstart?historical=python&live=python) guide.

You can find our full client API reference on the [Historical Reference](https://databento.com/docs/api-reference-historical?historical=python&live=python) and
[Live Reference](https://databento.com/docs/reference-live?historical=python&live=python) sections of our documentation. See also the
[Examples](https://databento.com/docs/examples?historical=python&live=python) section for various tutorials and code samples.

## Requirements
The library is fully compatible with distributions of Anaconda 2023.x and above.
The minimum dependencies as found in the `pyproject.toml` are also listed below:
- python = "^3.10"
- aiohttp = "^3.8.3"
- databento-dbn = "~0.44.0"
- numpy = ">=1.23.5"
- pandas = ">=1.5.3"
- pip-system-certs = ">=4.0" (Windows only)
- pyarrow = ">=13.0.0"
- requests = ">=2.25.1"
- zstandard = ">=0.21.0"

## Installation
To install the latest stable version of the package from PyPI:

    pip install -U databento

## Usage
The library needs to be configured with an API key from your account.
[Sign up](https://databento.com/signup) for free and you will automatically
receive a set of API keys to start with. Each API key is a 32-character
string starting with `db-`, that can be found on the API Keys page of your [Databento user portal](https://databento.com/platform/keys).

A simple Databento application looks like this:

```python
import databento as db

client = db.Historical('YOUR_API_KEY')
data = client.timeseries.get_range(
    dataset='GLBX.MDP3',
    symbols='ES.FUT',
    stype_in='parent',
    start='2022-06-10T14:30',
    end='2022-06-10T14:40',
)

data.replay(callback=print)  # market replay, with `print` as event handler
```

Replace `YOUR_API_KEY` with an actual API key, then run this program.

This uses `.replay()` to access the entire block of data
and dispatch each data event to an event handler. You can also use
`.to_df()` or `.to_ndarray()` to cast the data into a Pandas `DataFrame` or numpy `ndarray`:

```python
df = data.to_df()  # to DataFrame
array = data.to_ndarray()  # to ndarray
```

Note that the API key was also passed as a parameter, which is
[not recommended for production applications](https://databento.com/docs/portal/api-keys?historical=python&live=python).
Instead, you can leave out this parameter to pass your API key via the `DATABENTO_API_KEY` environment variable:

```python
import databento as db

# Pass as parameter
client = db.Historical('YOUR_API_KEY')

# Or, pass as `DATABENTO_API_KEY` environment variable
client = db.Historical()
```

## License
Distributed under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0.html).
