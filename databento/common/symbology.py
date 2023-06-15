import datetime as dt
from dataclasses import dataclass


ALL_SYMBOLS = "ALL_SYMBOLS"


@dataclass(frozen=True)
class InstrumentIdMappingInterval:
    """
    Represents an instrument ID to raw symbol mapping over a start and end date
    range interval.

    Parameters
    ----------
    start_date : dt.date
        The start of the mapping period.
    end_date : dt.date
        The end of the mapping period.
    raw_symbol : str
        The raw symbol value.
    instrument_id : int
        The instrument ID value.

    """

    start_date: dt.date
    end_date: dt.date
    raw_symbol: str
    instrument_id: int
