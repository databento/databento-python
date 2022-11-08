import datetime as dt
from dataclasses import dataclass


@dataclass(frozen=True)
class ProductIdMappingInterval:
    """
    Represents a product ID to native symbol mapping over a start and end date
    range interval.

    Parameters
    ----------
    start_date : dt.date
        The start of the mapping period.
    end_date : dt.date
        The end of the mapping period.
    native : str
        The native symbol value.
    product_id : int
        The product ID value.
    """

    start_date: dt.date
    end_date: dt.date
    native: str
    product_id: int
