import datetime as dt


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

    def __init__(
        self,
        start_date: dt.date,
        end_date: dt.date,
        native: str,
        product_id: int,
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.native = native
        self.product_id = product_id

    def __repr__(self):
        return (
            f"{type(self).__name__}("
            f"start_date={self.start_date}, "
            f"end_date={self.end_date}, "
            f"native='{self.native}', "
            f"product_id={self.product_id})"
        )
