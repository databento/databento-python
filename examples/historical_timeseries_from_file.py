import datetime

import databento as db
from databento import Bento


if __name__ == "__main__":
    db.log = "debug"  # optional debug logging

    # Can load from file path (if exists)
    ts_start = datetime.datetime.utcnow()
    data = Bento.from_file(path="test_data.mbo.dbz")

    print(data.to_df())
    print(datetime.datetime.utcnow() - ts_start)
