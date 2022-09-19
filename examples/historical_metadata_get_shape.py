from typing import Tuple

import databento as db


if __name__ == "__main__":
    db.log = "debug"  # optional debug logging

    key = "YOUR_API_KEY"
    client = db.Historical(key=key)

    shape: Tuple[int, ...] = client.metadata.get_shape(
        dataset="GLBX.MDP3",
        symbols=["ESM2"],
        schema="mbo",
        start="2022-06-10T12:00",
        end="2022-06-10T14:00",
        encoding="csv",
    )

    print(shape)
