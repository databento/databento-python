import datetime

from databento import DBNStore


if __name__ == "__main__":
    ts_start = datetime.datetime.utcnow()

    # Can load from file path (if exists)
    data = DBNStore.from_file(path="my_data.dbn")

    print(data.to_df())
    print(datetime.datetime.utcnow() - ts_start)
