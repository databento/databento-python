import databento as db


if __name__ == "__main__":
    db.log = "debug"  # Optional debug logging

    key = "YOUR_API_KEY"
    client = db.Historical(key=key)

    print(client.metadata.list_publishers())
    print(client.metadata.list_datasets())
    print(client.metadata.list_schemas(dataset="GLBX.MDP3"))
    print(client.metadata.list_fields(dataset="GLBX.MDP3"))
    print(client.metadata.list_encodings())
    print(client.metadata.list_compressions())
