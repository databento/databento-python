from databento import Historical


if __name__ == "__main__":
    key = "YOUR_API_KEY"
    client = Historical(key=key)

    print(client.metadata.list_publishers())
    print(client.metadata.list_datasets())
    print(client.metadata.list_schemas(dataset="GLBX.MDP3"))
    print(client.metadata.list_fields(dataset="GLBX.MDP3"))
