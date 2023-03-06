from databento import Historical


if __name__ == "__main__":
    key = "YOUR_API_KEY"
    client = Historical(key=key)

    cost: float = client.metadata.get_cost(
        dataset="GLBX.MDP3",
        symbols="ESM2",
        schema="mbo",
        start="2022-06-10",
        end="2022-06-15",
    )

    print(cost)
