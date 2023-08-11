from prefect import flow

@flow(log_prints=True)
def test():
    print(f"Passou no teste :)")


if __name__ == "__main__":
    test()