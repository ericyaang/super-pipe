from prefect import flow

@flow(log_prints=True)
def new():
    print(f"Passou no teste :)")


if __name__ == "__main__":
    new()