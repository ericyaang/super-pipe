import time
from datetime import datetime
from typing import Any, Dict, List

import requests
from prefect import flow, get_run_logger, task


@task(log_prints=True, retries=3, retry_delay_seconds=5)
def get_product_data(query, postal, country, delay=2):
    """Fetches data from Cornershop API.

    Args:
        query (str): The query parameter to search for.
        postal (str): The postal code.
        country (str): The country code.

    Returns:
        dict: A dictionary representing the JSON response.
    """

    base_url = "https://cornershopapp.com/api/v2/branches/search"
    params = {
        "query": query,
        "locality": postal,
        "country": country,
    }

    response = requests.get(base_url, params=params)

    if response.status_code != 200:
        raise Exception(f"Request failed with status {response.status_code}")

    time.sleep(delay)  # delay for `delay` seconds
    return response.json()


# Helper functions for create_data()
def get_nested(data: Dict, *args: str) -> Any:
    """
    Safely navigates a nested dictionary.
    """
    for arg in args:
        if data is None:
            return None
        data = data.get(arg)
    return data


def create_row(data: Dict, aisle: Dict, product: Dict) -> Dict:
    """
    Create a dictionary for a single row in the dataframe.
    """
    return {
        "date": datetime.today().strftime("%d-%m-%Y"),
        "aisle_name": aisle.get("aisle_name", None),
        "product_name": product.get("name", None),
        "brand": get_nested(product, "brand", "name"),
        "price": get_nested(product, "pricing", "price", "amount"),
        "package": product.get("package", None),
        "store_name": get_nested(data, "store", "name"),
        "store_city": get_nested(data, "store", "closest_branch", "city"),
        "search_term": get_nested(data, "search_result", "search_term"),
    }


@task()
def create_data(results: List[Dict]):
    """
    Create a ready dataframe format from the given results.
    """
    rows = [
        create_row(data, aisle, product)
        for data in results
        for aisle in get_nested(data, "search_result", "aisles")
        for product in aisle.get("products", [])
    ]
    return rows


@flow(name="Fetch-Corner-Shop-Data")
def fetch_corner_shop_data(
    item: str = "leite", postal: str = "88010560", country: str = "BR"
):
    logger = get_run_logger()
    logger.info("---Starting Data Fetch from CornerShop---")
    logger.info(f"Fetch using term {item} from postal code {postal}")
    raw_json_data = get_product_data(item, postal, country)
    json_data = raw_json_data.get("results", [])
    if not json_data:
        logger.info("---No data returned from the API---")
        return []
    data = create_data(json_data)
    logger.info(f"---Fetched a total of {len(data)} rows of data---")
    return data


if __name__ == "__main__":
    fetch_corner_shop_data()
