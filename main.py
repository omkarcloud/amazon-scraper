import os

from src import Amazon, create_db_connection, close_tunnel


if __name__ == "__main__":
    query = os.getenv("AMAZON_QUERY", "Macbook")
    country = os.getenv("AMAZON_COUNTRY", "US")

    store_to_db = os.getenv("STORE_TO_DB", "false").lower() == "true"

    products = Amazon.search(query=query, key=os.getenv("RAPIDAPI_KEY"), country=country)
    print(f"Fetched {len(products)} products for query '{query}' in {country}.")

    if store_to_db and products:
        connection = create_db_connection()
        try:
            rows = Amazon.fetch_and_store_search_results(
                query=query,
                connection=connection,
                key=os.getenv("RAPIDAPI_KEY"),
                country=country,
            )
            print(f"Stored {len(rows)} rows into database.")
        finally:
            connection.close()
            close_tunnel()
