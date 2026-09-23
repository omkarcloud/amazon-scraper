"""Use the scraper straight from Python — no server needed.

    python main.py

Every function returns the same JSON the API does; results are written to
output/*.json.
"""
import json
import os

from amazon.products import details
from amazon.rankings import bestsellers
from amazon.search import search

os.makedirs("output", exist_ok=True)


def save(name, data):
    path = os.path.join("output", name)
    with open(path, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"saved {path}")


if __name__ == "__main__":
    # an ASIN or any amazon.<tld> product link; country picks the marketplace
    save("product_B07QSFHT27.json", details("B07QSFHT27", country="US"))

    # sort, price, brand, Prime, rating and deal filters
    save("search_raspberry_pi.json", search("raspberry pi", country="US", sort="best_sellers"))

    # top 50 of a best sellers list (page=2 for 51-100)
    save("best_sellers_electronics.json", bestsellers("electronics", country="US"))
