"""Cache TTL per /amazon/* endpoint (cache.py, keyed on the validated
params — marshmallow fills the defaults, so `?page=1` and no `page` share a
row, and the id-form / link-form of a RefField share a row too).

Tiers follow how fast each surface moves: prices, stock and offers change
within the hour; search rankings, best sellers and deals daily-ish;
seller / influencer profiles slowly; reference lists (search aliases,
best-seller category trees) almost never."""
from datetime import timedelta

# --- search / discovery ------------------------------------------------------
SEARCH_CACHE = timedelta(hours=1)
CATEGORY_PRODUCTS_CACHE = timedelta(hours=1)
LOOKUP_CACHE = timedelta(hours=6)
AUTOCOMPLETE_CACHE = timedelta(days=1)
CATEGORIES_CACHE = timedelta(days=7)
CATEGORY_TREE_CACHE = timedelta(days=3)

# --- products ----------------------------------------------------------------
PRODUCT_CACHE = timedelta(hours=1)
OFFERS_CACHE = timedelta(minutes=30)
VARIATIONS_CACHE = timedelta(hours=6)
REVIEWS_CACHE = timedelta(hours=3)
BULK_CACHE = timedelta(hours=1)
SCRAPE_URL_CACHE = timedelta(hours=1)

# --- rankings / deals ---------------------------------------------------------
BESTSELLERS_CACHE = timedelta(hours=3)
BESTSELLER_CATEGORIES_CACHE = timedelta(days=7)
DEALS_CACHE = timedelta(minutes=30)

# --- sellers / influencers ------------------------------------------------------
SELLER_CACHE = timedelta(hours=12)
SELLER_FEEDBACK_CACHE = timedelta(hours=6)
SELLER_PRODUCTS_CACHE = timedelta(hours=1)
INFLUENCER_CACHE = timedelta(hours=12)
INFLUENCER_POSTS_CACHE = timedelta(hours=6)
