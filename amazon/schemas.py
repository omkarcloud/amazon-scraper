"""Marshmallow request schemas for every /amazon/* route.

Generic fields live in the shared top-level schema_fields.py; this module
adds the Amazon resolvers and per-route schemas. Every schema's load()
output is the kwargs dict its endpoint function takes.

ONE param per input (tripadvisor QueryOrLinkField convention, never a
sibling `url`/`id` pair): `product` takes an ASIN OR a product link,
`seller` an id OR a seller/storefront link, `influencer` a storefront
name OR its link, `post` a list id OR its link, `category` (browse) a
node id OR a category link, best-seller `category` an alias path OR a
list link, `query` a keyword OR a search link (refs.py).

`country` picks the marketplace (US default). When it is not given and a
ref param is a link on another marketplace (amazon.de/…), that
marketplace is used (MarketplaceSchema.pre_load).
"""
from marshmallow import ValidationError, missing, post_load, pre_load, validate

from amazon import refs, sites
from amazon.legacy import legacy_aliases
from amazon.products import CONDITIONS as OFFER_CONDITIONS
from amazon.rankings import DEAL_SORTS, LIST_TYPES
from amazon.search import CONDITIONS, DEAL_TYPES, SORTS
from amazon.sellers import FEEDBACK_FILTERS, FEEDBACK_PERIODS, FEEDBACK_SORTS
from schema_fields import (BaseSchema, ChoiceField, CommaListField, Flag, LanguageCodeField, LimitField, PageField,
                           PositiveInt, Price, QueryField, RefField, StrippedString)


# ---- fields ---------------------------------------------------------------------------------------------------

class CountryField(StrippedString):
    """Marketplace country (ISO code; UK accepted for GB). Default US."""

    def __init__(self, **kwargs):
        kwargs.setdefault("required", False)
        kwargs.setdefault("load_default", sites.DEFAULT_COUNTRY)
        super().__init__(**kwargs)

    def _deserialize(self, value, attr, data, **kwargs):
        value = super()._deserialize(value, attr, data, **kwargs)
        if value is None:
            return sites.DEFAULT_COUNTRY
        try:
            return sites.site(value)["country"]
        except ValueError:
            raise ValidationError(f"Must be one of: {', '.join(sites.COUNTRIES)}.")


class ProductRefField(RefField):
    resolver = staticmethod(refs.resolve_product)


class SellerRefField(RefField):
    resolver = staticmethod(refs.resolve_seller)


class InfluencerRefField(RefField):
    resolver = staticmethod(refs.resolve_influencer)


class PostRefField(RefField):
    resolver = staticmethod(refs.resolve_post)


class NodeRefField(RefField):
    resolver = staticmethod(refs.resolve_node)


class BestsellerCategoryField(RefField):
    resolver = staticmethod(refs.resolve_bestseller_category)


class SearchQueryField(RefField):
    resolver = staticmethod(refs.resolve_query)


class LinkField(RefField):
    resolver = staticmethod(refs.resolve_link)


class PostalCodeField(StrippedString):
    """Delivery postal code the page is rendered for (default: a code in the
    marketplace's own country, sites.py)."""

    def __init__(self, **kwargs):
        kwargs.setdefault("required", False)
        kwargs.setdefault("load_default", None)
        kwargs.setdefault("validate", validate.Length(min=3, max=12))
        super().__init__(**kwargs)


class AsinListField(CommaListField):
    """Comma-separated ASINs / product links (max 10)."""

    def __init__(self, **kwargs):
        kwargs.setdefault("max_items", 10)
        kwargs.pop("required", True)
        super().__init__(upper=False, **kwargs)
        # CommaListField defaults to optional; this one is mandatory.
        self.required = True
        self.load_default = missing

    def _deserialize(self, value, attr, data, **kwargs):
        items = super()._deserialize(value, attr, data, **kwargs)
        if not items:
            raise ValidationError("Must list at least one ASIN.")
        out = []
        for item in items:
            try:
                asin = refs.resolve_product(item)
            except ValueError as e:
                raise ValidationError(str(e))
            if asin not in out:
                out.append(asin)
        return out


_REF_KEYS = ("product", "products", "seller", "influencer", "post", "category", "query", "url")


class MarketplaceSchema(BaseSchema):
    """country + language, with the country inferred from a pasted link."""
    country = CountryField()
    language = LanguageCodeField()

    @pre_load
    def country_from_link(self, data, **kwargs):
        # retired amazon-scraper-api param names (country_code, sort_by, asin,
        # category_id) keep working; see amazon/legacy.py
        data = legacy_aliases(data, self.fields)
        if data.get("country"):
            return data
        for key in _REF_KEYS:
            value = data.get(key)
            if isinstance(value, str) and value:
                country = refs.country_of(value.split(",")[0])
                if country:
                    data["country"] = country
                    break
        return data


# ---- search --------------------------------------------------------------------------------------------------

class _SearchFilters(MarketplaceSchema):
    page = PageField(max_page=400)
    sort = ChoiceField(list(SORTS), load_default="relevance")
    min_price = Price()
    max_price = Price()
    brand = StrippedString(load_default=None, validate=validate.Length(max=100))
    condition = ChoiceField(list(CONDITIONS))
    is_prime = Flag()
    four_stars_and_up = Flag()
    deals = ChoiceField(list(DEAL_TYPES))

    @post_load
    def price_order(self, data, **kwargs):
        if data.get("min_price") is not None and data.get("max_price") is not None and data["min_price"] > data["max_price"]:
            raise ValidationError({"max_price": ["Must be greater than min_price."]})
        return data


class SearchSchema(_SearchFilters):
    query = SearchQueryField()
    category = StrippedString(load_default=None, validate=validate.Length(max=60))
    node = NodeRefField(required=False, load_default=None)
    seller = SellerRefField(required=False, load_default=None)


class CategoryProductsSchema(_SearchFilters):
    category = NodeRefField(data_key="category")

    @post_load
    def rename(self, data, **kwargs):
        data["node"] = data.pop("category")
        return data


class LookupSchema(MarketplaceSchema):
    identifier = StrippedString(required=True, validate=validate.Regexp(r"^[\w\- ]{6,40}$", error="Must be a UPC / EAN / GTIN / ISBN."))


class AutocompleteSchema(BaseSchema):
    query = QueryField(max_length=100)
    country = CountryField()
    category = StrippedString(load_default=None, validate=validate.Length(max=60))
    limit = LimitField(default=11, max_size=20)


class CategoriesSchema(MarketplaceSchema):
    pass


class CategoryTreeSchema(MarketplaceSchema):
    category = NodeRefField(data_key="category")

    @post_load
    def rename(self, data, **kwargs):
        data["node"] = data.pop("category")
        return data


# ---- products -----------------------------------------------------------------------------------------------

class ProductSchema(MarketplaceSchema):
    product = ProductRefField()


class ProductDetailsSchema(ProductSchema):
    postal_code = PostalCodeField()


class OffersSchema(ProductSchema):
    page = PageField(max_page=50)
    condition = ChoiceField(list(OFFER_CONDITIONS), load_default="all")
    prime_only = Flag()
    postal_code = PostalCodeField()


class BulkSchema(MarketplaceSchema):
    products = AsinListField(required=True)
    postal_code = PostalCodeField()


class ScrapeUrlSchema(BaseSchema):
    url = LinkField()
    language = LanguageCodeField()
    postal_code = PostalCodeField()


# ---- rankings / deals ------------------------------------------------------------------------------------

class BestsellersSchema(MarketplaceSchema):
    category = BestsellerCategoryField(required=False, load_default="")
    list_type = ChoiceField(list(LIST_TYPES), load_default="best_sellers")
    page = PageField(max_page=2)


class BestsellerCategoriesSchema(MarketplaceSchema):
    category = BestsellerCategoryField(required=False, load_default="")


class DealsSchema(MarketplaceSchema):
    collection = StrippedString(load_default=None, validate=validate.Length(max=80))
    department_id = StrippedString(load_default=None, validate=validate.Regexp(r"^\d{1,20}$", error="Must be a numeric department id (from `refinements`)."))
    brand = StrippedString(load_default=None, validate=validate.Length(max=100))
    min_discount = PositiveInt(max_value=99)
    max_discount = PositiveInt(max_value=100)
    min_rating = ChoiceField({"4": 4, "3": 3, "2": 2, "1": 1})
    prime_early_access = Flag()
    sort = ChoiceField(list(DEAL_SORTS), load_default="featured")
    page = PageField(max_page=50)
    include_prices = Flag(load_default=True)


# ---- sellers -------------------------------------------------------------------------------------------------

class SellerSchema(MarketplaceSchema):
    seller = SellerRefField()


class SellerFeedbackSchema(SellerSchema):
    page = PageField(max_page=1000)
    rating = ChoiceField(list(FEEDBACK_FILTERS), load_default="all")
    period = ChoiceField(list(FEEDBACK_PERIODS), load_default="lifetime")
    sort = ChoiceField(list(FEEDBACK_SORTS), load_default="recent")


class SellerProductsSchema(SellerSchema):
    page = PageField(max_page=400)
    sort = ChoiceField(list(SORTS), load_default="relevance")
    query = StrippedString(load_default=None, validate=validate.Length(max=200))


# ---- influencers -------------------------------------------------------------------------------------------

class InfluencerSchema(MarketplaceSchema):
    influencer = InfluencerRefField()


class InfluencerPostsSchema(InfluencerSchema):
    post_type = ChoiceField(["all", "list", "photo", "video", "post"], load_default="all")


class InfluencerPostSchema(MarketplaceSchema):
    post = PostRefField()
    influencer = InfluencerRefField(required=False, load_default=None)
