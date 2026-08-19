<p align="center">
  <img src="https://raw.githubusercontent.com/omkarcloud/botasaurus/master/images/mascot.png" alt="amazon scraper" />
</p>
<div align="center" style="margin-top: 0;">
  <h1>✨ Amazon Scraper 🤖</h1>
  <p><strong>Scrape Amazon product search, prices, ratings, reviews, and full product details — across 24 marketplaces. Clean JSON, no blocks, no proxies.</strong></p>
</div>
<em>
  <h5 align="center">(Programming Language - Python 3)</h5>
</em>
<p align="center">
  <a href="#">
    <img alt="amazon-scraper forks" src="https://img.shields.io/github/forks/omkarcloud/amazon-scraper?style=for-the-badge" />
  </a>
  <a href="#">
    <img alt="Repo stars" src="https://img.shields.io/github/stars/omkarcloud/amazon-scraper?style=for-the-badge&color=yellow" />
  </a>
</p>
<p align="center">
  <img src="https://views.whatilearened.today/views/github/omkarcloud/amazon-scraper.svg" width="80px" height="28px" alt="View" />
</p>

Amazon Scraper turns Amazon into clean JSON, pulled live — no blocks, no proxies to manage. Search products with sorting, browse any category, pull full product details, and fetch top reviews — all via one API.

Prices and ratings come back numeric — with the rating breakdown, an AI customer-feedback summary, images, videos, variants, and frequently-bought-together — so you can compare, monitor, and analyze them programmatically instead of scraping HTML yourself.

It works across **24 Amazon marketplaces** via `country_code` — `US`, `GB`, `DE`, `FR`, `JP`, `IN`, `CA`, `AU`, and 16 more — with results localized and priced in each marketplace's own currency.

- **Rated Excellent — 4.6 based on 25 reviews** on [Trustpilot](https://www.trustpilot.com/review/omkar.cloud). Our open source work is sponsored by [1000+ devs on GitHub](https://github.com/sponsors/omkarcloud).

[![Try the Amazon Scraper API in the live playground — free, no signup](https://img.shields.io/badge/%E2%96%B6%20Playground-Run%20a%20live%20request%2C%20free-brightgreen?style=for-the-badge)](https://www.omkar.cloud/tools/amazon-scraper-api/playground?utm_source=github&utm_medium=cpc&utm_content=badge)

[![Free Plan: 200 requests per month](https://img.shields.io/badge/Free%20tier-200%20requests%2Fmonth-blue?style=for-the-badge)](#pricing)

The same scraper is also available on **Apify** and **RapidAPI**:

[![Run on Apify](https://img.shields.io/badge/Run%20on-Apify-blue)](https://apify.com/omkar-cloud/amazon-scraper) [![Run on RapidAPI](https://img.shields.io/badge/Run%20on-RapidAPI-blue?logo=rapidapi)](https://rapidapi.com/Chetan11dev/api/amazon-scraper)

## Example: Amazon Product Data in One Request

One request to the product details API:

```
GET https://amazon-scraper-api.omkar.cloud/amazon/product-details?asin=B0FWD726XF
```

```json
{
  "asin": "B0FWD726XF",
  "product_name": "Apple 2025 MacBook Pro Laptop with Apple M5 chip ... 14.2-inch Liquid Retina XDR Display, 24GB Unified Memory, 1TB SSD; Space Black",
  "link": "https://www.amazon.com/dp/B0FWD726XF",
  "brand_info": "Visit the Apple Store",
  "current_price": 2049,
  "original_price": 2199,
  "currency": "USD",
  "availability": "In Stock",
  "condition": "Buy New",
  "number_of_offers": 6,
  "rating": 4.8,
  "reviews": 128,
  "detailed_rating": { "5": 78, "4": 12, "3": 4, "2": 1, "1": 5 },
  "is_amazon_choice": true,
  "is_prime": true,
  "sales_volume": "200+ bought in past month",
  "main_category": "Laptops",
  "key_features": [
    "SUPERCHARGED BY M5",
    "BUILT FOR APPLE INTELLIGENCE",
    "UP TO 24 HOURS OF BATTERY LIFE"
  ],
  "customer_feedback_summary": "Customers praise the blazing speed, stunning display, and all-day battery, and call it a major leap over Intel Macs.",
  "main_image_url": "https://m.media-amazon.com/images/I/71an9eiBxpL._AC_SL1500_.jpg"
}
```

*Trimmed for readability — the full response has 50+ fields including the full description, technical details, category hierarchy, all images and product videos, variants, A+ content, brand story, top reviews, and frequently-bought-together. See the [sample response](#product-details) in the API reference.*

Add `country_code` to pull the product from any of 24 marketplaces — priced in that marketplace's currency.

**[Run this exact request in the Playground — no signup, no key →](https://www.omkar.cloud/tools/amazon-scraper-api/playground?utm_source=github&utm_medium=cpc&utm_content=example)**

The playground comes prefilled with this request and runs it against the live API in your browser. The JSON it returns is identical to what the API returns.

## Start Getting Data in Minutes

Python and Node.js integration examples are available for every endpoint in the playground, so you can get Amazon data in minutes instead of days.

```python
import requests

# Scrape Amazon product search results, live
response = requests.get(
    "https://amazon-scraper-api.omkar.cloud/amazon/search",
    params={"query": "iPhone 16", "sort_by": "reviews", "country_code": "US"},
    headers={"API-Key": "YOUR_API_KEY"},
)

print(response.json())
```

## API Reference

All endpoints are GET requests against `https://amazon-scraper-api.omkar.cloud`, authenticated with the `API-Key` header, returning JSON.

Add `country_code` to any request to pull from one of [24 marketplaces](#supported-marketplaces) — results are localized and priced in that marketplace's currency.

### Product Search

▶ [Try it live in the Playground →](https://www.omkar.cloud/tools/amazon-scraper-api/playground?utm_source=github&utm_medium=cpc&utm_content=endpoint-search)

```
GET https://amazon-scraper-api.omkar.cloud/amazon/search?query=iPhone+16
```

Real-time Amazon product search with sorting. Returns 16 products per page with `next`/`previous` pagination links.

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `query` | Yes | — | Keyword or phrase to search for (e.g. `iPhone 16`). |
| `page` | No | `1` | Page number, 16 products per page. |
| `country_code` | No | `US` | Amazon marketplace — localizes results and currency. |
| `sort_by` | No | `relevance` | `relevance`, `lowest_price`, `highest_price`, `reviews`, `newest`, `best_sellers`. |

<details>
<summary>Sample Response (click to expand)</summary>

```json
{
  "count": 143,
  "per_page": 16,
  "current_page": 1,
  "total_pages": 9,
  "next": "https://amazon-scraper-api.omkar.cloud/amazon/search?query=iPhone+16&country_code=US&sort_by=relevance&page=2",
  "previous": null,
  "results": [
    {
      "title": "Apple iPhone 16, 128GB, Black - Unlocked (Renewed)",
      "price": 555,
      "original_price": null,
      "rating": 4,
      "reviews": 2504,
      "asin": "B0DHJH2GZL",
      "link": "https://www.amazon.com/dp/B0DHJH2GZL",
      "image_url": "https://m.media-amazon.com/images/I/71ShAbeIRdL._AC_UY654_QL65_.jpg",
      "currency": "USD",
      "is_best_seller": false,
      "is_amazon_choice": false,
      "is_prime": false,
      "delivery_info": "FREE delivery Fri, Aug 21. Only 4 left in stock - order soon.",
      "number_of_offers": 31,
      "lowest_offer_price": 516.72,
      "has_variations": true,
      "sales_volume": "1K+ bought in past month",
      "is_climate_friendly": false
    }
  ]
}
```

</details>

---

### Product Details

▶ [Try it live in the Playground →](https://www.omkar.cloud/tools/amazon-scraper-api/playground?utm_source=github&utm_medium=cpc&utm_content=endpoint-product)

```
GET https://amazon-scraper-api.omkar.cloud/amazon/product-details?asin=B0FWD726XF
```

Real-time full product details by ASIN — 50+ fields.

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `asin` | Yes | — | Amazon product ASIN (e.g. `B0FWD726XF`). |
| `country_code` | No | `US` | Amazon marketplace — prices come back in its currency. |

Returns pricing (current, original, unit), availability and condition, number of offers, delivery estimate, rating with the 1–5 breakdown, an AI customer-feedback summary, top reviews, all images and product videos, key features, full description, technical details, category hierarchy, variants, A+ content, brand story, and frequently-bought-together.

<details>
<summary>Sample Response (click to expand)</summary>

```json
{
  "asin": "B0FWD726XF",
  "product_name": "Apple 2025 MacBook Pro Laptop with Apple M5 chip ... 14.2-inch Liquid Retina XDR Display, 24GB Unified Memory, 1TB SSD; Space Black",
  "link": "https://www.amazon.com/dp/B0FWD726XF",
  "parent_asin": "B0FWCXXXXX",
  "brand_info": "Visit the Apple Store",
  "current_price": 2049,
  "original_price": 2199,
  "currency": "USD",
  "min_order_quantity": 1,
  "country": "US",
  "availability": "In Stock",
  "condition": "Buy New",
  "number_of_offers": 6,
  "delivery_info": "FREE delivery Wed, Aug 20",
  "rating": 4.8,
  "reviews": 128,
  "detailed_rating": { "5": 78, "4": 12, "3": 4, "2": 1, "1": 5 },
  "customer_feedback_summary": "Customers praise the blazing speed, stunning display, and all-day battery, and call it a major leap over Intel Macs.",
  "is_bestseller": false,
  "is_amazon_choice": true,
  "is_prime": true,
  "sales_volume": "200+ bought in past month",
  "main_image_url": "https://m.media-amazon.com/images/I/71an9eiBxpL._AC_SL1500_.jpg",
  "additional_image_urls": ["https://m.media-amazon.com/images/I/61X1Xl3G7lL._AC_SL1500_.jpg"],
  "has_video": true,
  "key_features": ["SUPERCHARGED BY M5", "BUILT FOR APPLE INTELLIGENCE", "UP TO 24 HOURS OF BATTERY LIFE"],
  "main_category": "Laptops",
  "category_hierarchy": ["Electronics", "Computers & Accessories", "Laptops", "Traditional Laptops"],
  "variation_dimensions": ["Color", "Size"],
  "variants": [
    { "asin": "B0FWD726XF", "name": "Space Black · 24GB · 1TB", "price": 2049 }
  ],
  "has_aplus_content": true,
  "frequently_bought_together": [
    { "asin": "B0CHX3QBCH", "title": "Apple USB-C to MagSafe 3 Cable", "price": 49 }
  ]
}
```

</details>

---

### Products By Category

▶ [Try it live in the Playground →](https://www.omkar.cloud/tools/amazon-scraper-api/playground?utm_source=github&utm_medium=cpc&utm_content=endpoint-category)

```
GET https://amazon-scraper-api.omkar.cloud/amazon/products/category?category_id=172282
```

Browse every product in an Amazon category, 24 per page, with `next`/`previous` links. Each product carries the same fields as a search result.

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `category_id` | Yes | — | Amazon category / browse-node ID (e.g. `172282`). |
| `page` | No | `1` | Page number, 24 products per page. |
| `country_code` | No | `US` | Amazon marketplace — localizes results and currency. |
| `sort_by` | No | `relevance` | `relevance`, `lowest_price`, `highest_price`, `reviews`, `newest`, `best_sellers`. |

<details>
<summary>Sample Response (click to expand)</summary>

```json
{
  "count": 77822,
  "per_page": 24,
  "current_page": 1,
  "total_pages": 3243,
  "next": "https://amazon-scraper-api.omkar.cloud/amazon/products/category?category_id=172282&country_code=US&sort_by=relevance&page=2",
  "previous": null,
  "results": [
    {
      "title": "Sony WH-1000XM5 Wireless Noise Canceling Headphones",
      "price": 328,
      "original_price": 399.99,
      "rating": 4.6,
      "reviews": 18542,
      "asin": "B09XS7JWHH",
      "link": "https://www.amazon.com/dp/B09XS7JWHH",
      "image_url": "https://m.media-amazon.com/images/I/61+btxzpfDL._AC_UY654_QL65_.jpg",
      "currency": "USD",
      "is_best_seller": true,
      "is_amazon_choice": false,
      "is_prime": true,
      "delivery_info": "FREE delivery Thu, Aug 20",
      "number_of_offers": 12,
      "lowest_offer_price": 319.99,
      "has_variations": true,
      "sales_volume": "5K+ bought in past month",
      "is_climate_friendly": false
    }
  ]
}
```

</details>

---

### Top Product Reviews

▶ [Try it live in the Playground →](https://www.omkar.cloud/tools/amazon-scraper-api/playground?utm_source=github&utm_medium=cpc&utm_content=endpoint-reviews)

```
GET https://amazon-scraper-api.omkar.cloud/amazon/product-reviews/top?asin=B0FWD726XF
```

The top reviews for a product by ASIN — full text, title, and review ID.

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `asin` | Yes | — | Amazon product ASIN. |
| `country_code` | No | `US` | Amazon marketplace. |

<details>
<summary>Sample Response (click to expand)</summary>

```json
{
  "results": [
    {
      "review_id": "R1LD6F7278XWSV",
      "product_asin": "B0FWD726XF",
      "review_title": "Incredible (from a PC guy)",
      "review_text": "I've been a Windows and Linux guy for decades and actively avoided Macs for years... The new Apple Silicon M5 changes all of that. This MacBook Pro is absurdly fast. Everything feels instant..."
    }
  ]
}
```

</details>

---

### Supported Marketplaces

Add `country_code` to any request to pull from that marketplace, localized and priced in its currency. 24 marketplaces are supported:

`US`, `GB`, `CA`, `AU`, `IE`, `DE`, `FR`, `IT`, `ES`, `NL`, `BE`, `SE`, `PL`, `TR`, `JP`, `CN`, `SG`, `IN`, `AE`, `SA`, `EG`, `ZA`, `BR`, `MX`

## Pricing

| Plan | Price | Requests/Month |
|------|-------|----------------|
| Free | $0 | 200 |
| Starter | $16 | 20,000 |
| Grow | $48 | 100,000 |
| Scale | $148 | 400,000 |

1 API call = 1 request

Free Plan Available — [create your API key →](https://www.omkar.cloud/auth/sign-up?redirect=/api-key&utm_source=github&utm_medium=cpc&utm_content=pricing-signup). No credit card for the free tier.

## FAQs

### Can I try the API before signing up?

Yes. The playground runs live requests in your browser — free, no account, no API key. [Try it in the Playground →](https://www.omkar.cloud/tools/amazon-scraper-api/playground?utm_source=github&utm_medium=cpc&utm_content=faq)

### How do I search Amazon products?

Call Product Search (`GET /amazon/search?query=iPhone+16`) with a keyword. It returns 16 products per page — title, price, rating, review count, ASIN, image, and best-seller/Prime flags — plus a `next` link to walk through every page. Sort with `sort_by` (`lowest_price`, `reviews`, `best_sellers`, and more).

### What's in the full product details?

Product Details (by ASIN) returns 50+ fields: current/original/unit pricing, availability and condition, the 1–5 rating breakdown, an AI customer-feedback summary, top reviews, all images and product videos, key features, full description, technical details, category hierarchy, variants, A+ content, brand story, and frequently-bought-together.

### Does it work on non-US Amazon sites, in local currency?

Yes. Pass `country_code` — 24 marketplaces are supported (`GB`, `DE`, `FR`, `JP`, `IN`, `CA`, `AU`, and more). Results are localized and prices come back in that marketplace's currency.

### How fresh is the data?

Data is scraped from Amazon in real time. Every API call fetches live data — not cached or stale results. Prices, ratings, stock, and reviews reflect what's on Amazon right now.

### Will I get blocked or need proxies?

No. We handle the scraping infrastructure — you call a normal REST API and never touch Amazon directly, so there are no proxies, headless browsers, or CAPTCHAs on your side.

### How do I get a product's ASIN or a category ID?

ASINs come back on every search and category result (the `asin` field), or take it from any Amazon product URL (`/dp/B0FWD726XF` → `B0FWD726XF`). Category IDs are Amazon browse-node IDs, found in category URLs.

## More E-commerce & Data Scrapers: AliExpress & Google Maps

- **[AliExpress Scraper API](https://github.com/omkarcloud/aliexpress-scraper)** — the same clean JSON for AliExpress: product search and full product details with per-SKU pricing, stock, and shipping across 64 ship-to countries. Compare the same product's price on Amazon and AliExpress.

- **[Google Maps Scraper (3,100+ GitHub Stars)](https://github.com/omkarcloud/google-maps-scraper)** — need tens of thousands of leads? Type a niche and a city ("dentists in New York") and get every matching business as a ready-to-call lead list — name, address, phone, website, emails, rating, and reviews. The free tier alone pulls up to 100K leads a month.

- **[Website Email Contact Scraper](https://github.com/omkarcloud/website-email-contact-scraper)** — **Free and open source.** Point it at any website and get every email, phone number, and social profile on it, each with source pages and an official/unofficial flag.

## Support

Built by developers, for developers — when you reach out, you talk to the engineers who built the API, not a support script. Message us anytime and we'll solve your query within 1 working day.


[![Contact Us on WhatsApp about Amazon Scraper](https://raw.githubusercontent.com/omkarcloud/assets/master/images/whatsapp-us.png)](https://api.whatsapp.com/send?phone=918178804274&text=I%20have%20a%20question%20about%20the%20Amazon%20Scraper%20API.)

Email: [happy.to.help@omkar.cloud](mailto:happy.to.help@omkar.cloud?subject=Amazon%20Scraper%20API%20Question)

[![Email Us about Amazon Scraper](https://raw.githubusercontent.com/omkarcloud/assets/master/images/ask-on-email.png)](mailto:happy.to.help@omkar.cloud?subject=Amazon%20Scraper%20API%20Question)

## Love It? Star It! ⭐

From one developer to another: If the Amazon Scraper API saved you time, please [star the repo](https://github.com/omkarcloud/amazon-scraper).

Here's why it matters: most developers judge a scraper by its stars before trying it. Your star helps the next developer — someone deciding whether the Amazon data here is real and reliable — try it with confidence.

It takes only 1 second, and means the world to me.
