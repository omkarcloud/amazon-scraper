![Amazon Scraper Featured Image](https://raw.githubusercontent.com/omkarcloud/amazon-scraper/master/amazon-scraper-featured-image.png)

# Amazon Scraper API

Scrape Amazon products, prices, reviews, and categories from 24 marketplaces via a simple REST API. 5,000 free requests/month.

## Key Features

- Search Amazon products, get product details, browse by category, and fetch top reviews — all via 1 API.
- 5,000 free queries per month. No credit card required.

Here's a sample response for a **product search results page**:
```json
{
  "title": "Apple iPhone 15, 128GB, Black - Unlocked (Renewed)",
  "price": 403.0,
  "rating": 4.1,
  "reviews": 2769,
  "asin": "B0CMPMY9ZZ",
  "link": "https://www.amazon.com/dp/B0CMPMY9ZZ",
  "image_url": "https://m.media-amazon.com/images/I/51PtFHUPjBL._AC_UY654_FMwebp_QL65_.jpg",
  "currency": "USD",
  "is_best_seller": false,
  "is_amazon_choice": false,
  "is_prime": false,
  "sales_volume": "2K+ bought in past month"
}
```

## Get API Key

Create an account at [omkar.cloud](https://www.omkar.cloud/auth/sign-up?redirect=/api-key) to get your API key.

It takes just 2 minutes to sign up. You get 5,000 free requests every month for detailed Amazon data than enough for most users to get their job done without paying a dime.

This is a well built product, and your search for the best Amazon Scraper API ends right here. 


## Quick Start

```bash
curl -X GET "https://amazon-scraper-api.omkar.cloud/amazon/search?query=iPhone%2016" \
  -H "API-Key: YOUR_API_KEY"
```

```json
{
  "results": [
    {
      "title": "Apple iPhone 15, 128GB, Black - Unlocked (Renewed)",
      "price": 403.0,
      "rating": 4.1,
      "reviews": 2769,
      "asin": "B0CMPMY9ZZ",
      "link": "https://www.amazon.com/dp/B0CMPMY9ZZ",
      "currency": "USD",
      "is_best_seller": false,
      "is_amazon_choice": false,
      "is_prime": false,
      "sales_volume": "2K+ bought in past month"
    }
  ]
}
```

## Quick Start (Python)

```bash
pip install requests
```

```python
import requests

# Search for products
response = requests.get(
    "https://amazon-scraper-api.omkar.cloud/amazon/search",
    params={"query": "iPhone 16", "country_code": "US"},
    headers={"API-Key": "YOUR_API_KEY"}
)

print(response.json())
```


## API Reference

### Product Search

```
GET https://amazon-scraper-api.omkar.cloud/amazon/search
```

#### Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `query` | Yes | — | Search query. Keywords or ASIN. |
| `page` | No | `1` | Page number. |
| `country_code` | No | `US` | Amazon marketplace code. |
| `sort_by` | No | `relevance` | `relevance`, `lowest_price`, `highest_price`, `reviews`, `newest`, `best_sellers` |

#### Example

```python
import requests

response = requests.get(
    "https://amazon-scraper-api.omkar.cloud/amazon/search",
    params={"query": "iPhone 16", "country_code": "US"},
    headers={"API-Key": "YOUR_API_KEY"}
)

print(response.json())
```

#### Response

<details>
<summary>Sample Response (click to expand)</summary>

```json
{
  "results": [
    {
      "title": "Apple iPhone 15, 128GB, Black - Unlocked (Renewed)",
      "price": 403.0,
      "original_price": null,
      "rating": 4.1,
      "reviews": 2769,
      "asin": "B0CMPMY9ZZ",
      "link": "https://www.amazon.com/dp/B0CMPMY9ZZ",
      "image_url": "https://m.media-amazon.com/images/I/51PtFHUPjBL._AC_UY654_FMwebp_QL65_.jpg",
      "currency": "USD",
      "is_best_seller": false,
      "is_amazon_choice": false,
      "is_prime": false,
      "delivery_info": "FREE delivery Fri, Feb 13",
      "number_of_offers": 62,
      "lowest_offer_price": 359.99,
      "has_variations": true,
      "sales_volume": "2K+ bought in past month",
      "is_climate_friendly": false
    }
  ]
}
```

</details>

---

### Products by Category

```
GET https://amazon-scraper-api.omkar.cloud/amazon/products/category
```

#### Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `category_id` | Yes | — | Amazon category ID (from URL `node=` param). |
| `page` | No | `1` | Page number. |
| `country_code` | No | `US` | Amazon marketplace code. |
| `sort_by` | No | `relevance` | `relevance`, `lowest_price`, `highest_price`, `reviews`, `newest`, `best_sellers` |

#### Example

```python
import requests

response = requests.get(
    "https://amazon-scraper-api.omkar.cloud/amazon/products/category",
    params={"category_id": "16225007011", "country_code": "US"},
    headers={"API-Key": "YOUR_API_KEY"}
)

print(response.json())
```

#### Response

<details>
<summary>Sample Response (click to expand)</summary>

```json
{
  "results": [
    {
      "title": "Apple iPhone 15, 128GB, Black - Unlocked (Renewed)",
      "price": 403.0,
      "original_price": null,
      "rating": 4.1,
      "reviews": 2769,
      "asin": "B0CMPMY9ZZ",
      "link": "https://www.amazon.com/dp/B0CMPMY9ZZ",
      "image_url": "https://m.media-amazon.com/images/I/51PtFHUPjBL._AC_UY654_FMwebp_QL65_.jpg",
      "currency": "USD",
      "is_best_seller": false,
      "is_amazon_choice": false,
      "is_prime": false,
      "delivery_info": "FREE delivery Fri, Feb 13",
      "number_of_offers": 62,
      "lowest_offer_price": 359.99,
      "has_variations": true,
      "sales_volume": "2K+ bought in past month",
      "is_climate_friendly": false
    }
  ]
}
```

</details>

---

### Product Details

```
GET https://amazon-scraper-api.omkar.cloud/amazon/product-details
```

#### Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `asin` | Yes | — | Amazon ASIN (e.g., `B0FWD726XF`). Also accepts product URLs. |
| `country_code` | No | `US` | Amazon marketplace code. |

#### Example

```python
import requests

response = requests.get(
    "https://amazon-scraper-api.omkar.cloud/amazon/product-details",
    params={"asin": "B0FWD726XF", "country_code": "US"},
    headers={"API-Key": "YOUR_API_KEY"}
)

print(response.json())
```

#### Response Fields

Returns 50+ fields including price, rating, description, key features, technical specs, all images/videos, rating distribution, product variants, category hierarchy, brand info, frequently bought together, and top reviews.

<details>
<summary>Sample Response (click to expand)</summary>

```json

{
  "asin": "B0FWD726XF",
  "product_name": "Apple 2025 MacBook Pro Laptop with M5 chip with 10‑core CPU and GPU: Built for Apple Intelligence, 14.2-inch Liquid Retina XDR Display, 24GB Unified Memory, 1TB SSD Storage, Space Black",
  "link": "https://www.amazon.com/dp/B0FWD726XF",
  "slug": "Apple-2025-MacBook-Laptop-10%E2%80%91core",
  "parent_asin": "B0DLRGKGTV",
  "landing_asin": "B0FWD726XF",
  "brand_info": "Visit the Apple Store",
  "brand_url": "https://www.amazon.com/stores/Apple/page/77D9E1F7-0337-4282-9DB6-B6B8FB2DC98D?lp_asin=B0FWD726XF&ref_=ast_bln",
  "brand_urls": [
    "https://www.amazon.com/stores/Apple/page/77D9E1F7-0337-4282-9DB6-B6B8FB2DC98D?lp_asin=B0FWD726XF&ref_=ast_bln"
  ],
  "current_price": 1849.0,
  "original_price": 1999.0,
  "unit_price": null,
  "unit_count": null,
  "currency": "USD",
  "min_order_quantity": null,
  "country": "US",
  "availability": "In Stock",
  "condition": "Buy new:",
  "number_of_offers": 6,
  "delivery_info": "FREE delivery Monday, February 16 Or Prime members get FREE delivery Tomorrow, February 12. Join Prime",
  "estimated_delivery_date": "Monday, February 16",
  "rating": 4.7,
  "reviews": 1167,
  "detailed_rating": {
    "1": 3,
    "2": 0,
    "3": 3,
    "4": 5,
    "5": 89
  },
  "customer_feedback_summary": null,
  "top_reviews": [
    {
      "review_id": "R1H9MVROAP2XKG",
      "product_asin": "B0FWD726XF",
      "review_title": "Sleek, powerful, and a photographers dream. 11/10",
      "review_text": "Okay so I actually had to return this item with amazon because they messed up delivery...",
      "review_link": "https://www.amazon.com/gp/customer-reviews/R1H9MVROAP2XKG",
      "rating": 5,
      "review_date": "Reviewed in the United States on December 1, 2025",
      "is_verified_purchase": true,
      "helpful_votes": 32,
      "reviewer_name": "Catherine Mason",
      "reviewer_id": "AF77JTSDDJJG6FXTYJAQOG4B4QFA",
      "reviewer_url": "https://www.amazon.com/gp/profile/amzn1.account.AF77JTSDDJJG6FXTYJAQOG4B4QFA",
      "reviewer_avatar": "https://m.media-amazon.com/images/S/amazon-avatars-global/default.png",
      "review_images": [],
      "review_video": null,
      "reviewed_variant": {
        "Style": "Apple M5 chip",
        "Capacity": "16GB Unified Memory, 512GB SSD Storage",
        "Color": "Space Black",
        "Set": "Without AppleCare+"
      },
      "is_vine_review": false
    },
    ...
  ],
  "is_bestseller": false,
  "is_amazon_choice": true,
  "is_prime": true,
  "is_climate_friendly": true,
  "sales_volume": "1K+ bought in past month",
  "main_image_url": "https://m.media-amazon.com/images/I/6112T6g2P-L._AC_SL1500_.jpg",
  "additional_image_urls": [
    "https://m.media-amazon.com/images/I/6112T6g2P-L._AC_SL1500_.jpg",
    "https://m.media-amazon.com/images/I/61SBJYmPyFL._AC_SL1500_.jpg",
    "https://m.media-amazon.com/images/I/81uUolI6viL._AC_SL1500_.jpg",
    "https://m.media-amazon.com/images/I/616eNe+uHRL._AC_.jpg",
    "https://m.media-amazon.com/images/I/81cfIW5+p7L._AC_SL1500_.jpg",
    "https://m.media-amazon.com/images/I/71c2cMlWe5L._AC_SL1500_.jpg",
    "https://m.media-amazon.com/images/I/61EQmd9L4kL._AC_SL1500_.jpg"
  ],
  "product_videos": [
    {
      "id": "amzn1.vse.video.0882c1076e654eab890412db954d0944",
      "title": "MacBook Pro M5 Chip",
      "url": "https://m.media-amazon.com/images/S/vse-vms-transcoding-artifact-us-east-1-prod/a60f6616-cd5d-4a84-a49a-b0ca7c22effb/default.jobtemplate.hls.m3u8",
      "height": 1080,
      "width": 1920,
      "thumbnail": "https://m.media-amazon.com/images/I/9103IPwwY2L._SX35_SY46._CR0,0,35,46_BG85,85,85_BR-120_PKdp-play-icon-overlay__.png",
      "product_id": "B0FWD726XF",
      "parent_id": "B0DLRGKGTV"
    },
    {
      "id": "amzn1.vse.video.007d9729d15a4dc7b533d61249583a76",
      "title": "AppleCare+ for Mac",
      "url": "https://m.media-amazon.com/images/S/vse-vms-transcoding-artifact-us-east-1-prod/3f731513-0ce4-4f42-ba76-e97deb21bcc7/default.jobtemplate.hls.m3u8",
      "height": 1080,
      "width": 1920,
      "thumbnail": "https://m.media-amazon.com/images/I/71N+KnY1REL._SX35_SY46._CR0,0,35,46_BG85,85,85_BR-120_PKdp-play-icon-overlay__.png",
      "product_id": "B0FWD726XF",
      "parent_id": "B0DLRGKGTV"
    }
  ],
  "user_videos": [],
  "video_thumbnail": "https://m.media-amazon.com/images/I/9103IPwwY2L.SX522_.png",
  "has_video": true,
  "key_features": [
    "SUPERCHARGED BY M5 — The 14-inch MacBook Pro with M5 brings next-generation speed and powerful on-device AI to personal, professional, and creative tasks. Featuring all-day battery life and a breathtaking Liquid Retina XDR display with up to 1600 nits peak brightness, it's pro in every way.*",
    "HAPPILY EVER FASTER — Along with its faster CPU and unified memory, M5 features a more powerful GPU with a Neural Accelerator built into each core, delivering faster AI performance. So you can blaze through demanding workloads at mind-bending speeds.",
    "BUILT FOR APPLE INTELLIGENCE — Apple Intelligence is the personal intelligence system that helps you write, express yourself, and get things done effortlessly. With groundbreaking privacy protections, it gives you peace of mind that no one else can access your data — not even Apple.*",
    "ALL-DAY BATTERY LIFE — MacBook Pro delivers the same exceptional performance whether it's running on battery or plugged in.",
    "APPS FLY WITH APPLE SILICON — All your favorites, including Microsoft 365 and Adobe Creative Cloud, run lightning fast in macOS.*",
    "IF YOU LOVE IPHONE, YOU'LL LOVE MAC — Mac works like magic with your other Apple devices. View and control what's on your iPhone from your Mac with iPhone Mirroring.* Copy something on iPhone and paste it on Mac. Send texts with Messages or use your Mac to make and answer FaceTime calls.*",
    "BRILLIANT PRO DISPLAY — The 14.2-inch Liquid Retina XDR display features 1600 nits peak brightness, up to 1000 nits sustained brightness, and 1,000,000:1 contrast.*",
    "ADVANCED CAMERA AND AUDIO — Stay perfectly framed and sound great with a 12MP Center Stage camera, three studio-quality mics, and six speakers with Spatial Audio and support for Dolby Atmos.",
    "CONNECT IT ALL — This MacBook Pro features three Thunderbolt 4 ports and a MagSafe 3 charging port, SDXC card slot, HDMI port, and headphone jack. And it supports up to two external displays.",
    "* LEGAL DISCLAIMERS — This is a summary of the main product features. See below to learn more."
  ],
  "full_description": null,
  "technical_details": {
    "Product Dimensions": "12.31 x 8.71 x 0.61 inches",
    "Item Weight": "3.41 pounds",
    "Manufacturer": "Apple",
    "ASIN": "B0FWD726XF",
    "Item model number": "MDE34LL/A",
    "Batteries": "1 Lithium Ion batteries required. (included)",
    "Date First Available": "October 14, 2025"
  },
  "product_details": {
    "Brand": "Apple",
    "Model Name": "MacBook Pro",
    "Screen Size": "14.2 Inches",
    "Color": "Space Black",
    "Hard Disk Size": "1 TB",
    "CPU Model": "Unknown",
    "Ram Memory Installed Size": "24 GB",
    "Operating System": "Mac OS",
    "Special Feature": "Backlit Keyboard, Fingerprint Reader",
    "Graphics Card Description": "Integrated"
  },
  "main_category": {
    "id": "aps",
    "name": "All Departments"
  },
  "category_hierarchy": [],
  "variation_dimensions": [
    "style",
    "size",
    "color",
    "configuration"
  ],
  "variants": {
    "style": [
      {
        "asin": "B0FWD726XF",
        "value": "Apple M5 chip",
        "is_available": true
      },
      {
        "asin": "B0DLHBGBW3",
        "value": "Apple M4 Pro chip",
        "is_available": true
      },
      {
        "asin": "B0DLHCXF81",
        "value": "Apple M4 Max chip",
        "is_available": false
      }
    ],
    "size": [
      {
        "asin": "B0FWD623D1",
        "value": "16GB Unified Memory, 1TB SSD Storage",
        "is_available": true
      },
      {
        "asin": "B0FWD6SKL6",
        "value": "16GB Unified Memory, 512GB SSD Storage",
        "is_available": true
      },
      {
        "asin": "B0FWD726XF",
        "value": "24GB Unified Memory, 1TB SSD Storage",
        "is_available": true
      },
      {
        "asin": "B0DLHY2BJ6",
        "value": "24GB Unified Memory, 512GB SSD Storage",
        "is_available": false
      },
      {
        "asin": "B0DLHCXF81",
        "value": "36GB Unified Memory, 1TB SSD Storage",
        "is_available": false
      }
    ],
    "color": [
      {
        "asin": "B0FWD726XF",
        "value": "Space Black",
        "photo": "https://m.media-amazon.com/images/I/01B-RyYQGML.jpg",
        "is_available": true
      },
      {
        "asin": "B0FWD7QF6M",
        "value": "Silver",
        "photo": "https://m.media-amazon.com/images/I/01xh++YLubL.jpg",
        "is_available": true
      }
    ],
    "configuration": [
      {
        "asin": "B0FWD726XF",
        "value": "Without AppleCare+",
        "is_available": true
      },
      {
        "asin": "B0FWLNSJ1M",
        "value": "With AppleCare+ (3 Years)",
        "is_available": true
      }
    ]
  },
  "all_variants": {
    "B0FWLNSJ1M": {
      "style": "Apple M5 chip",
      "size": "24GB Unified Memory, 1TB SSD Storage",
      "color": "Space Black",
      "configuration": "With AppleCare+ (3 Years)"
    },
    "B0FWD5MR3L": {
      "style": "Apple M5 chip",
      "size": "16GB Unified Memory, 512GB SSD Storage",
      "color": "Silver",
      "configuration": "Without AppleCare+"
    },
    "B0DLHDJH98": {
      "style": "Apple M4 Pro chip",
      "size": "24GB Unified Memory, 512GB SSD Storage",
      "color": "Silver",
      "configuration": "Without AppleCare+"
    },
    "B0FWKR7V14": {
      "style": "Apple M5 chip",
      "size": "24GB Unified Memory, 1TB SSD Storage",
      "color": "Silver",
      "configuration": "With AppleCare+ (3 Years)"
    },
    "B0DM6YG983": {
      "style": "Apple M4 Pro chip",
      "size": "24GB Unified Memory, 1TB SSD Storage",
      "color": "Space Black",
      "configuration": "With AppleCare+ (3 Years)"
    },
    "B0FWKTY1NM": {
      "style": "Apple M5 chip",
      "size": "16GB Unified Memory, 1TB SSD Storage",
      "color": "Space Black",
      "configuration": "With AppleCare+ (3 Years)"
    },
    "B0DM6ZP92H": {
      "style": "Apple M4 Pro chip",
      "size": "24GB Unified Memory, 512GB SSD Storage",
      "color": "Space Black",
      "configuration": "With AppleCare+ (3 Years)"
    },
    "B0DLHBGBW3": {
      "style": "Apple M4 Pro chip",
      "size": "24GB Unified Memory, 1TB SSD Storage",
      "color": "Space Black",
      "configuration": "Without AppleCare+"
    },
    "B0DLHY2BJ6": {
      "style": "Apple M4 Pro chip",
      "size": "24GB Unified Memory, 512GB SSD Storage",
      "color": "Space Black",
      "configuration": "Without AppleCare+"
    },
    "B0FWD6SKL6": {
      "style": "Apple M5 chip",
      "size": "16GB Unified Memory, 512GB SSD Storage",
      "color": "Space Black",
      "configuration": "Without AppleCare+"
    },
    "B0FWKWYYKS": {
      "style": "Apple M5 chip",
      "size": "16GB Unified Memory, 512GB SSD Storage",
      "color": "Silver",
      "configuration": "With AppleCare+ (3 Years)"
    },
    "B0FWD623D1": {
      "style": "Apple M5 chip",
      "size": "16GB Unified Memory, 1TB SSD Storage",
      "color": "Space Black",
      "configuration": "Without AppleCare+"
    },
    "B0FWD7QF6M": {
      "style": "Apple M5 chip",
      "size": "24GB Unified Memory, 1TB SSD Storage",
      "color": "Silver",
      "configuration": "Without AppleCare+"
    },
    "B0FWKT6VRG": {
      "style": "Apple M5 chip",
      "size": "16GB Unified Memory, 512GB SSD Storage",
      "color": "Space Black",
      "configuration": "With AppleCare+ (3 Years)"
    },
    "B0DLHGN9P2": {
      "style": "Apple M4 Pro chip",
      "size": "24GB Unified Memory, 1TB SSD Storage",
      "color": "Silver",
      "configuration": "Without AppleCare+"
    },
    "B0FWKV16DL": {
      "style": "Apple M5 chip",
      "size": "16GB Unified Memory, 1TB SSD Storage",
      "color": "Silver",
      "configuration": "With AppleCare+ (3 Years)"
    },
    "B0FWD726XF": {
      "style": "Apple M5 chip",
      "size": "24GB Unified Memory, 1TB SSD Storage",
      "color": "Space Black",
      "configuration": "Without AppleCare+"
    },
    "B0FWD8WBSW": {
      "style": "Apple M5 chip",
      "size": "16GB Unified Memory, 1TB SSD Storage",
      "color": "Silver",
      "configuration": "Without AppleCare+"
    },
    "B0DLHCXF81": {
      "style": "Apple M4 Max chip",
      "size": "36GB Unified Memory, 1TB SSD Storage",
      "color": "Space Black",
      "configuration": "Without AppleCare+"
    },
    "B0DM733GXY": {
      "style": "Apple M4 Pro chip",
      "size": "24GB Unified Memory, 512GB SSD Storage",
      "color": "Silver",
      "configuration": "With AppleCare+ (3 Years)"
    }
  },
  "has_aplus_content": false,
  "aplus_images": [],
  "has_brand_story": false,
  "frequently_bought_together": []
}
```

</details>

---

### Top Product Reviews

```
GET https://amazon-scraper-api.omkar.cloud/amazon/product-reviews/top
```

#### Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `asin` | Yes | — | Amazon ASIN. |
| `country_code` | No | `US` | Amazon marketplace code. |

#### Example

```python
import requests

response = requests.get(
    "https://amazon-scraper-api.omkar.cloud/amazon/product-reviews/top",
    params={"asin": "B0FWD726XF", "country_code": "US"},
    headers={"API-Key": "YOUR_API_KEY"}
)

print(response.json())
```

#### Response

<details>
<summary>Sample Response (click to expand)</summary>

```json
{
  "results": [
    {
      "review_id": "R1H9MVROAP2XKG",
      "product_asin": "B0FWD726XF",
      "review_title": "Sleek, powerful, and a photographers dream. 11/10",
      "review_text": "The battery life is good for how powerful it is. Overall super happy with this laptop. Would buy again every time.",
      "review_link": "https://www.amazon.com/gp/customer-reviews/R1H9MVROAP2XKG",
      "rating": 5,
      "review_date": "Reviewed in the United States on December 1, 2025",
      "is_verified_purchase": true,
      "helpful_votes": 32,
      "reviewer_name": "Catherine Mason",
      "reviewer_id": "AF77JTSDDJJG6FXTYJAQOG4B4QFA",
      "reviewer_url": "https://www.amazon.com/gp/profile/amzn1.account.AF77JTSDDJJG6FXTYJAQOG4B4QFA",
      "reviewer_avatar": "https://m.media-amazon.com/images/S/amazon-avatars-global/default.png",
      "review_images": [],
      "review_video": null,
      "reviewed_variant": {
        "Style": "Apple M5 chip",
        "Capacity": "16GB Unified Memory, 512GB SSD Storage",
        "Color": "Space Black",
        "Set": "Without AppleCare+"
      },
      "is_vine_review": false
    }
  ]
}
```

</details>

### Supported Amazon Marketplaces

`US`, `AU`, `BR`, `CA`, `CN`, `FR`, `DE`, `IN`, `IT`, `MX`, `NL`, `SG`, `ES`, `TR`, `AE`, `GB`, `JP`, `SA`, `PL`, `SE`, `BE`, `EG`, `ZA`, `IE`

## Error Handling

```python
response = requests.get(
    "https://amazon-scraper-api.omkar.cloud/amazon/search",
    params={"query": "iPhone 16"},
    headers={"API-Key": "YOUR_API_KEY"}
)

if response.status_code == 200:
    data = response.json()
elif response.status_code == 401:
    # Invalid API key
    pass
elif response.status_code == 429:
    # Rate limit exceeded
    pass
```

## FAQs

### What data does the API return?

**Product Search** returns per product:
- Title, price, original price, currency
- Star rating, review count, sales volume
- ASIN, product URL, image URL
- Best Seller, Amazon's Choice, and Prime badges
- Delivery info, number of offers, lowest offer price

**Product Details** returns 50+ fields including:
- Full description, key features, technical specs
- All product images and videos
- Rating distribution breakdown
- Product variations (size, color, etc.)
- Category hierarchy, brand info
- Frequently bought together items
- Customer feedback summary and top reviews

**Top Reviews** returns per review:
- Review title, text, rating, and date
- Verified purchase status, helpful vote count
- Reviewer name, avatar, and profile link
- Review images and videos


All in structured JSON. Ready to use in your app.

### How accurate is the data?

Data is pulled from Amazon in real time. Every API call fetches live data — not cached or stale results. Prices, availability, ratings, and reviews reflect what's on Amazon right now.

### What's the difference between Product Search and Products by Category?

**Product Search** finds products matching a keyword or ASIN — like typing into the Amazon search bar.

**Products by Category** returns all products within a specific Amazon category (e.g., "Electronics" or "Books > Science Fiction"). Pass a `category_id` from the Amazon URL, get a paginated list of every product in that category.

Use Search when you know what you're looking for. Use Category when you want to explore or monitor an entire product segment.

## Rate Limits

| Plan | Price | Requests/Month |
|------|-------|----------------|
| Free | $0 | 5,000 |
| Starter | $25 | 100,000 |
| Grow | $75 | 1,000,000 |
| Scale | $150 | 10,000,000 |

## Questions? We have answers.

Reach out anytime. We will solve your query within 1 working day.

[![Contact Us on WhatsApp about Amazon Scraper](https://raw.githubusercontent.com/omkarcloud/assets/master/images/whatsapp-us.png)](https://api.whatsapp.com/send?phone=918178804274&text=I%20have%20a%20question%20about%20the%20Amazon%20Scraper%20API.)

[![Contact Us on Email about Amazon Scraper](https://raw.githubusercontent.com/omkarcloud/assets/master/images/ask-on-email.png)](mailto:happy.to.help@omkar.cloud?subject=Amazon%20Scraper%20API%20Question)
