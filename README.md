# 🛒 Amazon Scraper

Amazon Scraper is a **free and open-source** scraper that gets you **unlimited** detailed Amazon data for free.

## ✨ What Can I Get?

- 🛍️ **Full details on any product in 23 marketplaces** — price, seller, stock, delivery, specs, images, variants & AI review summary
- 🔍 **Search & browse every category** — sort by price, reviews or best sellers; filter by brand, Prime, rating & deals
- 🏆 **Top 100 Best Sellers in every category** — plus New Releases, Movers & Shakers, Most Wished For & Today's Deals
- 🏪 **Offers, sellers & influencers** — every seller's offer, seller ratings & feedback, influencer storefronts

## 🎥 Example: A Full Amazon Product

```json
{
  "asin": "B07QSFHT27",
  "title": "PAVOI 14K Gold Plated Crystal Solitaire 1.5 Carat (7.3mm) Cubic Zirconia Dainty Choker Necklace | Gold Necklaces for Women",
  "link": "https://www.amazon.com/dp/B07QSFHT27",
  "brand": { "name": "PAVOI", "store_link": "https://www.amazon.com/stores/PAVOIJewelry/page/6D60F9B2-5B35-4871-AEF7-73C2BF359213" },
  "price": { "amount": 13.45, "currency": "USD", "list_price": null },
  "availability": { "text": "In Stock", "is_in_stock": true },
  "buybox": {
    "seller": { "name": "PAVOI Jewelry", "id": "A1H1EU8178QTRH", "link": "https://www.amazon.com/sp?seller=A1H1EU8178QTRH" },
    "is_fulfilled_by_amazon": false
  },
  "rating": { "average": 4.4, "count": 18151, "histogram": { "5_star": 71, "4_star": 12, "3_star": 8, "2_star": 3, "1_star": 6 } },
  "bought_past_month": 3000,
  "best_sellers_rank": [
    { "rank": 6, "category": "Women's Pendant Necklaces", "link": "https://www.amazon.com/gp/bestsellers/fashion/7454934011" }
  ],
  "details": { "material": "Yellow Gold", "metal_type": "14k gold plated", "date_first_available": "April 18, 2019" },
  "images": [{ "link": "https://m.media-amazon.com/images/I/61n6j7tPrvL._AC_SL1500_.jpg", "variant": "MAIN" }],
  "variations": { "parent_asin": "B0F48ZYTDC", "count": 5 },
  "customers_say": {
    "summary": "Customers love this necklace for its high-shine CZ stone that maintains its sparkle over time, and appreciate its simple design.",
    "aspects": [{ "name": "quality", "sentiment": "positive", "mentions": 819, "mentions_percentage": 79 }]
  },
  "top_reviews": [
    { "id": "ROM1ZNS2SXNC3", "title": "Beautiful Quality Doesnt Tarnish", "rating": 5.0, "date": "2026-09-03", "is_verified_purchase": true }
  ]
}
```

*Trimmed for readability.*

## 🚀 Unlimited Free Amazon Data — Get It in 60 Seconds

1️⃣ Clone and install:
```bash
git clone https://github.com/omkarcloud/amazon-scraper
cd amazon-scraper
python -m pip install -r requirements.txt
```

2️⃣ Start the API:
```bash
python run.py
```

3️⃣ Get your first data:
```bash
curl "http://localhost:8000/products/details?product=B07QSFHT27"
```

```json
{
  "asin": "B07QSFHT27",
  "title": "PAVOI 14K Gold Plated Crystal Solitaire 1.5 Carat (7.3mm) Cubic Zirconia Dainty Choker Necklace | Gold Necklaces for Women",
  "link": "https://www.amazon.com/dp/B07QSFHT27",
  "price": { "amount": 13.45, "currency": "USD", "list_price": null },
  "availability": { "text": "In Stock", "is_in_stock": true, "quantity_left": null },
  "buybox": {
    "seller": { "name": "PAVOI Jewelry", "id": "A1H1EU8178QTRH", "link": "https://www.amazon.com/sp?seller=A1H1EU8178QTRH" }
  },
  "rating": { "average": 4.4, "count": 18151 },
  "bought_past_month": 3000,
  "best_sellers_rank": [
    { "rank": 2946, "category": "Clothing, Shoes & Jewelry", "link": "https://www.amazon.com/gp/bestsellers/fashion" },
    { "rank": 6, "category": "Women's Pendant Necklaces", "link": "https://www.amazon.com/gp/bestsellers/fashion/7454934011" }
  ],
  "details": { "material": "Yellow Gold", "metal_type": "14k gold plated" }
}
```

All 21 endpoints are now live at `http://localhost:8000`.

## 📚 Endpoints

21 endpoints cover everything you need.

| Endpoint | Path | Returns |
|---|---|---|
| Product Details | `/products/details` | Price, seller, stock, delivery, specs, images, variants & reviews in one call |
| Search Autocomplete | `/search/autocomplete` | Amazon's live search-box suggestions |
| Search Products | `/search` | Products with sort, price, brand, Prime, rating & deal filters |
| Product Reviews | `/products/reviews` | Top reviews, star histogram & the AI "Customers say" summary |
| Product Offers | `/products/offers` | Every seller's offer with price, condition & delivery |
| Product Variations | `/products/variations` | Every size, color & style with its own ASIN |
| Bulk Products | `/products/bulk` | Price, stock & rating for up to 10 products at once |
| Product Lookup by Barcode | `/products/lookup` | Products matching a UPC, EAN, GTIN or ISBN |
| Best Sellers | `/best-sellers` | Top 100 Best Sellers, New Releases, Movers & Shakers & more |
| Best Seller Categories | `/best-sellers/categories` | The full best sellers category tree |
| Deals | `/deals` | Today's Deals with deal price, discount & end time |
| Categories / Category Tree | `/categories`, `/categories/tree` | Every department, and any category's parents & children |
| Category Products | `/categories/products` | Every product in a category, sortable and filterable |
| Seller Details / Feedback / Products | `/sellers/details`, `/sellers/feedback`, `/sellers/products` | Seller profile, ratings, customer feedback & full catalog |
| Influencer Details / Posts / List Products | `/influencers/details`, `/influencers/posts`, `/influencers/posts/products` | Influencer storefronts, idea lists & their products |
| Scrape Any Amazon URL | `/scrape` | Paste any Amazon link, get structured JSON back |

## 🔍 Exploring Parameters

The same API is published on RapidAPI, and its playground is the easiest place to try parameters and see raw responses. Once a request looks right, run it locally for **unlimited free** data.

1. [Subscribe to the free plan](https://rapidapi.com/OmkarCloud/api/best-amazon-scraper-free-1000-calls/pricing) — 1,000 calls/month, no credit card.
2. [Try the endpoints in the playground](https://rapidapi.com/OmkarCloud/api/best-amazon-scraper-free-1000-calls/playground) — every param is pre-filled, so you see real data in one click.
3. Copy the generated code and replace `https://best-amazon-scraper-free-1000-calls.p.rapidapi.com` with `http://localhost:8000`. It will now run against your local API.

```python
import requests

# generated by the playground, host swapped for the local API
response = requests.get(
    "http://localhost:8000/products/details",
    params={"product": "B07QSFHT27"},
)
print(response.json())
```

## 💬 Have Questions? We Have Answers.

You're a developer — we know how hard completing a project can be. So we offer full support: just message us and we'll reply ✅ with a solution within 1 working day.

[![Message Us on WhatsApp about Amazon Scraper](https://raw.githubusercontent.com/omkarcloud/assets/master/images/whatsapp-us.png)](https://api.whatsapp.com/send?phone=918178804274&text=I%20need%20help%20using%20the%20Amazon%20Scraper%20API.)

[![Ask Us by Email about Amazon Scraper](https://raw.githubusercontent.com/omkarcloud/assets/master/images/ask-on-email.png)](mailto:happy.to.help@omkar.cloud?subject=Help%20with%20Amazon%20Scraper%20API&body=I%20need%20help%20using%20the%20Amazon%20Scraper%20API.)

## ⚡ Popular Scrapers by Omkar Cloud

- [**Google Maps Scraper (3,100+ GitHub Stars)**](https://github.com/omkarcloud/google-maps-scraper) — type "dentists in New York", get every business as a ready-to-call lead list: phones, emails, websites & reviews. Up to 100K free leads/month.
- [**G2 Scraper**](https://www.omkar.cloud/tools/g2-scraper) — G2 product details, ratings & AI-found contacts
- [**Website Email Contact Scraper**](https://www.omkar.cloud/tools/website-email-contact-scraper) — emails, phones & socials from any website
- [**AliExpress Scraper**](https://www.omkar.cloud/tools/aliexpress-scraper) — live product details, SKU variants, stock & shipping
- [**Booking Scraper**](https://www.omkar.cloud/tools/booking-scraper) — Booking.com hotels: prices, ratings, rooms & amenities
- [**Etsy Scraper**](https://www.omkar.cloud/tools/etsy-scraper) — Etsy products: prices, discounts, shops & variations

## ⭐ Love It? [Star It ⭐!](https://github.com/omkarcloud/amazon-scraper)

Star the repo ⭐ and become my star hero!

It's just 1 click, but it means the world to me.

[![Star us on GitHub](https://raw.githubusercontent.com/omkarcloud/google-maps-scraper/master/screenshots/star-us.png)](https://github.com/omkarcloud/amazon-scraper)
