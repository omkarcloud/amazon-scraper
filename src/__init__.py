from .amazon_scraper import Amazon
from .config import get_rds_conn
from .database import create_db_connection, close_tunnel
from .etl import (
    ensure_all_tables,
    extract_to_dwh,
    extract_reviews_to_dwh,
    extract_offers_to_dwh,
    extract_asin_hierarchy,
    clean_dwh_data,
    build_outin_daily_sales,
    build_outin_monthly_sales,
    build_outin_review_trend,
    build_coffee_market_share,
    build_product_daily_metrics,
    build_segment_product_daily_metrics,
    build_daily_sales_estimates,
    build_brand_market_share,
    build_segment_market_share,
    detect_and_store_trend_alerts,
)
from .rapidapi_client import RapidAmazonDataClient, RapidAPIError
from .sales_estimator import (
    BayesianDailySalesEstimator,
    CategoryParams,
    calibrate_bsr_params,
    estimate_daily_sales_batch,
    get_category_params,
)
from .tasks import (
    task_fetch_and_store_search,
    task_fetch_and_store_product_details,
    task_fetch_and_store_category,
    task_fetch_and_store_segment,
    task_fetch_and_store_bestsellers,
    task_fetch_and_store_reviews,
    task_fetch_and_store_offers,
    task_extract_to_dwh,
    task_extract_reviews_to_dwh,
    task_extract_offers_to_dwh,
    task_extract_asin_hierarchy,
    task_build_outin_daily_sales,
    task_build_outin_sales,
    task_build_review_trend,
    task_build_market_share,
    task_build_product_daily_metrics,
    task_build_segment_product_daily_metrics,
    task_build_daily_sales_estimates,
    task_build_brand_market_share,
    task_build_segment_market_share,
    task_detect_trend_alerts,
)
