"""
OutIn Coffee Machine Market Analysis Dashboard.

Run with:  streamlit run dashboard/app.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st

from src.database import create_db_connection
from src.queries import (
    get_filter_options,
    get_market_share,
    get_market_share_trend,
    get_outin_monthly_sales,
    get_outin_review_trend,
)

from dashboard.components.market_share import render_market_share
from dashboard.components.review_chart import render_review_chart
from dashboard.components.sales_chart import render_sales_chart

st.set_page_config(
    page_title="OutIn 咖啡机市场分析",
    page_icon="☕",
    layout="wide",
)


@st.cache_resource
def _get_connection():
    return create_db_connection()


def main() -> None:
    conn = _get_connection()

    try:
        options = get_filter_options(conn)
    except Exception as e:
        st.error(f"数据库连接或查询失败: {e}")
        st.info("请确认数据库已连接，且 ETL 已运行过至少一次。")
        return

    # ---- Sidebar ----
    with st.sidebar:
        st.header("筛选条件")

        months = options.get("months", [])
        if months:
            start_month, end_month = st.select_slider(
                "时间范围",
                options=months,
                value=(months[0], months[-1]),
            )
        else:
            start_month, end_month = None, None

        marketplaces = options.get("marketplaces", [])
        marketplace = st.selectbox(
            "市场",
            options=["全部"] + marketplaces,
        )
        if marketplace == "全部":
            marketplace = None

        asin_options = options.get("asins", [])
        asin_labels = {
            a["asin"]: f"{a['asin']} - {(a['title'] or '')[:40]}"
            for a in asin_options
        }
        selected_asins = st.multiselect(
            "产品 (ASIN)",
            options=list(asin_labels.keys()),
            format_func=lambda x: asin_labels.get(x, x),
        )
        if not selected_asins:
            selected_asins = None

    # ---- Header KPIs ----
    st.title("OutIn 咖啡机市场分析")

    sales_df = get_outin_monthly_sales(
        conn,
        marketplace=marketplace,
        start_month=start_month,
        end_month=end_month,
        asins=selected_asins,
    )
    review_df = get_outin_review_trend(
        conn,
        marketplace=marketplace,
        start_month=start_month,
        end_month=end_month,
        asins=selected_asins,
    )
    share_df = get_market_share(conn, marketplace=marketplace, month=end_month)
    trend_df = get_market_share_trend(
        conn,
        brand="outin",
        marketplace=marketplace,
        start_month=start_month,
        end_month=end_month,
    )

    _render_kpis(sales_df, review_df, share_df)

    # ---- Tabs ----
    tab1, tab2, tab3 = st.tabs(["月销量趋势", "客户评价变化", "市场份额"])

    with tab1:
        render_sales_chart(sales_df)

    with tab2:
        render_review_chart(review_df)

    with tab3:
        render_market_share(share_df, trend_df)

    # ---- Raw Data ----
    with st.expander("查看原始数据"):
        data_tab = st.radio(
            "数据集", ["销量", "评价", "市场份额"], horizontal=True,
            key="raw_data_tab",
        )
        if data_tab == "销量":
            st.dataframe(sales_df, use_container_width=True)
        elif data_tab == "评价":
            st.dataframe(review_df, use_container_width=True)
        else:
            st.dataframe(share_df, use_container_width=True)


def _render_kpis(
    sales_df,
    review_df,
    share_df,
) -> None:
    """Render top-level KPI cards."""
    col1, col2, col3, col4 = st.columns(4)

    if not sales_df.empty:
        latest = sales_df.sort_values("observed_month").iloc[-1]
        col1.metric(
            "最新月销量",
            f"{latest.get('month_end_sales_volume', 'N/A'):,}"
            if latest.get("month_end_sales_volume") is not None
            else "N/A",
            delta=(
                f"{int(latest['sales_volume_mom_change']):+,}"
                if latest.get("sales_volume_mom_change") is not None
                else None
            ),
        )
        col2.metric(
            "最新月价格",
            f"${latest.get('month_end_price', 0):.2f}"
            if latest.get("month_end_price") is not None
            else "N/A",
        )
    else:
        col1.metric("最新月销量", "N/A")
        col2.metric("最新月价格", "N/A")

    if not review_df.empty:
        latest_r = review_df.sort_values("observed_month").iloc[-1]
        col3.metric(
            "最新评分",
            f"{latest_r.get('month_end_star_rating', 0):.2f}"
            if latest_r.get("month_end_star_rating") is not None
            else "N/A",
            delta=(
                f"{latest_r['star_rating_mom_change']:+.2f}"
                if latest_r.get("star_rating_mom_change") is not None
                else None
            ),
        )
    else:
        col3.metric("最新评分", "N/A")

    outin_share = share_df[
        share_df["brand"].str.lower().str.contains("outin", na=False)
    ] if not share_df.empty else share_df
    if not outin_share.empty:
        pct = outin_share["rating_share_pct"].iloc[0]
        col4.metric("市场份额", f"{pct:.1f}%")
    else:
        col4.metric("市场份额", "N/A")


if __name__ == "__main__":
    main()
