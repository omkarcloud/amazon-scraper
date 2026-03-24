"""
OutIn Coffee Machine Market Analysis Dashboard.

Run with:  streamlit run dashboard/app.py
"""

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import streamlit as st

from src.database import create_db_connection
from src.queries import (
    get_filter_options,
    get_market_share,
    get_market_share_trend,
    get_outin_daily_sales,
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


def _get_healthy_connection():
    """Return a live DB connection, reconnecting if the cached one is dead."""
    conn = _get_connection()
    try:
        conn.ping(reconnect=True)
        return conn
    except Exception:
        _get_connection.clear()
        return _get_connection()


def main() -> None:
    conn = _get_healthy_connection()

    try:
        options = get_filter_options(conn)
    except Exception as e:
        _get_connection.clear()
        st.error(f"数据库连接或查询失败: {e}")
        st.info("请确认数据库已连接，且 ETL 已运行过至少一次。点击右上角 Rerun 重试。")
        return

    # ---- Sidebar ----
    with st.sidebar:
        st.header("筛选条件")

        granularity = st.radio(
            "视图粒度", ["日视图", "月视图"], horizontal=True, key="granularity",
        )
        is_daily = granularity == "日视图"

        # --- Time range ---
        if is_daily:
            dates = options.get("dates", [])
            if len(dates) > 1:
                date_objects = [datetime.strptime(d, "%Y-%m-%d").date()
                                for d in dates]
                start_date, end_date = st.date_input(
                    "日期范围",
                    value=(min(date_objects), max(date_objects)),
                    min_value=min(date_objects),
                    max_value=max(date_objects),
                    key="date_range",
                )
                start_date_str = start_date.strftime("%Y-%m-%d")
                end_date_str = end_date.strftime("%Y-%m-%d")
            elif len(dates) == 1:
                st.info(f"当前仅有 **{dates[0]}** 一天的数据")
                start_date_str = end_date_str = dates[0]
            else:
                start_date_str = end_date_str = None
            start_month = end_month = None
        else:
            months = options.get("months", [])
            if len(months) > 1:
                start_month, end_month = st.select_slider(
                    "时间范围",
                    options=months,
                    value=(months[0], months[-1]),
                )
            elif len(months) == 1:
                st.info(f"当前仅有 **{months[0]}** 一个月份的数据")
                start_month, end_month = months[0], months[0]
            else:
                start_month, end_month = None, None
            start_date_str = end_date_str = None

        # --- Cascading marketplace -> ASIN ---
        mp_map = options.get("marketplace_asin_map", {})
        all_marketplaces = sorted(mp_map.keys())

        marketplace = st.selectbox(
            "市场",
            options=["全部"] + all_marketplaces,
        )
        if marketplace == "全部":
            available_asins = []
            for items in mp_map.values():
                available_asins.extend(items)
            marketplace = None
        else:
            available_asins = mp_map.get(marketplace, [])

        seen = set()
        unique_asins = []
        for a in available_asins:
            if a["asin"] not in seen:
                seen.add(a["asin"])
                unique_asins.append(a)

        asin_labels = {
            a["asin"]: f"{a['asin']} - {(a['title'] or '')[:40]}"
            for a in unique_asins
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

    if is_daily:
        sales_df = get_outin_daily_sales(
            conn,
            marketplace=marketplace,
            start_date=start_date_str,
            end_date=end_date_str,
            asins=selected_asins,
        )
    else:
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

    _render_kpis(sales_df, review_df, share_df, is_daily)

    # ---- Tabs ----
    tab1, tab2, tab3 = st.tabs(["销量趋势", "客户评价变化", "市场份额"])

    with tab1:
        render_sales_chart(sales_df, granularity="daily" if is_daily else "monthly")

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
    sales_df: pd.DataFrame,
    review_df: pd.DataFrame,
    share_df: pd.DataFrame,
    is_daily: bool = False,
) -> None:
    """Render top-level KPI cards."""
    col1, col2, col3, col4 = st.columns(4)

    if not sales_df.empty:
        time_col = "observed_date" if is_daily else "observed_month"
        latest = sales_df.sort_values(time_col).iloc[-1]

        sv = latest.get("sales_volume_num" if is_daily else "month_end_sales_volume")
        col1.metric(
            "最新日销量" if is_daily else "最新月销量",
            f"{int(sv):,}" if pd.notna(sv) else "N/A",
            delta=(
                f"{int(latest['sales_volume_dod_change']):+,}"
                if is_daily and pd.notna(latest.get("sales_volume_dod_change"))
                else (
                    f"{int(latest['sales_volume_mom_change']):+,}"
                    if not is_daily and pd.notna(latest.get("sales_volume_mom_change"))
                    else None
                )
            ),
        )

        price_val = latest.get("price" if is_daily else "month_end_price")
        col2.metric(
            "最新价格",
            f"${float(price_val):.2f}" if pd.notna(price_val) else "N/A",
        )
    else:
        col1.metric("最新销量", "N/A")
        col2.metric("最新价格", "N/A")

    if not review_df.empty:
        latest_r = review_df.sort_values("observed_month").iloc[-1]
        sr = latest_r.get("month_end_star_rating")
        col3.metric(
            "最新评分",
            f"{float(sr):.2f}" if pd.notna(sr) else "N/A",
            delta=(
                f"{float(latest_r['star_rating_mom_change']):+.2f}"
                if pd.notna(latest_r.get("star_rating_mom_change"))
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
        col4.metric("市场份额", f"{pct:.1f}%" if pd.notna(pct) else "N/A")
    else:
        col4.metric("市场份额", "N/A")


if __name__ == "__main__":
    main()
