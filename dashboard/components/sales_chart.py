"""Sales trend chart for OutIn products — supports daily and monthly views."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


def render_sales_chart(df: pd.DataFrame, granularity: str = "monthly") -> None:
    """Render the OutIn sales trend chart.

    Args:
        df: Sales DataFrame (daily or monthly depending on granularity).
        granularity: "daily" or "monthly".
    """
    if df.empty:
        st.info("暂无销量数据")
        return

    is_daily = granularity == "daily"
    x_col = "observed_date" if is_daily else "observed_month"

    sv_col = "sales_volume_num" if is_daily else "month_end_sales_volume"
    filtered = df[df[sv_col].notna() & (df[sv_col] > 0)].copy()
    if filtered.empty:
        st.info("所选范围内无有效销量数据")
        return

    if is_daily:
        change_col = "sales_volume_dod_change"
        metric_options = {
            "sales_volume_num": "当日销量 (累计 bought)",
            "sales_volume_dod_change": "日新增销量 (日环比变化)",
        }
    else:
        change_col = "sales_volume_mom_change"
        metric_options = {
            "month_end_sales_volume": "月末销量 (sales_volume)",
            "num_ratings_mom_delta": "评价数月增量 (近似月销量)",
        }

    metric_col = st.radio(
        "选择指标",
        list(metric_options.keys()),
        format_func=lambda x: metric_options[x],
        horizontal=True,
        key="sales_metric",
    )

    fig = go.Figure()
    for asin in filtered["asin"].unique():
        asin_df = filtered[filtered["asin"] == asin].sort_values(x_col)
        label = asin_df["product_title"].iloc[0] or asin
        if len(label) > 50:
            label = label[:50] + "..."
        fig.add_trace(go.Scatter(
            x=asin_df[x_col],
            y=asin_df[metric_col],
            mode="lines+markers",
            name=f"{asin} - {label}",
            hovertemplate=(
                "<b>%{fullData.name}</b><br>"
                + ("日期" if is_daily else "月份")
                + ": %{x}<br>"
                "值: %{y:,}<br>"
                "<extra></extra>"
            ),
        ))

    y_title = metric_options.get(metric_col, metric_col)
    x_title = "日期" if is_daily else "月份"
    title_prefix = "OutIn 产品日销量趋势" if is_daily else "OutIn 产品月销量趋势"

    n_asins = filtered["asin"].nunique()
    legend_rows = (n_asins + 1) // 2
    bottom_margin = max(100, legend_rows * 28 + 60)

    fig.update_layout(
        title=title_prefix,
        xaxis_title=x_title,
        yaxis_title=y_title,
        hovermode="x unified",
        height=500 + bottom_margin,
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.15,
            xanchor="left",
            x=0,
            font=dict(size=11),
            tracegroupgap=2,
        ),
        margin=dict(l=60, r=30, t=50, b=bottom_margin),
    )
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("查看价格趋势"):
        price_fig = go.Figure()
        for asin in filtered["asin"].unique():
            asin_df = filtered[filtered["asin"] == asin].sort_values(x_col)
            label = asin_df["product_title"].iloc[0] or asin
            if len(label) > 50:
                label = label[:50] + "..."
            price_col = "price" if is_daily else "month_end_price"
            price_data = asin_df[price_col].dropna()
            if price_data.empty:
                continue
            price_fig.add_trace(go.Scatter(
                x=asin_df.loc[price_data.index, x_col],
                y=price_data,
                mode="lines+markers",
                name=f"{asin} - {label}",
            ))
        price_n = filtered["asin"].nunique()
        price_legend_rows = (price_n + 1) // 2
        price_bm = max(100, price_legend_rows * 28 + 60)

        price_fig.update_layout(
            title="OutIn 产品价格趋势",
            xaxis_title=x_title,
            yaxis_title="价格",
            hovermode="x unified",
            height=500 + price_bm,
            legend=dict(
                orientation="h",
                yanchor="top",
                y=-0.15,
                xanchor="left",
                x=0,
                font=dict(size=11),
                tracegroupgap=2,
            ),
            margin=dict(l=60, r=30, t=50, b=price_bm),
        )
        st.plotly_chart(price_fig, use_container_width=True)
