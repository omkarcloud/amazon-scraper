"""Monthly sales trend chart for OutIn products."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


def render_sales_chart(df: pd.DataFrame) -> None:
    """Render the OutIn monthly sales trend chart."""
    if df.empty:
        st.info("暂无销量数据")
        return

    metric_col = st.radio(
        "选择指标",
        ["month_end_sales_volume", "num_ratings_mom_delta"],
        format_func=lambda x: {
            "month_end_sales_volume": "月末销量 (sales_volume)",
            "num_ratings_mom_delta": "评价数月增量 (近似月销量)",
        }.get(x, x),
        horizontal=True,
        key="sales_metric",
    )

    fig = go.Figure()
    for asin in df["asin"].unique():
        asin_df = df[df["asin"] == asin].sort_values("observed_month")
        label = asin_df["product_title"].iloc[0] or asin
        if len(label) > 50:
            label = label[:50] + "..."
        fig.add_trace(go.Scatter(
            x=asin_df["observed_month"],
            y=asin_df[metric_col],
            mode="lines+markers",
            name=f"{asin} - {label}",
            hovertemplate=(
                "<b>%{fullData.name}</b><br>"
                "月份: %{x}<br>"
                "值: %{y:,}<br>"
                "<extra></extra>"
            ),
        ))

    y_title = (
        "月末销量" if metric_col == "month_end_sales_volume"
        else "评价数月增量"
    )
    fig.update_layout(
        title="OutIn 产品月销量趋势",
        xaxis_title="月份",
        yaxis_title=y_title,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=-0.3),
        margin=dict(l=60, r=30, t=50, b=80),
    )
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("查看价格趋势"):
        price_fig = go.Figure()
        for asin in df["asin"].unique():
            asin_df = df[df["asin"] == asin].sort_values("observed_month")
            label = asin_df["product_title"].iloc[0] or asin
            if len(label) > 50:
                label = label[:50] + "..."
            price_fig.add_trace(go.Scatter(
                x=asin_df["observed_month"],
                y=asin_df["month_end_price"],
                mode="lines+markers",
                name=f"{asin} - {label}",
            ))
        price_fig.update_layout(
            title="OutIn 产品价格趋势",
            xaxis_title="月份",
            yaxis_title="价格",
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=-0.3),
            margin=dict(l=60, r=30, t=50, b=80),
        )
        st.plotly_chart(price_fig, use_container_width=True)
