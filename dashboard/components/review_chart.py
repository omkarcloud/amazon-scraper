"""Review trend charts for OutIn products."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots


def render_review_chart(df: pd.DataFrame) -> None:
    """Render the OutIn review trend combo chart."""
    if df.empty:
        st.info("暂无评价数据")
        return

    selected_asin = st.selectbox(
        "选择产品",
        options=["全部"] + df["asin"].unique().tolist(),
        format_func=lambda x: (
            "全部产品"
            if x == "全部"
            else f"{x} - {df[df['asin']==x]['product_title'].iloc[0][:40]}..."
            if len(df[df["asin"] == x]["product_title"].iloc[0] or "") > 40
            else f"{x} - {df[df['asin']==x]['product_title'].iloc[0]}"
        ),
        key="review_asin",
    )

    if selected_asin == "全部":
        plot_df = (
            df.groupby("observed_month")
            .agg(
                new_ratings_this_month=("new_ratings_this_month", "sum"),
                month_end_star_rating=("month_end_star_rating", "mean"),
                month_end_num_ratings=("month_end_num_ratings", "sum"),
            )
            .reset_index()
            .sort_values("observed_month")
        )
        title = "OutIn 全部产品 - 评价趋势"
    else:
        plot_df = (
            df[df["asin"] == selected_asin]
            .sort_values("observed_month")
            .copy()
        )
        title = f"{selected_asin} - 评价趋势"

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Bar(
            x=plot_df["observed_month"],
            y=plot_df["new_ratings_this_month"],
            name="月新增评价数",
            marker_color="rgba(55, 128, 191, 0.6)",
            hovertemplate="新增评价: %{y:,}<extra></extra>",
        ),
        secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(
            x=plot_df["observed_month"],
            y=plot_df["month_end_star_rating"],
            mode="lines+markers",
            name="评分",
            line=dict(color="rgba(219, 64, 82, 1)", width=2),
            marker=dict(size=8),
            hovertemplate="评分: %{y:.2f}<extra></extra>",
        ),
        secondary_y=True,
    )

    fig.update_layout(
        title=title,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=-0.25),
        margin=dict(l=60, r=60, t=50, b=80),
    )
    fig.update_yaxes(title_text="月新增评价数", secondary_y=False)
    fig.update_yaxes(title_text="评分", range=[0, 5.5], secondary_y=True)
    fig.update_xaxes(title_text="月份")

    st.plotly_chart(fig, use_container_width=True)

    with st.expander("查看累计评价数趋势"):
        cum_fig = go.Figure()
        if selected_asin == "全部":
            for asin in df["asin"].unique():
                asin_df = df[df["asin"] == asin].sort_values("observed_month")
                cum_fig.add_trace(go.Scatter(
                    x=asin_df["observed_month"],
                    y=asin_df["month_end_num_ratings"],
                    mode="lines+markers",
                    name=asin,
                ))
        else:
            cum_fig.add_trace(go.Scatter(
                x=plot_df["observed_month"],
                y=plot_df["month_end_num_ratings"],
                mode="lines+markers",
                name=selected_asin,
            ))
        cum_fig.update_layout(
            title="累计评价数",
            xaxis_title="月份",
            yaxis_title="累计评价数",
            hovermode="x unified",
        )
        st.plotly_chart(cum_fig, use_container_width=True)
