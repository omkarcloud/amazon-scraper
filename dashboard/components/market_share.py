"""Market share charts for the coffee machine category."""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


def render_market_share(
    share_df: pd.DataFrame,
    trend_df: pd.DataFrame,
) -> None:
    """Render market share pie chart and trend line."""
    col1, col2 = st.columns(2)

    with col1:
        _render_pie(share_df)

    with col2:
        _render_trend(trend_df)

    with st.expander("查看完整品牌数据"):
        if not share_df.empty:
            display_cols = [
                "brand", "distinct_asins", "total_num_ratings",
                "total_sales_volume", "avg_price", "avg_star_rating",
                "rating_share_pct", "sales_share_pct",
            ]
            existing_cols = [c for c in display_cols if c in share_df.columns]
            st.dataframe(
                share_df[existing_cols]
                .sort_values("rating_share_pct", ascending=False)
                .reset_index(drop=True),
                use_container_width=True,
            )


def _render_pie(df: pd.DataFrame) -> None:
    """Render market share pie chart for the latest month."""
    if df.empty:
        st.info("暂无市场份额数据")
        return

    month_label = df["observed_month"].iloc[0] if not df.empty else ""

    top_n = 10
    if len(df) > top_n:
        top = df.nlargest(top_n - 1, "rating_share_pct")
        others = pd.DataFrame([{
            "brand": "其他",
            "rating_share_pct": df[~df["brand"].isin(top["brand"])]["rating_share_pct"].sum(),
        }])
        plot_df = pd.concat([top, others], ignore_index=True)
    else:
        plot_df = df.copy()

    outin_mask = plot_df["brand"].str.lower().str.contains("outin", na=False)
    colors = ["#ff6b6b" if m else None for m in outin_mask]

    fig = px.pie(
        plot_df,
        values="rating_share_pct",
        names="brand",
        title=f"咖啡机市场份额 ({month_label})",
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    if any(colors):
        pull = [0.08 if m else 0 for m in outin_mask]
        fig.update_traces(pull=pull)

    fig.update_traces(
        textposition="inside",
        textinfo="label+percent",
        hovertemplate="<b>%{label}</b><br>份额: %{percent}<extra></extra>",
    )
    fig.update_layout(
        showlegend=False,
        margin=dict(l=20, r=20, t=50, b=20),
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_trend(df: pd.DataFrame) -> None:
    """Render OutIn market share trend over time."""
    if df.empty:
        st.info("暂无 OutIn 市场份额趋势数据")
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["observed_month"],
        y=df["rating_share_pct"],
        mode="lines+markers+text",
        name="评价数份额",
        text=[f"{v:.1f}%" for v in df["rating_share_pct"]],
        textposition="top center",
        line=dict(color="#ff6b6b", width=3),
        marker=dict(size=10),
    ))

    if "sales_share_pct" in df.columns and df["sales_share_pct"].sum() > 0:
        fig.add_trace(go.Scatter(
            x=df["observed_month"],
            y=df["sales_share_pct"],
            mode="lines+markers+text",
            name="销量份额",
            text=[f"{v:.1f}%" for v in df["sales_share_pct"]],
            textposition="bottom center",
            line=dict(color="#4ecdc4", width=3),
            marker=dict(size=10),
        ))

    fig.update_layout(
        title="OutIn 市场份额变化趋势",
        xaxis_title="月份",
        yaxis_title="份额 (%)",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=-0.25),
        margin=dict(l=60, r=30, t=50, b=80),
    )
    st.plotly_chart(fig, use_container_width=True)
