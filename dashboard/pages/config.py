"""
采集配置管理页面 — 更直观的任务控制台。
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
import pymysql
import streamlit as st

CONFIG_DB = "gurysk_app"
CONFIG_TABLE = "app_scraper_config"

TYPE_META = {
    "product_query": {
        "label": "单品/品牌关键词",
        "icon": "🔎",
        "desc": "按关键词搜索商品，适合发现品牌或单品相关结果。",
        "key_label": "搜索关键词",
        "key_placeholder": "例: outin nano",
    },
    "category_query": {
        "label": "品类关键词",
        "icon": "🧭",
        "desc": "按品类关键词搜索，适合宽口径探索市场。",
        "key_label": "品类关键词",
        "key_placeholder": "例: coffee machines",
    },
    "target_asin": {
        "label": "跟踪 ASIN",
        "icon": "🎯",
        "desc": "持续跟踪核心商品详情，适合重点监控。",
        "key_label": "ASIN",
        "key_placeholder": "例: B0BRKFWPF3",
    },
    "brand_search": {
        "label": "品牌搜索",
        "icon": "🏷️",
        "desc": "按品牌+查询词抓取品牌市场表现。",
        "key_label": "品牌名",
        "key_placeholder": "例: OUTIN",
    },
    "category_scan": {
        "label": "类目扫描",
        "icon": "🗂️",
        "desc": "按 Amazon 官方类目 ID 扫描，适合做类目份额。",
        "key_label": "类目 ID",
        "key_placeholder": "例: 289745",
    },
    "segment_scan": {
        "label": "自定义细分市场",
        "icon": "📦",
        "desc": "按关键词定义业务市场池，适合便携咖啡机这类自定义市场。",
        "key_label": "细分市场关键词",
        "key_placeholder": "例: portable coffee machine",
    },
    "bestseller_scan": {
        "label": "榜单扫描",
        "icon": "📈",
        "desc": "抓取榜单商品，用于识别头部品牌和品类热度。",
        "key_label": "榜单类目",
        "key_placeholder": "例: kitchen/coffee-machines",
    },
    "review_scan": {
        "label": "评论采集",
        "icon": "💬",
        "desc": "抓取重点 ASIN 的评论，用于评价趋势分析。",
        "key_label": "ASIN",
        "key_placeholder": "例: B0BRKFWPF3",
    },
    "offer_scan": {
        "label": "报价采集",
        "icon": "💲",
        "desc": "抓取重点 ASIN 的报价，用于折扣和竞价监控。",
        "key_label": "ASIN",
        "key_placeholder": "例: B0BRKFWPF3",
    },
    "setting": {
        "label": "全局设置",
        "icon": "⚙️",
        "desc": "控制采集节奏与调度参数，不直接抓取商品。",
        "key_label": "设置项名称",
        "key_placeholder": "例: request_delay",
    },
}

SCHEDULE_LABELS = {
    "daily": "每日",
    "weekly": "每周",
    "both": "两者",
}

st.set_page_config(
    page_title="采集配置管理",
    page_icon="⚙️",
    layout="wide",
)


@st.cache_resource
def _get_config_conn():
    from src.database import create_db_connection

    conn = create_db_connection(database=CONFIG_DB)
    conn.cursorclass = pymysql.cursors.DictCursor
    return conn


def _healthy_conn():
    conn = _get_config_conn()
    try:
        conn.ping(reconnect=True)
        return conn
    except Exception:
        _get_config_conn.clear()
        return _get_config_conn()


def _load_configs(conn) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {CONFIG_TABLE} ORDER BY config_type, id")
        rows = cur.fetchall()
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _insert_config(conn, data: dict):
    cols = ", ".join(data.keys())
    phs = ", ".join(["%s"] * len(data))
    sql = f"INSERT INTO {CONFIG_TABLE} ({cols}) VALUES ({phs})"
    with conn.cursor() as cur:
        cur.execute(sql, list(data.values()))
    conn.commit()


def _update_config(conn, row_id: int, data: dict):
    sets = ", ".join(f"{k}=%s" for k in data)
    sql = f"UPDATE {CONFIG_TABLE} SET {sets} WHERE id=%s"
    with conn.cursor() as cur:
        cur.execute(sql, list(data.values()) + [row_id])
    conn.commit()


def _delete_config(conn, row_id: int):
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {CONFIG_TABLE} WHERE id=%s", (row_id,))
    conn.commit()


def _parse_config_value(raw_value):
    if raw_value in (None, ""):
        return None
    try:
        return json.loads(raw_value)
    except (TypeError, json.JSONDecodeError):
        return raw_value


def _inject_styles() -> None:
    st.markdown(
        """
        <style>
        .cfg-banner {
            padding: 1rem 1.1rem;
            border: 1px solid rgba(120,120,120,0.22);
            border-radius: 16px;
            background: linear-gradient(180deg, rgba(245,247,250,0.96), rgba(236,240,244,0.94));
            margin-bottom: 0.8rem;
        }
        .cfg-banner h3 {
            margin: 0 0 0.35rem 0;
            font-size: 1.1rem;
            color: #1f2937;
        }
        .cfg-banner p {
            margin: 0;
            color: #4b5563;
            line-height: 1.45;
        }
        .cfg-card {
            border: 1px solid rgba(120,120,120,0.22);
            border-radius: 14px;
            padding: 0.9rem 1rem;
            margin: 0.45rem 0 0.75rem 0;
            background: rgba(245,247,250,0.94);
        }
        .cfg-title {
            font-weight: 600;
            font-size: 1rem;
            margin-bottom: 0.15rem;
            color: #111827;
        }
        .cfg-subtle {
            color: #4b5563;
            font-size: 0.9rem;
        }
        .cfg-chip-row {
            display: flex;
            flex-wrap: wrap;
            gap: 0.4rem;
            margin-top: 0.6rem;
        }
        .cfg-chip {
            display: inline-block;
            padding: 0.2rem 0.55rem;
            border-radius: 999px;
            font-size: 0.78rem;
            border: 1px solid rgba(120,120,120,0.24);
            background: rgba(255,255,255,0.82);
            color: #374151;
        }
        .cfg-chip-ok {
            color: #166534;
            border-color: rgba(125,220,139,0.35);
            background: rgba(220,252,231,0.75);
        }
        .cfg-chip-pause {
            color: #92400e;
            border-color: rgba(246,199,96,0.35);
            background: rgba(254,243,199,0.85);
        }
        .cfg-form-note {
            border: 1px dashed rgba(120,120,120,0.28);
            border-radius: 12px;
            padding: 0.75rem 0.9rem;
            background: rgba(248,250,252,0.95);
            color: #374151;
            margin: 0.25rem 0 0.75rem 0;
            line-height: 1.5;
        }
        .cfg-form-note strong {
            color: #111827;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _safe_text(value, default="—"):
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def _render_summary(df: pd.DataFrame) -> None:
    total_count = len(df)
    active_count = int(df["is_active"].fillna(0).astype(int).sum()) if not df.empty else 0
    paused_count = total_count - active_count
    daily_count = len(df[df["schedule_profile"] == "daily"]) if not df.empty else 0
    weekly_count = len(df[df["schedule_profile"] == "weekly"]) if not df.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("总配置数", total_count)
    c2.metric("启用中", active_count)
    c3.metric("已停用", paused_count)
    c4.metric("每日任务", daily_count)

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("每周任务", weekly_count)
    c6.metric("类目扫描", len(df[df["config_type"] == "category_scan"]) if not df.empty else 0)
    c7.metric("自定义细分市场", len(df[df["config_type"] == "segment_scan"]) if not df.empty else 0)
    c8.metric("跟踪 ASIN", len(df[df["config_type"] == "target_asin"]) if not df.empty else 0)


def _render_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.markdown("### 浏览与筛选")
    col1, col2, col3 = st.columns([2.2, 1.2, 1.2])
    with col1:
        keyword = st.text_input("搜索配置", placeholder="搜索关键词、ASIN、说明或类目名称", key="cfg_search")
    with col2:
        type_filter = st.selectbox(
            "类型筛选",
            options=["全部"] + list(TYPE_META.keys()),
            format_func=lambda x: "全部类型" if x == "全部" else TYPE_META[x]["label"],
            key="cfg_type_filter",
        )
    with col3:
        status_filter = st.selectbox(
            "状态筛选",
            options=["全部", "启用", "停用"],
            key="cfg_status_filter",
        )

    filtered = df.copy()
    if keyword.strip():
        search_text = keyword.strip().lower()

        def _row_matches(row) -> bool:
            parsed = _parse_config_value(row.get("config_value"))
            values = [
                row.get("config_key"),
                row.get("description"),
                row.get("user_id"),
                parsed.get("category_name") if isinstance(parsed, dict) else None,
                parsed.get("segment_name") if isinstance(parsed, dict) else None,
            ]
            return any(search_text in str(v).lower() for v in values if v is not None)

        filtered = filtered[filtered.apply(_row_matches, axis=1)]

    if type_filter != "全部":
        filtered = filtered[filtered["config_type"] == type_filter]

    if status_filter == "启用":
        filtered = filtered[filtered["is_active"] == 1]
    elif status_filter == "停用":
        filtered = filtered[filtered["is_active"] != 1]

    st.caption(f"当前显示 {len(filtered)} 条配置")
    return filtered


def _build_config_value(new_type: str, raw_value: str, category_name: str, segment_name: str):
    config_value = raw_value.strip() or None
    if new_type not in {"category_scan", "segment_scan"}:
        return config_value

    payload = {}
    if config_value:
        try:
            parsed = json.loads(config_value)
            if isinstance(parsed, dict):
                payload.update(parsed)
        except json.JSONDecodeError as exc:
            raise ValueError("附加参数必须是合法 JSON。") from exc

    if new_type == "category_scan" and category_name.strip():
        payload["category_name"] = category_name.strip()
    if new_type == "segment_scan" and segment_name.strip():
        payload["segment_name"] = segment_name.strip()

    return json.dumps(payload, ensure_ascii=False) if payload else None


def main():
    _inject_styles()
    st.title("采集配置管理")
    st.markdown(
        """
        <div class="cfg-banner">
            <h3>任务控制台</h3>
            <p>在这里统一管理 Amazon 数据采集任务。你可以新增关键词、官方类目、自定义细分市场、跟踪 ASIN 和全局设置，修改会在下一轮调度时自动生效。</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    conn = _healthy_conn()
    df = _load_configs(conn)

    _render_summary(df)
    st.divider()

    with st.expander("新增配置项", expanded=False):
        _render_add_form(conn)

    st.divider()
    filtered_df = _render_filters(df)
    st.divider()

    for config_type, meta in TYPE_META.items():
        subset = filtered_df[filtered_df["config_type"] == config_type] if not filtered_df.empty else pd.DataFrame()
        with st.expander(f"{meta['icon']} {meta['label']}  ({len(subset)})", expanded=len(subset) > 0 and config_type in {"segment_scan", "category_scan", "brand_search"}):
            st.caption(meta["desc"])
            if subset.empty:
                st.info("当前筛选条件下暂无配置")
            else:
                _render_config_table(conn, subset, config_type)


def _render_add_form(conn):
    new_type = st.selectbox(
        "类型",
        options=list(TYPE_META.keys()),
        format_func=lambda x: f"{TYPE_META[x]['icon']} {TYPE_META[x]['label']}",
        key="cfg_new_type",
    )
    meta = TYPE_META[new_type]

    st.info(meta["desc"])
    st.markdown(
        f"""
        <div class="cfg-form-note">
            <strong>填写说明</strong><br/>
            1. <strong>{meta['key_label']}</strong>：这条任务的主键值，决定采集什么。<br/>
            2. <strong>附加参数 JSON</strong>：仅在需要补充过滤条件或扩展参数时填写。<br/>
            3. <strong>调度频率 / 适用市场 / 搜索页数</strong>：决定采集何时运行、在哪些站点运行、抓多少页。<br/>
            4. <strong>说明</strong>：建议写清用途，方便后续维护。<br/>
            5. <strong>操作者 ID</strong>：用于记录是谁创建或修改了这条配置。
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.form("add_config_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            new_key = st.text_input(
                meta["key_label"],
                placeholder=meta["key_placeholder"],
                help="主采集键。关键词类填搜索词；ASIN 类填商品 ASIN；类目扫描填 Amazon 类目 ID；细分市场填你定义市场池的关键词。",
            )
            new_value = st.text_area(
                "附加参数 JSON（可选）",
                placeholder='例如：{"query":"coffee maker"}',
                height=80,
                help="用于补充额外参数。brand_search 可写 query/brand；category_scan 可扩展 category_name；segment_scan 可扩展 segment_name。不会写 JSON 可以留空。",
            )
            new_desc = st.text_input(
                "说明",
                placeholder="给这条任务起一个便于识别的说明",
                help="建议描述任务目的，例如“OutIn 核心单品跟踪”或“便携咖啡机市场池采样”。",
            )
        with col2:
            new_schedule = st.selectbox(
                "调度频率",
                options=list(SCHEDULE_LABELS.keys()),
                format_func=lambda x: SCHEDULE_LABELS[x],
                help="决定任务会在每日、每周，还是两种调度中执行。",
            )
            new_countries = st.text_input(
                "适用市场",
                value="ALL",
                placeholder="ALL 或 US,GB,DE",
                help="决定在哪些 Amazon 站点采集。可填 ALL，也可填逗号分隔的国家码。",
            )
            new_pages = st.number_input(
                "搜索页数",
                min_value=1,
                max_value=10,
                value=1,
                help="搜索类任务会抓取的页数。页数越大，API 消耗越高。",
            )
            new_user = st.text_input(
                "操作者 ID",
                placeholder="你的用户名",
                help="用于记录谁创建或维护了这条配置。",
            )

        extra1, extra2 = st.columns(2)
        with extra1:
            category_name = st.text_input(
                "类目名称（仅 category_scan）",
                placeholder="例: Coffee Machines",
                disabled=new_type != "category_scan",
                help="仅 category_scan 需要。用于把官方类目 ID 映射成更易读的名称，便于首页筛选与展示。",
            )
        with extra2:
            segment_name = st.text_input(
                "细分市场名称（仅 segment_scan）",
                placeholder="例: Portable Coffee Machines",
                disabled=new_type != "segment_scan",
                help="仅 segment_scan 需要。用于给自定义关键词市场池命名，后续首页会直接显示这个名称。",
            )

        submitted = st.form_submit_button("添加配置", type="primary", use_container_width=True)
        if submitted:
            if not new_key.strip():
                st.error("关键词 / ASIN / 类目不能为空")
                return

            try:
                config_value = _build_config_value(new_type, new_value, category_name, segment_name)
            except ValueError as exc:
                st.error(str(exc))
                return

            data = {
                "config_type": new_type,
                "config_key": new_key.strip(),
                "config_value": config_value,
                "schedule_profile": new_schedule,
                "countries": new_countries.strip() or "ALL",
                "pages": new_pages,
                "is_active": 1,
                "description": new_desc.strip() or None,
                "user_id": new_user.strip() or None,
            }
            try:
                _insert_config(conn, data)
                st.success(f"已添加：[{TYPE_META[new_type]['label']}] {new_key}")
                st.rerun()
            except pymysql.err.IntegrityError:
                st.error("该配置已存在（类型 + 主键 + 调度频率 组合必须唯一）")
            except Exception as exc:
                st.error(f"添加失败：{exc}")


def _status_chip(is_active: bool) -> str:
    if is_active:
        return '<span class="cfg-chip cfg-chip-ok">启用中</span>'
    return '<span class="cfg-chip cfg-chip-pause">已停用</span>'


def _render_config_table(conn, subset: pd.DataFrame, config_type: str):
    for _, row in subset.iterrows():
        row_id = int(row["id"])
        parsed_value = _parse_config_value(row.get("config_value"))
        display_name = row["config_key"]
        display_subtitle = row.get("description") or "未填写说明"

        if config_type == "category_scan" and isinstance(parsed_value, dict):
            display_subtitle = parsed_value.get("category_name") or display_subtitle
        elif config_type == "segment_scan" and isinstance(parsed_value, dict):
            display_subtitle = parsed_value.get("segment_name") or display_subtitle
        elif config_type == "setting":
            display_subtitle = row.get("config_value") or "未设置值"

        st.markdown(
            f"""
            <div class="cfg-card">
                <div class="cfg-title">{TYPE_META[config_type]['icon']} {display_name}</div>
                <div class="cfg-subtle">{_safe_text(display_subtitle)}</div>
                <div class="cfg-chip-row">
                    <span class="cfg-chip">{SCHEDULE_LABELS.get(row['schedule_profile'], '—')}</span>
                    <span class="cfg-chip">市场 {_safe_text(row.get('countries'))}</span>
                    <span class="cfg-chip">页数 {int(row.get('pages') or 0)}</span>
                    {_status_chip(bool(row.get('is_active')))}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        action_col1, action_col2, action_col3 = st.columns([1, 1, 6])
        toggle_label = "停用" if bool(row.get("is_active")) else "启用"
        if action_col1.button(toggle_label, key=f"toggle_{row_id}", use_container_width=True):
            _update_config(conn, row_id, {
                "is_active": 0 if bool(row.get("is_active")) else 1,
                "user_id": "dashboard",
            })
            st.rerun()

        if action_col2.button("删除", key=f"del_{row_id}", use_container_width=True):
            _delete_config(conn, row_id)
            st.rerun()

        with action_col3.expander("查看配置明细", expanded=False):
            detail = {
                "id": row.get("id"),
                "config_type": row.get("config_type"),
                "config_key": row.get("config_key"),
                "config_value": parsed_value,
                "schedule_profile": row.get("schedule_profile"),
                "countries": row.get("countries"),
                "pages": row.get("pages"),
                "is_active": row.get("is_active"),
                "description": row.get("description"),
                "user_id": row.get("user_id"),
            }
            st.json(detail, expanded=False)


if __name__ == "__main__":
    main()
