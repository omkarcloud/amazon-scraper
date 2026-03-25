"""
采集配置管理页面 — 增删改搜索关键词 / ASIN / 全局设置。
"""

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
import pymysql
import streamlit as st

CONFIG_DB = "gurysk_app"
CONFIG_TABLE = "app_scraper_config"

TYPE_LABELS = {
    "product_query": "单品/品牌关键词",
    "category_query": "品类关键词",
    "target_asin": "跟踪 ASIN",
    "setting": "全局设置",
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
    from src.config import DB_CONFIG, SSH_CONFIG
    from sshtunnel import SSHTunnelForwarder

    if SSH_CONFIG.get("enabled"):
        tunnel = SSHTunnelForwarder(
            (SSH_CONFIG["host"], SSH_CONFIG["port"]),
            ssh_username=SSH_CONFIG["user"],
            ssh_pkey=SSH_CONFIG["key_file"],
            remote_bind_address=(DB_CONFIG["host"], DB_CONFIG["port"]),
        )
        tunnel.start()
        conn = pymysql.connect(
            host="127.0.0.1",
            port=tunnel.local_bind_port,
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=CONFIG_DB,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
        )
    else:
        conn = pymysql.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=CONFIG_DB,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
        )
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
        cur.execute(
            f"SELECT * FROM {CONFIG_TABLE} ORDER BY config_type, id"
        )
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


def main():
    st.title("⚙️ 采集配置管理")
    st.caption("管理 Amazon 数据采集的关键词、ASIN 和全局参数。修改后在下次采集任务运行时自动生效。")

    conn = _healthy_conn()
    df = _load_configs(conn)

    # ---- Summary ----
    if not df.empty:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("单品关键词", len(df[df["config_type"] == "product_query"]))
        c2.metric("品类关键词", len(df[df["config_type"] == "category_query"]))
        c3.metric("跟踪 ASIN", len(df[df["config_type"] == "target_asin"]))
        c4.metric("全局设置", len(df[df["config_type"] == "setting"]))

    st.divider()

    # ---- Add new config ----
    with st.expander("➕ 新增配置项", expanded=False):
        _render_add_form(conn)

    st.divider()

    # ---- Config tables by type ----
    for ct, label in TYPE_LABELS.items():
        subset = df[df["config_type"] == ct] if not df.empty else pd.DataFrame()
        st.subheader(f"{label}  ({len(subset)})")
        if subset.empty:
            st.info("暂无配置")
        else:
            _render_config_table(conn, subset, ct)
        st.markdown("")


def _render_add_form(conn):
    with st.form("add_config_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            new_type = st.selectbox(
                "类型",
                options=list(TYPE_LABELS.keys()),
                format_func=lambda x: TYPE_LABELS[x],
            )
            new_key = st.text_input("关键词 / ASIN / 设置项名", placeholder="例: outin nano")
            new_value = st.text_input(
                "设置值（仅 setting 类型需要）", placeholder="例: 2",
            )
        with col2:
            new_schedule = st.selectbox(
                "调度频率",
                options=list(SCHEDULE_LABELS.keys()),
                format_func=lambda x: SCHEDULE_LABELS[x],
            )
            new_countries = st.text_input("适用市场", value="ALL", placeholder="ALL 或 US,GB,DE")
            new_pages = st.number_input("搜索页数", min_value=1, max_value=10, value=1)
        new_desc = st.text_input("说明", placeholder="配置说明")
        new_user = st.text_input("操作者 ID", placeholder="你的用户名")

        submitted = st.form_submit_button("添加", type="primary", use_container_width=True)
        if submitted:
            if not new_key.strip():
                st.error("关键词 / ASIN 不能为空")
            else:
                data = {
                    "config_type": new_type,
                    "config_key": new_key.strip(),
                    "config_value": new_value.strip() or None,
                    "schedule_profile": new_schedule,
                    "countries": new_countries.strip() or "ALL",
                    "pages": new_pages,
                    "is_active": 1,
                    "description": new_desc.strip() or None,
                    "user_id": new_user.strip() or None,
                }
                try:
                    _insert_config(conn, data)
                    st.success(f"已添加: [{TYPE_LABELS[new_type]}] {new_key}")
                    st.rerun()
                except pymysql.err.IntegrityError:
                    st.error("该配置已存在（类型 + 关键词 + 调度频率 组合必须唯一）")
                except Exception as e:
                    st.error(f"添加失败: {e}")


def _render_config_table(conn, subset: pd.DataFrame, config_type: str):
    for _, row in subset.iterrows():
        row_id = int(row["id"])
        cols = st.columns([3, 2, 1, 1, 1, 1, 1, 1])

        cols[0].text(row["config_key"])
        if config_type == "setting":
            cols[1].text(row.get("config_value") or "")
        else:
            cols[1].text(row.get("description") or "")
        cols[2].text(SCHEDULE_LABELS.get(row["schedule_profile"], ""))
        cols[3].text(str(row["countries"]))
        cols[4].text(str(row["pages"]))
        is_active = bool(row["is_active"])
        cols[5].text("✅" if is_active else "⏸️")

        toggle_label = "停用" if is_active else "启用"
        if cols[6].button(toggle_label, key=f"toggle_{row_id}", use_container_width=True):
            _update_config(conn, row_id, {
                "is_active": 0 if is_active else 1,
                "user_id": "dashboard",
            })
            st.rerun()

        if cols[7].button("🗑️", key=f"del_{row_id}", use_container_width=True):
            _delete_config(conn, row_id)
            st.rerun()


if __name__ == "__main__":
    main()
