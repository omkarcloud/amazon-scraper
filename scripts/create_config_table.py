"""Create / upgrade app_scraper_config table in gurysk_app with seed data."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import create_db_connection


def main():
    conn = create_db_connection(database="gurysk_app")

    ddl = """
    CREATE TABLE IF NOT EXISTS app_scraper_config (
        id INT AUTO_INCREMENT PRIMARY KEY,
        config_type VARCHAR(32) NOT NULL
            COMMENT 'product_query / category_query / target_asin / brand_search / category_scan / segment_scan / bestseller_scan / review_scan / offer_scan / setting',
        config_key VARCHAR(128) NOT NULL
            COMMENT '关键词/ASIN/category_id/品牌名/设置项名称',
        config_value TEXT
            COMMENT '设置项的值 或 附加参数 JSON',
        schedule_profile ENUM('daily','weekly','both') NOT NULL DEFAULT 'daily'
            COMMENT 'daily=每日, weekly=每周, both=两者',
        countries VARCHAR(512) NOT NULL DEFAULT 'US'
            COMMENT '适用市场(ALL或逗号分隔国家码)',
        pages INT NOT NULL DEFAULT 1
            COMMENT '搜索/类目页数',
        is_active TINYINT(1) NOT NULL DEFAULT 1
            COMMENT '1=启用, 0=停用',
        description VARCHAR(255)
            COMMENT '配置说明',
        user_id VARCHAR(64)
            COMMENT '创建/修改者ID',
        create_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        update_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_type_key_schedule (config_type, config_key, schedule_profile)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    COMMENT='Amazon采集器配置表 - 支持多品牌多品类扩展'
    """

    seed = """
    INSERT INTO app_scraper_config
        (config_type, config_key, config_value, schedule_profile, countries, pages, is_active, description, user_id)
    VALUES
        -- Legacy: keyword search
        ('product_query','outin',NULL,'daily','US',1,1,'Outin品牌关键词搜索','system'),
        -- Legacy: category keyword search
        ('category_query','coffee machines',NULL,'weekly','US',3,1,'咖啡机大类关键词搜索','system'),
        ('category_query','portable coffee maker',NULL,'weekly','US',3,1,'便携咖啡机细分搜索','system'),
        -- Target ASIN tracking
        ('target_asin','B0BRKFWPF3',NULL,'daily','US',1,1,'Outin Nano - 核心单品跟踪','system'),
        -- Brand search (brand filter on /search)
        ('brand_search','OUTIN','{"brand":"OUTIN","query":"coffee maker"}','daily','US',2,1,'OUTIN品牌搜索(带brand过滤)','system'),
        -- Category scan (/products-by-category)
        ('category_scan','289745','{"category_name":"Coffee Machines"}','daily','US',3,1,'Home & Kitchen > Kitchen & Dining > Coffee Machines 类目扫描','system'),
        -- Segment scan (custom keyword-defined market)
        ('segment_scan','portable coffee machine','{"segment_name":"Portable Coffee Machines"}','daily','US',2,1,'自定义细分市场：便携咖啡机','system'),
        -- BSR scan (/best-sellers)
        ('bestseller_scan','kitchen/coffee-machines',NULL,'weekly','US',1,1,'咖啡机Best Seller排名','system'),
        -- Review scan (/top-product-reviews)
        ('review_scan','B0BRKFWPF3',NULL,'weekly','US',1,1,'Outin Nano Top Reviews采集','system'),
        -- Offer scan (/product-offers)
        ('offer_scan','B0BRKFWPF3',NULL,'daily','US',1,1,'Outin Nano 多卖家报价监控','system'),
        -- Settings
        ('setting','request_delay','1','both','US',1,1,'API请求间隔(秒)','system'),
        ('setting','daily_schedule_time','08:00','daily','US',1,1,'每日采集时间(CST)','system'),
        ('setting','weekly_schedule_day','monday','weekly','US',1,1,'每周采集执行日','system')
    ON DUPLICATE KEY UPDATE
        config_value=VALUES(config_value),
        countries=VALUES(countries),
        pages=VALUES(pages),
        is_active=VALUES(is_active),
        description=VALUES(description),
        user_id=VALUES(user_id),
        update_timestamp=CURRENT_TIMESTAMP
    """

    with conn.cursor() as cur:
        cur.execute(ddl)
        cur.execute(seed)
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, config_type, config_key, config_value, schedule_profile, "
            "countries, pages, is_active, description, user_id "
            "FROM app_scraper_config ORDER BY config_type, id"
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        print("Table created with %d rows:" % len(rows))
        for r in rows:
            print("  ", dict(zip(cols, r)))

    conn.close()


if __name__ == "__main__":
    main()
