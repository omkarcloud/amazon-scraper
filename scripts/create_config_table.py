"""Create app_scraper_config table in gurysk_app and insert initial data."""
import os
import pymysql


def main():
    conn = pymysql.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database="gurysk_app",
        charset="utf8mb4",
    )

    ddl = """
    CREATE TABLE IF NOT EXISTS app_scraper_config (
        id INT AUTO_INCREMENT PRIMARY KEY,
        config_type ENUM('product_query','category_query','target_asin','setting') NOT NULL
            COMMENT 'product_query=单品/品牌关键词, category_query=品类关键词, target_asin=跟踪ASIN, setting=全局设置',
        config_key VARCHAR(128) NOT NULL
            COMMENT '关键词/ASIN/设置项名称',
        config_value TEXT
            COMMENT '设置项的值(仅setting类型使用)',
        schedule_profile ENUM('daily','weekly','both') NOT NULL DEFAULT 'daily'
            COMMENT 'daily=每日, weekly=每周, both=两者',
        countries VARCHAR(512) NOT NULL DEFAULT 'ALL'
            COMMENT '适用市场(ALL或逗号分隔国家码)',
        pages INT NOT NULL DEFAULT 1
            COMMENT '搜索页数(仅query类型有效)',
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
    COMMENT='Amazon采集器配置表'
    """

    seed = """
    INSERT INTO app_scraper_config
        (config_type, config_key, config_value, schedule_profile, countries, pages, is_active, description, user_id)
    VALUES
        ('product_query','outin',NULL,'daily','ALL',1,1,'Outin品牌关键词 - 追踪品牌搜索表现','system'),
        ('category_query','coffee machines',NULL,'weekly','ALL',3,1,'咖啡机大类 - 市场份额基数','system'),
        ('category_query','portable coffee maker',NULL,'weekly','ALL',3,1,'便携咖啡机细分 - 细分市场份额','system'),
        ('target_asin','B0BRKFWPF3',NULL,'daily','ALL',1,1,'Outin Nano - 核心单品日度跟踪','system'),
        ('setting','request_delay','1','both','ALL',1,1,'API请求间隔(秒)','system'),
        ('setting','daily_schedule_time','08:00','daily','ALL',1,1,'每日采集时间(CST)','system'),
        ('setting','weekly_schedule_day','monday','weekly','ALL',1,1,'每周采集执行日','system')
    ON DUPLICATE KEY UPDATE update_timestamp=CURRENT_TIMESTAMP
    """

    with conn.cursor() as cur:
        cur.execute(ddl)
        cur.execute(seed)
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT id, config_type, config_key, config_value, schedule_profile, countries, pages, is_active, description, user_id FROM app_scraper_config ORDER BY config_type, id")
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        print("Table created with %d rows:" % len(rows))
        for r in rows:
            print("  ", dict(zip(cols, r)))

    conn.close()


if __name__ == "__main__":
    main()
