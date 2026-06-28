from datetime import datetime

DAG_CONFIG = {
    "dag_id": "enjout_exam_aggregates",
    "schedule": "0 2 * * 1",  # каждый понедельник в 02:00
    "start_date": datetime(2024, 1, 1),
    "catchup": False,
    "max_active_runs": 1,
    "max_active_tasks": 32,
    "tags": ["enjout", "aggregates"],
    "description": "Динамические агрегаты по таблицам из конфига",
}

TABLE_CONFIGS = [
    {
        "table_name": "agg_sales_weekly",
        "table_ddl": """
            CREATE TABLE IF NOT EXISTS agg_sales_weekly (
                week_start date NOT NULL,
                week_end date NOT NULL,
                category varchar,
                total_amount numeric,
                load_dt date NOT NULL
            )
        """,

        "table_dml": """
            SELECT
                '{{ previous_week_start(ds) }}'::date AS week_start,
                '{{ previous_week_end(ds) }}'::date AS week_end,
                category,
                SUM(amount) AS total_amount,
                CURRENT_DATE AS load_dt
            FROM raw_sales
            WHERE sale_date BETWEEN '{{ previous_week_start(ds) }}'::date
                                AND '{{ previous_week_end(ds) }}'::date
            GROUP BY category
        """,
        "need_to_export": True,
        "min_rows": 10,
    },
    {
        "table_name": "agg_users_weekly",
        "table_ddl": """
            CREATE TABLE IF NOT EXISTS agg_users_weekly (
                week_start date NOT NULL,
                week_end date NOT NULL,
                segment varchar,
                new_users bigint,
                load_dt date NOT NULL
            )
        """,
        "table_dml": """
            SELECT
                '{{ previous_week_start(ds) }}'::date,
                '{{ previous_week_end(ds) }}'::date,
                segment,
                COUNT(DISTINCT user_id) AS new_users,
                CURRENT_DATE
            FROM raw_users
            WHERE registered_at BETWEEN '{{ previous_week_start(ds) }}'::date
                                    AND '{{ previous_week_end(ds) }}'::date
            GROUP BY segment
        """,
        "need_to_export": False,
        "min_rows": 5,
    },
]
