# dags/dynamic_dag.py
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.config import DAG_CONFIG, TABLE_CONFIGS
from include.utils import build_idempotency_sql, build_insert_sql


class WeekTemplates:
    @staticmethod
    def previous_week_start(date_str: str) -> str:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        week_start = dt - timedelta(days=dt.weekday() + 7)
        return week_start.strftime("%Y-%m-%d")

    @staticmethod
    def previous_week_end(date_str: str) -> str:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        week_start = dt - timedelta(days=dt.weekday() + 7)
        week_end = week_start + timedelta(days=6)
        return week_end.strftime("%Y-%m-%d")


with DAG(
    dag_id=DAG_CONFIG["dag_id"],
    schedule=DAG_CONFIG["schedule"],
    start_date=DAG_CONFIG["start_date"],
    catchup=DAG_CONFIG["catchup"],
    max_active_runs=DAG_CONFIG["max_active_runs"],
    tags=DAG_CONFIG["tags"],
    description=DAG_CONFIG["description"],
    default_args={
        "owner": "enjout",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    user_defined_macros={
        "previous_week_start": WeekTemplates.previous_week_start,
        "previous_week_end": WeekTemplates.previous_week_end,
    },
) as dag:

    dag_start = EmptyOperator(task_id="dag_start")
    dag_end = EmptyOperator(task_id="dag_end")


    group_end_tasks = []

    for cfg in TABLE_CONFIGS:
        table_name = cfg["table_name"]
        group_id = f"group_{table_name}"

        with TaskGroup(group_id=group_id, tooltip=f"Агрегат: {table_name}") as tg:
            # 1. Создаём таблицу (DDL)
            create_table = PostgresOperator(
                task_id=f"create_{table_name}",
                postgres_conn_id="conn_pg",
                sql=cfg["table_ddl"],
                autocommit=True,  # DDL обычно требуют autocommit
            )

            # 2. Загрузка: DELETE + INSERT
            idempotency_sql = build_idempotency_sql(table_name)
            insert_sql = build_insert_sql(table_name, cfg["table_dml"])

            load_data = PostgresOperator(
                task_id=f"load_{table_name}",
                postgres_conn_id="conn_pg",
                sql=[idempotency_sql, insert_sql],
                autocommit=False,  # чтобы DELETE+INSERT были в одной транзакции
            )

            # 3. Проверка качества
            def quality_check(table, min_rows, **context):
                hook = PostgresHook(postgres_conn_id="conn_pg")
                week_start = context["macros"].previous_week_start(context["ds"])
                week_end = context["macros"].previous_week_end(context["ds"])

                sql = f"""
                    SELECT COUNT(*) FROM {table}
                    WHERE week_start = %s::date AND week_end = %s::date
                """
                row_count = hook.get_first(sql, parameters=(week_start, week_end))[0]

                print(f"Проверка качества: таблица={table}, строк={row_count}, min={min_rows}")
                if row_count < min_rows:
                    raise ValueError(f"Недостаточно строк в таблице {table}: {row_count} < {min_rows}")

            check_quality = PythonOperator(
                task_id=f"check_quality_{table_name}",
                python_callable=quality_check,
                op_kwargs={"table": table_name, "min_rows": cfg["min_rows"]},
            )

            create_table >> load_data >> check_quality

            # 4. Экспорт в S3
            if cfg.get("need_to_export"):
                def export_to_s3(table, **context):
                    hook = PostgresHook(postgres_conn_id="conn_pg")
                    week_start = context["macros"].previous_week_start(context["ds"])
                    week_end = context["macros"].previous_week_end(context["ds"])

                    sql = f"""
                        SELECT * FROM {table}
                        WHERE week_start = %s::date AND week_end = %s::date
                    """
                    rows = hook.get_records(sql, parameters=(week_start, week_end))
                    if not rows:
                        print("Нет данных для экспорта.")
                        return

                    columns = hook.get_columns(table)  # или явно задать список колонок
                    col_names = [c[0] for c in columns]

        
                    import csv
                    from io import StringIO
                    import boto3
                    from botocore.client import Config

                    s3_conn = hook.get_connection("conn_s3")
                    s3 = boto3.client(
                        "s3",
                        endpoint_url=s3_conn.host,
                        aws_access_key_id=s3_conn.login,
                        aws_secret_access_key=s3_conn.password,
                        config=Config(signature_version="s3v4"),
                    )

                    csv_buffer = StringIO()
                    writer = csv.writer(csv_buffer, delimiter="\t", lineterminator="\n")
                    writer.writerow(col_names)
                    writer.writerows(rows)

                    key = f"aggregates/{table}_{week_start}_to_{week_end}_{context['ds']}.csv"
                    s3.put_object(Body=csv_buffer.getvalue(), Bucket="your-bucket-name", Key=key)
                    print(f"Экспорт: s3://your-bucket-name/{key} ({len(rows)} строк)")

                export_task = PythonOperator(
                    task_id=f"export_{table_name}_s3",
                    python_callable=export_to_s3,
                    op_kwargs={"table": table_name},
                )
                check_quality >> export_task
                group_end_tasks.append(export_task)
            else:
                group_end_tasks.append(check_quality)

        dag_start >> tg

    for end_task in group_end_tasks:
        end_task >> dag_end
