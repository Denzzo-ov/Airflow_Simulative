def build_idempotency_sql(table_name: str) -> str:
    return f"""
        DELETE FROM {table_name}
        WHERE week_start = '{{{{ previous_week_start(ds) }}}}'::date
          AND week_end   = '{{{{ previous_week_end(ds)   }}}}'::date;
    """

def build_insert_sql(table_name: str, dml_query: str) -> str:
    return f"INSERT INTO {table_name}\n{dml_query}"
