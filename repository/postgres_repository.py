from config.db_config import get_postgres_engine

def insert_into_postgres(df, table_name):
    engine = get_postgres_engine()
    df.to_sql(table_name, engine, schema="staging", if_exists="append", index=False)
