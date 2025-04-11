from config.db_config import get_postgres_engine
from sqlalchemy import text

def insert_into_postgres(df, table_name):
    engine = get_postgres_engine()
    df.to_sql(table_name, engine, schema="staging", if_exists="append", index=False, chunksize=500,method="multi")

def execute_postgres_query(sql):
    engine = get_postgres_engine()
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()