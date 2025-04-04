import pandas as pd
from config.db_config import get_sqlserver_connection, get_sqlserver_engine

def get_data_with_query(query):
    conn = get_sqlserver_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_data_with_query2(query):
    engine = get_sqlserver_engine()
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df
