import pandas as pd

def get_data_with_query(conn, query):
    return pd.read_sql(query, conn)
