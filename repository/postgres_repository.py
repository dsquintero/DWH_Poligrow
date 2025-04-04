def insert_into_postgres(df, engine, table_name):
    df.to_sql(table_name, engine, schema="staging", if_exists="append", index=False)
