from config.db_config import get_sqlserver_connection, get_postgres_engine
from repository.sqlserver_repository import get_data_with_query
from repository.postgres_repository import insert_into_postgres
from services.transform_service import normalize_column_names

def run_ocrd_etl():
    """Ejecuta el ETL para OCRD con una consulta específica."""
    query = """
        SELECT TOP 1000 CardCode, CardName, CardType, GroupCode, CreateDate, UpdateDate  
        FROM OCRD
    """
    
    conn_sql = get_sqlserver_connection()
    engine_pg = get_postgres_engine()
    
    print("Extrayendo datos de OCRD...")
    df = get_data_with_query(conn_sql, query)
    df = normalize_column_names(df)
    
    print("Cargando OCRD en PostgreSQL...")
    insert_into_postgres(df, engine_pg, "dim_ocrd")
    
    conn_sql.close()
    print("ETL de OCRD completado.")
