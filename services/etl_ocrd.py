from repository.sqlserver_repository import get_data_with_query2
from repository.postgres_repository import insert_into_postgres
from services.transform_service import normalize_column_names

def run_ocrd_etl():
    """Ejecuta el ETL para OCRD con una consulta específica."""
    query = """
        SELECT TOP 1000 CardCode, CardName, CardType, GroupCode, CreateDate, UpdateDate  
        FROM OCRD
    """
    
    print("Extrayendo datos de OCRD...")
    df = get_data_with_query2(query)
    df = normalize_column_names(df)
    
    print("Cargando OCRD en PostgreSQL...")
    insert_into_postgres(df, "dim_ocrd")
    
    print("ETL de OCRD completado.")
