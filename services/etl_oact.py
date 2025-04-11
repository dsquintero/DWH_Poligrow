from repository.sqlserver_repository import get_data_with_query
from repository.postgres_repository import execute_postgres_query, insert_into_postgres
from services.transform_service import normalize_column_names

def run_oact_etl():
    """Ejecuta el ETL para OACT con una consulta específica."""
    query = """
        SELECT AcctCode, AcctName, Levels, FatherNum, LocManTran, ActCurr, CreateDate, UpdateDate, ValidFor 
        FROM OACT
        --WHERE CAST(CreateDate AS DATE) >= CAST(DATEADD(DAY, -30, GETDATE()) AS DATE)
        --OR CAST(UpdateDate AS DATE) >= CAST(DATEADD(DAY, -30, GETDATE()) AS DATE)
    """
    
    print("Extrayendo datos de OACT...")
    df = get_data_with_query(query)
    df = normalize_column_names(df)
    
    # Paso 1: Truncar staging
    print("Truncando staging.dim_oact...")
    execute_postgres_query("TRUNCATE TABLE staging.dim_oact;")

    # Paso 2: Insertar en staging
    print("Insertando en staging.dim_oact...")
    insert_into_postgres(df, "dim_oact")

# Paso 3: Eliminar de warehouse por acctcode
    print("Eliminando datos existentes en warehouse.dim_oact...")
    delete_query = """
    DELETE FROM warehouse.dim_oact
    WHERE acctcode IN (SELECT acctcode FROM staging.dim_oact);
    """
    execute_postgres_query(delete_query)

    # Paso 4: Insertar en warehouse desde staging
    print("Insertando nuevos datos en warehouse.dim_oact...")
    insert_query = """
    INSERT INTO warehouse.dim_oact
    SELECT * FROM staging.dim_oact;
    """
    execute_postgres_query(insert_query)

    # Paso 5: Limpiar staging
    print("Limpiando staging.dim_oact...")
    execute_postgres_query("TRUNCATE TABLE staging.dim_oact;")

    print("ETL de OACT completado con limpieza y actualización de warehouse.")
