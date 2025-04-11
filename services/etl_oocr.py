from repository.sqlserver_repository import get_data_with_query
from repository.postgres_repository import execute_postgres_query, insert_into_postgres
from services.transform_service import normalize_column_names

def run_oocr_etl():
    """Ejecuta el ETL para OOCR."""
    query = """
        SELECT OcrCode, OcrName, OcrTotal, Direct, Locked, DimCode, Active  
        FROM OOCR
    """

    print("Extrayendo datos de OOCR...")
    df = get_data_with_query(query)
    df = normalize_column_names(df)

    # Paso 1: Truncar staging
    print("Truncando staging.dim_oocr...")
    execute_postgres_query("TRUNCATE TABLE staging.dim_oocr;")

    # Paso 2: Insertar en staging
    print("Insertando en staging.dim_oocr...")
    insert_into_postgres(df, "dim_oocr") 

    # Paso 3: Truncar warehouse
    print("Truncando warehouse.dim_oocr...")
    execute_postgres_query("TRUNCATE TABLE warehouse.dim_oocr;")

    # Paso 4: Insertar en warehouse desde staging
    print("Insertando nuevos datos en warehouse.dim_oocr...")
    insert_query = """
    INSERT INTO warehouse.dim_oocr
    SELECT * FROM staging.dim_oocr;
    """
    execute_postgres_query(insert_query)

    # Paso 5: Limpiar staging
    print("Limpiando staging.dim_oocr...")
    execute_postgres_query("TRUNCATE TABLE staging.dim_oocr;")

    print("ETL de OOCR completado con limpieza y actualización de warehouse.")    