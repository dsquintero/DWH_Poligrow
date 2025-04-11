from repository.sqlserver_repository import get_data_with_query
from repository.postgres_repository import fast_copy_insert, execute_postgres_query
from services.transform_service import normalize_column_names

def run_ojdt_etl():
    """Ejecuta el ETL para OJDT con una consulta específica."""
    query = """
    SELECT A.TransId, A.TransType AS Cab_TransType, A.BaseRef, A.RefDate, A.Memo,A.Ref1, A.Ref2, A.CreatedBy, A.DueDate, A.TaxDate, A.CreateDate, A.UpdateDate, 
    B.Line_ID, B.Account, B.Debit, B.Credit, (B.Debit-B.Credit) AS Amount, B.ShortName, B.LineMemo,B.TransType AS Line_TransType, B.ProfitCode AS OcrCode1 ,B.OcrCode2, B.OcrCode3, B.OcrCode4, B.OcrCode5, B.Project
    , B.U_InfoCo01 AS U_Tercero
    --, B.U_HBT_Tercero AS U_Tercero
    FROM OJDT A
    INNER JOIN JDT1 B ON B.TransId = A.TransId
    WHERE CAST(A.RefDate AS DATE) >= CAST(DATEADD(DAY, -30, GETDATE()) AS DATE)
    ORDER BY A.RefDate, A.TransId, B.Line_ID
    """
    
    print("Extrayendo datos de OJDT...")
    df = get_data_with_query(query)
    df = normalize_column_names(df)

    # Paso 1: Truncar staging
    print("Truncando staging.fact_journal...")
    execute_postgres_query("TRUNCATE TABLE staging.fact_journal;")
    
    # Paso 2: Insertar en staging
    print("Insertando en staging.fact_journal...")
    fast_copy_insert(df, "fact_journal")

    # Paso 3: Eliminar de warehouse por CardCode
    print("Eliminando datos existentes en warehouse.fact_journal...")
    delete_query = """
    DELETE FROM warehouse.fact_journal
    WHERE transid IN (SELECT DISTINCT transid FROM staging.fact_journal);
    """
    execute_postgres_query(delete_query)

    # Paso 4: Insertar en warehouse desde staging
    print("Insertando nuevos datos en warehouse.fact_journal...")
    insert_query = """
    INSERT INTO warehouse.fact_journal
    SELECT * FROM staging.fact_journal;
    """
    execute_postgres_query(insert_query)

    # Paso 5: Limpiar staging
    print("Limpiando staging.fact_journal...")
    execute_postgres_query("TRUNCATE TABLE staging.fact_journal;")

    print("ETL de OJDT completado con limpieza y actualización de warehouse.")
