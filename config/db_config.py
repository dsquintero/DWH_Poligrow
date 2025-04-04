import os
import pyodbc
from sqlalchemy import create_engine
#from dotenv import load_dotenv

#load_dotenv()
#def load_environment(env_file=".env"):
#    load_dotenv(dotenv_path=env_file)

def get_sqlserver_connection():
    conn_str = (
        f"DRIVER={{{os.getenv('SQL_DRIVER')}}};"
        f"SERVER={os.getenv('SQL_SERVER')};"
        f"DATABASE={os.getenv('SQL_DATABASE')};"
        f"UID={os.getenv('SQL_USER')};"
        f"PWD={os.getenv('SQL_PASSWORD')};"
    )
    return pyodbc.connect(conn_str)

def get_postgres_engine():
    postgres_conn_str = (
        f"postgresql://{os.getenv('POSTGRES_USER')}:"
        f"{os.getenv('POSTGRES_PASSWORD')}@"
        f"{os.getenv('POSTGRES_HOST')}:"
        f"{os.getenv('POSTGRES_PORT')}/"
        f"{os.getenv('POSTGRES_DB')}"
    )
    return create_engine(postgres_conn_str)
