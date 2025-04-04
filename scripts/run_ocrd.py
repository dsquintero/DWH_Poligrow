import sys
from dotenv import load_dotenv
from services.etl_ocrd import run_ocrd_etl

if __name__ == "__main__":

    env_file = ".env"
    if "--env" in sys.argv:
        env_file = sys.argv[sys.argv.index("--env") + 1]
    
    load_dotenv(dotenv_path=env_file) # Aquí cargas el .env según argumento
    run_ocrd_etl()
