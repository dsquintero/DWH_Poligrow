import os

CRON_FILE = "config/cron_jobs.conf"

def update_cron():
    """Actualiza SOLO los CRON jobs del ETL sin borrar los manuales."""
    
    if not os.path.exists(CRON_FILE):
        print(f"Error: El archivo {CRON_FILE} no existe.")
        return

    with open(CRON_FILE, "r") as file:
        new_cron_jobs = file.read().strip().split("\n")  # Lista de líneas nuevas

    # Obtener los CRON actuales del usuario
    current_cron = os.popen("crontab -l").read().strip().split("\n")

    # Filtrar tareas previas del ETL (marcadas con "# ETL_")
    filtered_cron = [line for line in current_cron if not line.startswith("# ETL_") and not line.strip().startswith("/usr/bin/python3 /ruta/dwh_etl")]

    # Agregar las nuevas tareas
    final_cron = filtered_cron + new_cron_jobs

    # Guardar en crontab
    cron_text = "\n".join(final_cron)
    os.system(f"(echo \"{cron_text}\") | crontab -")

    print("✅ CRON actualizado sin borrar tareas manuales.")

if __name__ == "__main__":
    update_cron()
