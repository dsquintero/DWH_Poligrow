
# DWH Poligrow

Este proyecto implementa un conjunto de procesos ETL modulares para la extracción de datos desde SQL Server, su transformación y carga en PostgreSQL. Se ha estructurado de forma limpia usando buenas prácticas de separación por capas.

## 🏗️ Estructura del Proyecto

```
dwh_etl/
├── config/            # Configuración de conexiones y cron
├── repository/        # Acceso a fuentes de datos
├── services/          # Transformaciones y lógica de negocio
├── scripts/           # Scripts ejecutables para cada ETL
├── .env               # Variables de entorno por defecto
├── requirements.txt   # Dependencias
```

## ⚙️ Requisitos

- Python 3.8+
- PostgreSQL
- SQL Server (con ODBC Driver instalado)

Instalar dependencias:

```bash
pip install -r requirements.txt
```

## 🌍 Variables de entorno

Utiliza múltiples archivos `.env` para manejar entornos (dev, prod, test). Ejemplo de `.env`:

```env
SQL_SERVER=sqlserver_host
SQL_DATABASE=nombre_bd
SQL_USER=usuario
SQL_PASSWORD=clave
SQL_DRIVER=ODBC Driver 17 for SQL Server

POSTGRES_USER=usuario_pg
POSTGRES_PASSWORD=clave_pg
POSTGRES_HOST=host_pg
POSTGRES_PORT=5432
POSTGRES_DB=nombre_bd_pg
```

Puedes usar múltiples entornos:

```bash
python3 scripts/run_ocrd.py --env .env.prod
```

## 📅 Agendar con CRON

Puedes programar ETLs usando cron jobs definidos en `config/cron_jobs.conf` y actualizarlos con:

```bash
python3 scripts/update_cron.py
```

## 🧪 ETLs Disponibles

- OCRD (Clientes)
- OCRG (Grupos de Clientes)
- OACT (Cuentas contables)
- OOCR (Centros de costos)

Cada uno tiene su script ejecutable en la carpeta `scripts/`.

## 🛠️ Estandarización

- Todos los nombres de columnas se transforman a minúsculas antes de insertarse en PostgreSQL.
- Los procesos están desacoplados para facilitar su mantenimiento y ejecución independiente.
