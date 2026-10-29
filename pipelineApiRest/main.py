import os
import time
import logging
import pandas as pd
import requests
from requests.exceptions import RequestException, Timeout, HTTPError
from config import API_TOKEN, API_EMAIL, API_BASE_URL

# Configuración del logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Funciones principales del pipeline
def fetch_data(dataset_type: str = "ecommerce", rows: int = 1000) -> dict:
    """Obtiene datos de la API."""
    url = f"{API_BASE_URL}/datasets.php"
    params = {
        "type": dataset_type,
        "rows": rows,
        "token": API_TOKEN,
        "key": API_TOKEN,
        "email": API_EMAIL,
    }

    logger.info(f"Fetching {rows} rows of {dataset_type} data...")

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    orders = data.get("tables", {}).get("orders", [])
    logger.info(f"Received {len(orders)} orders")

    return data

# Función para manejar reintentos en caso de errores
def fetch_data_with_retry(
    dataset_type: str = "ecommerce",
    rows: int = 1000,
    max_retries: int = 3,
    backoff_factor: float = 2.0,
) -> dict:
    """Obtiene datos con reintentos automáticos."""
    for attempt in range(max_retries):
        try:
            return fetch_data(dataset_type, rows)

        except Timeout:
            logger.warning(f"Timeout en intento {attempt + 1}/{max_retries}")
        # Manejo de errores HTTP
        except HTTPError as e:
            if e.response is not None and e.response.status_code >= 500:
                logger.warning(f"Error del servidor: {e}")
            else:
                logger.error(f"Error del cliente: {e}")
                raise
        # Manejo de errores de conexión
        except RequestException as e:
            logger.warning(f"Error de conexión: {e}")

        if attempt < max_retries - 1:
            wait_time = backoff_factor ** attempt
            logger.info(f"Reintentando en {wait_time} segundos...")
            time.sleep(wait_time)

    raise Exception(f"Falló después de {max_retries} intentos")

# Función para transformar y enriquecer los datos
def transform_data(raw_data: dict) -> pd.DataFrame:
    """Transforma y enriquece los datos."""
    logger.info("Transformando datos...")

    orders = raw_data.get("tables", {}).get("orders", [])
    df = pd.DataFrame(orders)
    # Validación de datos
    if df.empty:
        logger.warning("No hay datos para procesar")
        return df

    if "total" not in df.columns and "total_amount" in df.columns:
        df["total"] = df["total_amount"]

    df["order_date"] = pd.to_datetime(df["order_date"])
    df["total"] = pd.to_numeric(df["total"], errors="coerce")

    df["order_month"] = df["order_date"].dt.to_period("M").astype(str)
    df["order_year"] = df["order_date"].dt.year
    df["is_high_value"] = df["total"] > 100
    df["day_of_week"] = df["order_date"].dt.day_name()

    invalid_totals = df["total"].isna().sum()
    if invalid_totals > 0:
        logger.warning(f"{invalid_totals} ordenes con total invalido")

    logger.info(f"Transformadas {len(df)} ordenes")
    return df

# Función para guardar los datos en formato Parquet
def save_data(df: pd.DataFrame, output_dir: str = "output"):
    """Guarda datos particionados por mes."""
    logger.info(f"Guardando datos en {output_dir}/...")

    os.makedirs(output_dir, exist_ok=True)
    # Particionar por año y mes facilita consultar períodos específicos
    df.to_parquet(
        f"{output_dir}/orders",
        partition_cols=["order_year", "order_month"],
        index=False,
    )
    # Guarda una copia completa para consultas generales
    df.to_parquet(f"{output_dir}/orders_all.parquet", index=False)

    logger.info(f"Guardadas {len(df)} ordenes")
    logger.info(f"Particiones: {df['order_month'].nunique()} meses")

# Función principal del pipeline
def main():
    """Pipeline principal."""
    logger.info("=" * 50)
    logger.info("API Pipeline - Iniciando")
    logger.info("=" * 50)
    
    try:
        raw_data = fetch_data_with_retry(rows=5000)
        df = transform_data(raw_data)

        if df.empty:
            logger.error("No hay datos para guardar")
            return

        save_data(df)

        logger.info("=" * 50)
        logger.info("Pipeline completado exitosamente!")
        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Pipeline falló: {e}")
        raise

# Punto de entrada del script
if __name__ == "__main__":
    main()
