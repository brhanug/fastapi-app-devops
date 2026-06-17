import logging
import os
import sqlite3
import sys
import time
from datetime import datetime

import httpx
from prometheus_client import CollectorRegistry, Counter, Gauge, push_to_gateway
from pydantic import BaseModel, Field, ValidationError

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("weather-etl")

# Configs from environment variables (Secrets/Config management)
API_URL = os.getenv("ETL_API_URL", "https://api.open-meteo.com/v1/forecast")
LATITUDE = os.getenv("ETL_LATITUDE", "50.8503")  # Brussels
LONGITUDE = os.getenv("ETL_LONGITUDE", "4.3517")
DB_PATH = os.getenv("ETL_DB_PATH", "weather_data.db")
PUSHGATEWAY_URL = os.getenv("ETL_PUSHGATEWAY_URL", "")

# Observability metrics definition
registry = CollectorRegistry()

weather_etl_success = Gauge(
    "weather_etl_run_success",
    "Was the last Weather ETL run successful (1) or did it fail (0)",
    registry=registry,
)
weather_etl_duration = Gauge(
    "weather_etl_run_duration_seconds",
    "Duration of the last Weather ETL run in seconds",
    registry=registry,
)
weather_etl_records = Counter(
    "weather_etl_records_inserted_total",
    "Total number of weather records inserted into the database",
    registry=registry,
)
weather_etl_last_success = Gauge(
    "weather_etl_last_success_timestamp_seconds",
    "Epoch timestamp of the last successful Weather ETL run",
    registry=registry,
)


# Pydantic schema for transform/validation
class CurrentWeatherModel(BaseModel):
    time: str
    temperature: float = Field(..., alias="temperature")
    windspeed: float = Field(..., alias="windspeed")
    weathercode: int = Field(..., alias="weathercode")


class ApiResponseModel(BaseModel):
    latitude: float
    longitude: float
    current_weather: CurrentWeatherModel


def init_db(db_path: str):
    """Initialize SQLite database and table securely."""
    logger.info(f"Initializing database at: {db_path}")
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT UNIQUE,
                temperature REAL,
                windspeed REAL,
                weathercode INTEGER,
                ingested_at TEXT
            )
        """)
        conn.commit()
    finally:
        conn.close()


def extract_data(url: str, lat: str, lon: str) -> dict:
    """Fetch weather data from public API using httpx."""
    params = {"latitude": lat, "longitude": lon, "current_weather": "true"}
    logger.info(f"Extracting data from API: {url} for lat={lat}, lon={lon}")

    # Secure HTTP client configuration (timeout included)
    with httpx.Client(timeout=10.0) as client:
        response = client.get(url, params=params)
        response.raise_for_status()
        return response.json()


def transform_data(payload: dict) -> dict:
    """Validate and clean data using Pydantic."""
    logger.info("Transforming and validating API response...")
    try:
        validated_data = ApiResponseModel(**payload)
        current = validated_data.current_weather

        # Clean/Format fields
        transformed = {
            "timestamp": current.time,
            "temperature": current.temperature,
            "windspeed": current.windspeed,
            "weathercode": current.weathercode,
            "ingested_at": datetime.utcnow().isoformat(),
        }
        logger.info(f"Data transformed successfully: {transformed}")
        return transformed
    except ValidationError as e:
        logger.error(f"Data validation failed: {e}")
        raise ValueError("Invalid weather payload structure") from e


def load_data(db_path: str, data: dict) -> bool:
    """Load transformed data into SQLite securely using parameterized queries.
    Returns True if record was inserted, False otherwise.
    """
    logger.info(f"Loading record into database at {db_path}...")
    conn = sqlite3.connect(db_path)
    inserted = False
    try:
        cursor = conn.cursor()
        # Parameterized query to prevent SQL Injection (verified by Bandit)
        query = """
            INSERT OR IGNORE INTO weather_records
            (timestamp, temperature, windspeed, weathercode, ingested_at)
            VALUES (?, ?, ?, ?, ?)
        """
        cursor.execute(
            query,
            (
                data["timestamp"],
                data["temperature"],
                data["windspeed"],
                data["weathercode"],
                data["ingested_at"],
            ),
        )
        conn.commit()
        if cursor.rowcount > 0:
            logger.info("Record loaded successfully into DB.")
            inserted = True
        else:
            logger.info("Duplicate record skipped.")
    except sqlite3.Error as e:
        logger.error(f"Database insertion error: {e}")
        raise
    finally:
        conn.close()
    return inserted


def push_metrics(gateway_url: str, job_name: str = "weather_etl"):
    if not gateway_url:
        logger.info("Prometheus Pushgateway URL not set. Skipping metrics push.")
        return
    logger.info(f"Pushing metrics to Prometheus Pushgateway at {gateway_url}...")
    try:
        push_to_gateway(gateway_url, job=job_name, registry=registry)
        logger.info("Metrics successfully pushed.")
    except Exception as e:
        logger.error(f"Failed to push metrics to Pushgateway: {e}")


def run_etl():
    """Run the end-to-end ETL flow."""
    logger.info("Starting Weather ETL Ingestion Pipeline...")
    start_time = time.time()
    success = 0
    try:
        init_db(DB_PATH)
        raw_payload = extract_data(API_URL, LATITUDE, LONGITUDE)
        transformed_record = transform_data(raw_payload)
        inserted = load_data(DB_PATH, transformed_record)
        if inserted:
            weather_etl_records.inc(1)
        else:
            weather_etl_records.inc(0)
        success = 1
        weather_etl_last_success.set(time.time())
        logger.info("ETL Pipeline completed successfully!")
    except Exception as e:
        logger.critical(f"ETL Pipeline execution failed: {e}", exc_info=True)
        success = 0
        raise
    finally:
        duration = time.time() - start_time
        weather_etl_duration.set(duration)
        weather_etl_success.set(success)
        push_metrics(PUSHGATEWAY_URL)


if __name__ == "__main__":
    run_etl()
