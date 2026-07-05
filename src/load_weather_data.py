import os

import psycopg2
from dotenv import load_dotenv
from prefect import task, get_run_logger

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


@task(retries=2, retry_delay_seconds=3, timeout_seconds=15, log_prints=True)
def task_load_city_data_if_necessary(city_data_to_insert: dict) -> int:
    """Insert the city if it doesn't already exist, and return its id either way."""
    logger = get_run_logger()
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO city (name, region, country, time_zone, latitude, longitude)
                VALUES (%(name)s, %(region)s, %(country)s, %(time_zone)s, %(latitude)s, %(longitude)s)
                ON CONFLICT ON CONSTRAINT city_unique_constraint
                DO UPDATE SET name = EXCLUDED.name
                RETURNING id
                """,
                city_data_to_insert,
            )
            city_id = cur.fetchone()[0]
            conn.commit()
            logger.info(f"City '{city_data_to_insert['name']}' loaded with id={city_id}")
            return city_id
    except Exception as e:
        conn.rollback()
        logger.exception("Could not load city data")
        raise e
    finally:
        conn.close()


@task(retries=2, retry_delay_seconds=3, timeout_seconds=15, log_prints=True)
def task_load_weather_data_if_necessary(weather_data_to_insert: dict, city_id: int):
    """Insert the hourly weather reading, linked to city_id."""
    logger = get_run_logger()
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO current_weather (
                    city_id, date, time, temp_c, feels_like_c, condition_text,
                    wind_speed_kph, wind_speed_mps, wind_degree, wind_dir,
                    pressure_mb, precip_mm, humidity_perc, cloud_perc,
                    uv, gust_kph, is_day
                )
                VALUES (
                    %(city_id)s, %(date)s, %(time)s, %(temp_c)s, %(feels_like_c)s, %(condition_text)s,
                    %(wind_speed_kph)s, %(wind_speed_mps)s, %(wind_degree)s, %(wind_dir)s,
                    %(pressure_mb)s, %(precip_mm)s, %(humidity_perc)s, %(cloud_perc)s,
                    %(uv)s, %(gust_kph)s, %(is_day)s
                )
                ON CONFLICT ON CONSTRAINT weather_unique_constraint
                DO NOTHING
                """,
                {**weather_data_to_insert, "city_id": city_id},
            )
            conn.commit()
            logger.info(f"Weather data loaded for city_id={city_id}")
    except Exception as e:
        conn.rollback()
        logger.exception("Could not load weather data")
        raise e
    finally:
        conn.close()