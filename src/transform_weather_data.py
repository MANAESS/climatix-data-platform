import datetime

from prefect import task

# Compass points lookup table (16-point compass rose)
# Note: the API already provides wind_dir as text, but we recompute it
# ourselves from wind_degree as an exercise / to avoid depending only on
# the API's own text field.
COMPASS_POINTS = [
    "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
    "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"
]


@task(retries=2, retry_delay_seconds=2, timeout_seconds=10, log_prints=True)
def task_fill_direct_city_fields(weather_data: dict, city_data_to_insert: dict):
    """Extract fields from the 'location' block -> city table"""
    location = weather_data["location"]
    city_data_to_insert["name"] = location["name"]
    city_data_to_insert["region"] = location["region"]
    city_data_to_insert["country"] = location["country"]
    city_data_to_insert["time_zone"] = location["tz_id"]
    city_data_to_insert["latitude"] = location["lat"]
    city_data_to_insert["longitude"] = location["lon"]


@task(retries=2, retry_delay_seconds=2, timeout_seconds=10, log_prints=True)
def task_fill_direct_weather_fields(weather_data: dict, weather_data_to_insert: dict):
    """Extract simple fields from the 'current' block -> current_weather table
    (anything that doesn't need transformation)"""
    current = weather_data["current"]
    weather_data_to_insert["temp_c"] = current["temp_c"]
    weather_data_to_insert["feels_like_c"] = current["feelslike_c"]
    weather_data_to_insert["condition_text"] = current["condition"]["text"]
    weather_data_to_insert["wind_speed_kph"] = current["wind_kph"]
    weather_data_to_insert["wind_degree"] = current["wind_degree"]
    weather_data_to_insert["pressure_mb"] = current["pressure_mb"]
    weather_data_to_insert["precip_mm"] = current["precip_mm"]
    weather_data_to_insert["humidity_perc"] = current["humidity"]
    weather_data_to_insert["cloud_perc"] = current["cloud"]
    weather_data_to_insert["uv"] = current["uv"]
    weather_data_to_insert["gust_kph"] = current["gust_kph"]
    weather_data_to_insert["is_day"] = bool(current["is_day"])


@task(retries=2, retry_delay_seconds=2, timeout_seconds=10, log_prints=True)
def task_transform_date_time_fields(weather_data: dict, weather_data_to_insert: dict):
    """last_updated ('2026-07-05 10:30') -> separate date + time fields"""
    last_updated = weather_data["current"]["last_updated"]
    dt = datetime.datetime.strptime(last_updated, "%Y-%m-%d %H:%M")
    weather_data_to_insert["date"] = dt.date().isoformat()
    weather_data_to_insert["time"] = dt.time().isoformat()


@task(retries=2, retry_delay_seconds=2, timeout_seconds=10, log_prints=True)
def task_transform_wind_speed_mps(weather_data: dict, weather_data_to_insert: dict):
    """km/h -> m/s (1 km/h = 1/3.6 m/s)"""
    wind_kph = weather_data["current"]["wind_kph"]
    weather_data_to_insert["wind_speed_mps"] = round(wind_kph / 3.6, 2)


@task(retries=2, retry_delay_seconds=2, timeout_seconds=10, log_prints=True)
def task_transform_wind_dir(weather_data: dict, weather_data_to_insert: dict):
    """degrees (0-360) -> cardinal direction from the 16-point compass rose"""
    degree = weather_data["current"]["wind_degree"]
    index = round(degree / 22.5) % 16
    weather_data_to_insert["wind_dir"] = COMPASS_POINTS[index]