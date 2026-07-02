CREATE TABLE IF NOT EXISTS city (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    region VARCHAR(100),
    country VARCHAR(100) NOT NULL,
    time_zone VARCHAR(50) NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL
);

ALTER TABLE city
    ADD CONSTRAINT city_unique_constraint
    UNIQUE (name, region, country, time_zone, latitude, longitude);

CREATE TABLE IF NOT EXISTS current_weather (
    id SERIAL PRIMARY KEY,
    city_id INT NOT NULL REFERENCES city(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    time TIME NOT NULL,
    temp_c FLOAT,
    feels_like_c FLOAT,
    condition_text VARCHAR(150),
    wind_speed_kph FLOAT,
    wind_speed_mps FLOAT,
    wind_degree INT,
    wind_dir VARCHAR(5),
    pressure_mb FLOAT,
    precip_mm FLOAT,
    humidity_perc INT,
    cloud_perc INT,
    uv FLOAT,
    gust_kph FLOAT,
    is_day BOOLEAN
);

ALTER TABLE current_weather
    ADD CONSTRAINT weather_unique_constraint
    UNIQUE (city_id, date, time);

CREATE TABLE IF NOT EXISTS daily_weather_analyses (
    id SERIAL PRIMARY KEY,
    city_id INT NOT NULL REFERENCES city(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    max_temp_c FLOAT,
    min_temp_c FLOAT,
    avg_temp_c FLOAT,
    max_wind_speed_kph FLOAT,
    max_wind_speed_mps FLOAT,
    avg_wind_speed_kph FLOAT,
    avg_wind_speed_mps FLOAT,
    total_precip_mm FLOAT,
    avg_humidity_perc INT,
    sunrise TIME,
    sunset TIME,
    moonrise TIME,
    moonset TIME,
    moon_phase VARCHAR(50)
);

ALTER TABLE daily_weather_analyses
    ADD CONSTRAINT daily_weather_unique_constraint
    UNIQUE (city_id, date);
