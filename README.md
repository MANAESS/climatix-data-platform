# Climatix Data Platform

A weather data pipeline built with Python, Prefect, and PostgreSQL. The
platform extracts hourly weather data for multiple cities, transforms it
into a clean structured format, and loads it into a relational database
for analysis.

## Overview

Climatix Data Platform automates the collection of current weather
conditions for a set of cities (Lille, Toulouse, Paris, Brussels) using
the [WeatherAPI](https://www.weatherapi.com/) service. Data flows through
a classic ETL pipeline — Extract, Transform, Load — orchestrated with
Prefect and persisted in PostgreSQL.

## Tech Stack

| Component | Purpose |
|---|---|
| **Python** | Extraction, transformation, and loading scripts |
| **Prefect** | Workflow orchestration, scheduling, retries, and observability |
| **Docker / docker-compose** | Containerized infrastructure (database, admin UI) |
| **PostgreSQL** | Relational data storage |
| **pgAdmin** | Web-based database administration |
| **httpx** | HTTP client for API requests |
| **python-dotenv** | Environment variable management |

## Project Structure