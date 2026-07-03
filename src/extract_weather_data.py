# Import required libraries
import httpx
import os

from dotenv import load_dotenv
from prefect import get_run_logger
from prefect import task


# Load environment variables from .env
load_dotenv()
 api_key = os.getenv("WEATHER_API_KEY")

# Base URL and path for the WeatherAPI realtime endpoint
base_url = "https://api.weatherapi.com"
path_url_realtime_api = "/v1/current.json"