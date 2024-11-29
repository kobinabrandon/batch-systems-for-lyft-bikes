import pandas as pd 

from datetime import datetime, UTC
from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.setup.paths import PARENT_DIR


_ = load_dotenv(PARENT_DIR.parent.resolve() / ".env")


class GeneralConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file=f"{PARENT_DIR}/.env", env_file_encoding="utf-8", extra="allow")

    # Names 
    year: int = 2024
    n_features: int = 672
    email: str

    # CometML
    comet_api_key: str
    comet_workspace: str

    # Hopsworks
    hopsworks_api_key: str
    hopsworks_project_name: str
    feature_group_version: int = 1
    feature_view_version: int = 1

    # PostgreSQL
    database_public_url: str

    backfill_days: int = 180
    current_hour: datetime = pd.to_datetime(datetime.now(tz=UTC)).floor("H")
    displayed_scenario_names: dict[str, str] = {"start": "Departures", "end": "Arrivals"} 


config = GeneralConfig()
cities = ["bay_area", "chicago", "washington_dc", "new_york", "portland", "columbus"]


def proper_city_name(city_name: str) -> str:
   
    assert city_name in cities

    if "_" not in city_name: 
        return city_name.title()
    elif city_name == "bay_area":
        return "the Bay Area"
    elif city_name == "washington_dc":
        return "Washington DC"
    else:
        return city_name.replace("_", " ").title()

