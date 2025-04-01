import os
from datetime import datetime

import polars as pl
import requests
from tqdm import tqdm


def realtime_url() -> str:
    return os.path.join(
        "https://opendata.elia.be/api/explore/v2.1/catalog/datasets/ods086/",
        "exports/json?limit=-1&timezone=UTC&use_labels=false&epsg=4326&refine=region%3A%22Federal%22",
    )


def generate_url_history(dt: datetime) -> str:
    year = dt.year
    month = str(dt.month).zfill(2)
    day = str(dt.day).zfill(2)
    return os.path.join(
        "https://opendata.elia.be/api/explore/v2.1/catalog/datasets/ods031/records",
        f"?limit=-1&refine=datetime%3A%22{year}%2F{month}%2F{day}%22&refine=region%3A%22Federal%22",
    )


def read_elia_wind_data(data: dict, realtime: bool = False) -> pl.DataFrame:
    """Read data from Elia API"""
    actual = "realtime" if realtime else "historical"
    return pl.DataFrame(
        data,
        strict=False,
        schema=pl.Schema([
            ("datetime", pl.String),
            ("resolutioncode", pl.String),
            ("offshoreonshore", pl.String),
            ("region", pl.String),
            ("gridconnectiontype", pl.String),
            (actual, pl.Float64),
            ("mostrecentforecast", pl.Float64),
            ("mostrecentconfidence10", pl.Float64),
            ("mostrecentconfidence90", pl.Float64),
            ("dayahead11hforecast", pl.Float64),
            ("dayahead11hconfidence10", pl.Float64),
            ("dayahead11hconfidence90", pl.Float64),
            ("dayaheadforecast", pl.Float64),
            ("dayaheadconfidence10", pl.Float64),
            ("dayaheadconfidence90", pl.Float64),
            ("weekaheadforecast", pl.Float64),
            ("weekaheadconfidence10", pl.Float64),
            ("weekaheadconfidence90", pl.Float64),
            ("monitoredcapacity", pl.Float64),
            ("loadfactor", pl.Float64),
            ("decrementalbidid", pl.String),
        ]),
    )


def load_realtime_measurements() -> pl.DataFrame:
    """Load realtime data"""
    url = realtime_url()
    result = requests.get(url, timeout=10)
    output_df = (
        read_elia_wind_data(result.json()["results"], realtime=True)
        .with_columns(pl.col("datetime").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%z"))
        .sort("datetime")
        .rename({"realtime": "value"})
        .select([
            "datetime",
            "value",
            "dayahead11hconfidence10",
            "dayahead11hforecast",
            "dayahead11hconfidence90",
        ])
    )
    return output_df


def load_history_measurements(start_date: datetime, end_date: datetime) -> pl.DataFrame:
    """Load historical data over a time range"""
    date_range = list(pl.date_range(start_date, end_date, eager=True, interval="1d"))
    historical_data = []
    for date in tqdm(date_range, desc="Loading historical data"):
        url = generate_url_history(date)
        result = requests.get(url, timeout=10)

        df = read_elia_wind_data(result.json()["results"], realtime=False)
        historical_data.append(df)

    output_df = (
        pl.concat(historical_data, how="vertical")
        .with_columns(pl.col("datetime").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%z"))
        .sort("datetime")
        .rename({"measured": "value"})
        .select([
            "datetime",
            "value",
            "dayahead11hconfidence10",
            "dayahead11hforecast",
            "dayahead11hconfidence90",
        ])
    )
    return output_df
