from datetime import datetime
from typing import Literal

import polars as pl
import requests
from tqdm import tqdm


def realtime_url() -> str:
    return (
        "https://opendata.elia.be/api/explore/v2.1/catalog/datasets/ods086/"
        "exports/json?limit=-1&timezone=UTC&refine=offshoreonshore%3A%22Offshore%22"
        "&select=datetime,realtime,dayahead11hforecast,dayahead11hconfidence10,dayahead11hconfidence90,monitoredcapacity,loadfactor,decrementalbidid"
    )


def generate_url_history(dt: datetime) -> str:
    year = dt.year
    month = str(dt.month).zfill(2)
    day = str(dt.day).zfill(2)
    return (
        "https://opendata.elia.be/api/explore/v2.1/catalog/datasets/ods031/records"
        f"?refine=offshoreonshore%3A%22Offshore%22&limit=-1&refine=datetime%3A%22{year}%2F{month}%2F{day}%22"
        "&select=datetime,measured,dayahead11hforecast,dayahead11hconfidence10,dayahead11hconfidence90,monitoredcapacity,loadfactor,decrementalbidid"
    )


def read_elia_wind_data(data: dict, target: Literal["realtime", "measured"]) -> pl.DataFrame:
    """Read data from Elia API"""
    return pl.DataFrame(
        data,
        strict=False,
        schema=pl.Schema([
            ("datetime", pl.String),
            (target, pl.Float64),
            ("dayahead11hforecast", pl.Float64),
            ("dayahead11hconfidence10", pl.Float64),
            ("dayahead11hconfidence90", pl.Float64),
            ("monitoredcapacity", pl.Float64),
            ("loadfactor", pl.Float64),
            ("decrementalbidid", pl.String),
        ]),
    )


def load_realtime_measurements() -> pl.DataFrame:
    """Load realtime data"""
    result = requests.get(realtime_url(), timeout=10)
    df = read_elia_wind_data(result.json(), target="realtime")
    output_df = (
        df.with_columns(pl.col("datetime").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%z"))
        .sort("datetime")
        .rename({"realtime": "value"})
    )
    return output_df


def load_history_measurements(start_date: datetime, end_date: datetime) -> pl.DataFrame:
    """Load historical data over a time range"""
    date_range = list(pl.date_range(start_date, end_date, eager=True, interval="1d"))
    historical_data = []
    for date in tqdm(date_range, desc="Loading historical data"):
        url = generate_url_history(date)
        result = requests.get(url, timeout=10)
        df = read_elia_wind_data(result.json()["results"], target="measured")
        historical_data.append(df)

    output_df = (
        pl.concat(historical_data, how="vertical")
        .with_columns(pl.col("datetime").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%z"))
        .sort("datetime")
        .rename({"measured": "value"})
    )
    return output_df
