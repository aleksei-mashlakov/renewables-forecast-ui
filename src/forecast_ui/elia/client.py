from datetime import datetime

import polars as pl
import requests
from tqdm import tqdm

from forecast_ui.elia.data_schema import EliaDataset


class EliaAPIClient:
    """Client for interacting with the Elia API."""

    def __init__(self, dataset: EliaDataset):
        self.dataset = dataset

    def load_realtime_measurements(self) -> pl.DataFrame:
        """Load realtime data"""
        result = requests.get(realtime_url(self.dataset), timeout=20)
        df = pl.DataFrame(result.json(), strict=False, schema=self.dataset.value[1])
        output_df = (
            df.with_columns(pl.col("datetime").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%z"))
            .sort("datetime")
            .rename({"realtime": "value"})
        )
        return output_df

    def load_history_measurements(self, start_date: datetime, end_date: datetime) -> pl.DataFrame:
        """Load historical data over a time range"""
        date_range = list(pl.date_range(start_date, end_date, eager=True, interval="1d"))
        historical_data = []
        for date in tqdm(date_range, desc="Loading historical data"):
            url = generate_url_history(date, self.dataset)
            result = requests.get(url, timeout=20)
            df = pl.DataFrame(result.json()["results"], strict=False, schema=self.dataset.value[1])
            historical_data.append(df)

        output_df = (
            pl.concat(historical_data, how="vertical")
            .with_columns(pl.col("datetime").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%z"))
            .sort(by="datetime")
            .rename({"measured": "value"})
        )
        return output_df


def realtime_url(dataset: EliaDataset) -> str:
    """Generate the URL for real-time data from Elia."""
    match dataset:
        case EliaDataset.WIND_REALTIME:
            return (
                f"https://opendata.elia.be/api/explore/v2.1/catalog/datasets/{EliaDataset.WIND_REALTIME.value[0]}/"
                "exports/json?limit=-1&timezone=UTC&refine=offshoreonshore%3A%22Offshore%22"
                "&select=datetime,realtime,dayahead11hforecast,dayahead11hconfidence10,dayahead11hconfidence90,monitoredcapacity,loadfactor,decrementalbidid"
            )
        case EliaDataset.SOLAR_REALTIME:
            return (
                f"https://opendata.elia.be/api/explore/v2.1/catalog/datasets/{EliaDataset.SOLAR_REALTIME.value[0]}/"
                "exports/json?limit=-1&timezone=UTC&refine=region%3A%22Belgium%22"
                "&select=datetime,realtime,dayahead11hforecast,dayahead11hconfidence10,dayahead11hconfidence90,monitoredcapacity,loadfactor"
            )
        case _:
            raise ValueError(f"Unsupported dataset: {dataset}")


def generate_url_history(dt: datetime, dataset: EliaDataset) -> str:
    year = dt.year
    month = str(dt.month).zfill(2)
    day = str(dt.day).zfill(2)
    match dataset:
        case EliaDataset.WIND_HISTORY:
            return (
                f"https://opendata.elia.be/api/explore/v2.1/catalog/datasets/{dataset.value[0]}/records"
                f"?refine=offshoreonshore%3A%22Offshore%22&limit=-1&refine=datetime%3A%22{year}%2F{month}%2F{day}%22"
                "&select=datetime,realtime,dayahead11hforecast,dayahead11hconfidence10,dayahead11hconfidence90,monitoredcapacity,loadfactor,decrementalbidid"
            )
        case EliaDataset.SOLAR_HISTORY:
            return (
                f"https://opendata.elia.be/api/explore/v2.1/catalog/datasets/{dataset.value[0]}/records"
                f"?refine=region%3A%22Belgium%22&limit=-1&refine=datetime%3A%22{year}%2F{month}%2F{day}%22"
                "&select=datetime,measured,dayahead11hforecast,dayahead11hconfidence10,dayahead11hconfidence90,monitoredcapacity,loadfactor"
            )
        case _:
            raise ValueError(f"Unsupported dataset: {dataset}")
