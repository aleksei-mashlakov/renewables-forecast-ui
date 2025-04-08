import json
import os

import polars as pl
from huggingface_hub import hf_hub_download, login


def download_hf_file(filename: str, repo_id: str, repo_type: str = "dataset") -> str:
    login(token=os.getenv("HF_TOKEN"))
    file = hf_hub_download(repo_id=repo_id, filename=filename, repo_type=repo_type)
    return file


def download_forecast_dataframe() -> pl.DataFrame:
    file = download_hf_file("wind-forecasts.parquet", "rexsovietskiy/wind-forecast", "dataset")
    df = pl.read_parquet(file)
    return df


def load_json(filename: str) -> dict:
    """Load the JSON file from the local filesystem."""
    with open(filename) as f:
        data = json.loads(f.read())
    return data  # type: ignore[no-any-return]


def save_json(filename: str, data: dict) -> None:
    """Save the JSON data to the local filesystem."""
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
