import json

import polars as pl
from loguru import logger

from wind_forecast_ui.time import next_day_start_end
from wind_forecast_ui.utils import download_dataframe, load_json, save_json


def update_json_forecast_file(filepath: str) -> None:
    """Update the forecast file with new data."""
    start, end = next_day_start_end("CET")
    next_day_forecast = json.loads(
        download_dataframe()
        .rename({"valid_time": "datetime", "q10": "ci_lower", "q50": "forecast", "q90": "ci_upper"})
        .filter(pl.col("datetime").is_between(start, end, closed="left"))
        .sort("datetime")
        .with_columns(pl.col("datetime").dt.strftime("%Y-%m-%d %H:%M:%SZ"))
        .select(["datetime", "ci_lower", "forecast", "ci_upper"])
        .write_json()
    )
    logger.info(next_day_forecast)

    data = load_json(filepath)
    # historical_forecasts = (
    #     pl.DataFrame(data["data"]).sort("datetime").select(["datetime", "ci_lower", "forecast", "ci_upper"])
    # )

    data["data"].extend(next_day_forecast)

    # data["data"] = (
    #     pl.concat([historical_forecasts, next_day_forecast], how="vertical")
    #     .unique("datetime", keep="first")
    #     .write_json()
    # )

    save_json(filepath, data)
    logger.info(data)
    logger.info(f"Forecast data successfully written to {filepath}.")


def update_benchmark_file(filepath: str) -> None:
    """Update the benchmark file with new data."""
    start, end = next_day_start_end("CET")
    next_day_benchmark = json.loads(
        download_dataframe()
        .rename({"valid_time": "datetime", "q10": "ci_lower", "q50": "forecast", "q90": "ci_upper"})
        .filter(pl.col("datetime").is_between(start, end, closed="left"))
        .sort("datetime")
        .with_columns(pl.col("datetime").dt.strftime("%Y-%m-%d %H:%M:%SZ"))
        .select(["datetime", "ci_lower", "forecast", "ci_upper"])
        .write_json()
    )
    logger.info(next_day_benchmark)

    data = load_json(filepath)
    # historical_forecasts = (
    #     pl.DataFrame(data["data"]).sort("datetime").select(["datetime", "ci_lower", "forecast", "ci_upper"])
    # )

    data["data"].extend(next_day_benchmark)

    # data["data"] = (
    #     pl.concat([historical_forecasts, next_day_benchmark], how="vertical")
    #     .unique("datetime", keep="first")
    #     .write_json()
    # )

    save_json(filepath, data)
    logger.info(data)
    logger.info(f"Benchmark data successfully written to {filepath}.")
