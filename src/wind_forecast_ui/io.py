import json

import polars as pl
from loguru import logger

from wind_forecast_ui.elia import load_history_measurements, load_realtime_measurements
from wind_forecast_ui.time import last_day_start_end, next_day_start_end
from wind_forecast_ui.utils import download_forecast_dataframe, load_json, save_json


def update_realtime_data(filepath: str) -> None:
    data = load_json(filepath)
    actual = pl.DataFrame(data["data"]).sort("datetime")

    realtime_data = (
        load_realtime_measurements()
        .sort("datetime")
        .with_columns(pl.col("datetime").dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
        .select(["datetime", "value"])
        .drop_nulls()
    )

    data["data"] = json.loads(
        pl.concat([actual, realtime_data], how="vertical").unique("datetime", keep="last").sort("datetime").write_json()
    )
    save_json(filepath, data)


def update_forecast_file(filepath: str) -> None:
    """Update the forecast file with new data."""
    start, end = next_day_start_end("CET")
    next_day_forecast = (
        download_forecast_dataframe()
        .rename({"valid_time": "datetime", "q10": "ci_lower", "q50": "forecast", "q90": "ci_upper"})
        .filter(pl.col("datetime").is_between(start, end, closed="left"))
        .sort("datetime")
        .with_columns(pl.col("datetime").dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
        .select(["datetime", "ci_lower", "forecast", "ci_upper"])
    )

    data = load_json(filepath)
    historical_forecasts = pl.DataFrame(data["data"]).sort("datetime")

    data["data"] = json.loads(
        pl.concat([historical_forecasts, next_day_forecast], how="vertical")
        .unique("datetime", keep="last")
        .write_json()
    )

    save_json(filepath, data)
    logger.info(f"Forecast data successfully written to {filepath}.")


def update_benchmark_file(filepath: str) -> None:
    """Update the benchmark file with new data."""
    start, end = next_day_start_end("CET")

    next_day_benchmark = (
        load_realtime_measurements()
        .rename({
            "dayahead11hconfidence10": "ci_lower",
            "dayahead11hforecast": "forecast",
            "dayahead11hconfidence90": "ci_upper",
        })
        .filter(pl.col("datetime").is_between(start, end, closed="left"))
        .sort("datetime")
        .with_columns(pl.col("datetime").dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
        .select(["datetime", "ci_lower", "forecast", "ci_upper"])
    )

    data = load_json(filepath)
    historical_benchmark_forecasts = pl.DataFrame(data["data"])

    data["data"] = json.loads(
        pl.concat([historical_benchmark_forecasts, next_day_benchmark], how="vertical")
        .unique("datetime", keep="last")
        .sort("datetime")
        .write_json()
    )

    save_json(filepath, data)
    logger.info(f"Benchmark forecast data successfully written to {filepath}.")


def update_history_benchmark_file(actual_filepath: str, forecast_filepath: str) -> None:
    """Update the benchmark file with new data."""
    start, end = last_day_start_end("CET")

    last_day_benchmark = (
        load_history_measurements(start, end)
        .rename({
            "dayahead11hconfidence10": "ci_lower",
            "dayahead11hforecast": "forecast",
            "dayahead11hconfidence90": "ci_upper",
        })
        .filter(pl.col("datetime").is_between(start, end, closed="left"))
        .sort("datetime")
        .with_columns(pl.col("datetime").dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
        .select(["datetime", "ci_lower", "forecast", "ci_upper"])
    )

    # update the last day forecast
    last_day_forecast = last_day_benchmark.select(["datetime", "ci_lower", "forecast", "ci_upper"]).drop_nulls()

    data = load_json(forecast_filepath)
    historical_benchmark_forecasts = (
        pl.DataFrame(data["data"]).sort("datetime").select(["datetime", "ci_lower", "forecast", "ci_upper"])
    )

    data["data"] = json.loads(
        pl.concat([historical_benchmark_forecasts, last_day_forecast], how="vertical")
        .unique("datetime", keep="last")
        .sort("datetime")
        .write_json()
    )

    save_json(forecast_filepath, data)
    logger.info(f"Forecast data successfully written to {forecast_filepath}.")

    # Update the last day value
    last_day_value = last_day_benchmark.select(["datetime", "value"]).drop_nulls()

    data = load_json(actual_filepath)
    historical_benchmark_value = pl.DataFrame(data["data"]).sort("datetime")
    data["data"] = json.loads(
        pl.concat([historical_benchmark_value, last_day_value], how="vertical")
        .unique("datetime", keep="last")
        .sort("datetime")
        .write_json()
    )

    save_json(actual_filepath, data)
    logger.info(f"Actual data successfully written to {actual_filepath}.")
