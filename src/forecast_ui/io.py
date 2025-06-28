import datetime
import json

import polars as pl
from loguru import logger

from forecast_ui.elia import EliaAPIClient, EliaDataset
from forecast_ui.path import FilePath
from forecast_ui.time import last_day_start_end, next_day_start_end
from forecast_ui.utils import JSONUtils, download_forecast_dataframe


class BaseForecastConfig:
    """Base class for forecast configuration."""

    def __init__(self, realtime_dataset: EliaDataset, history_dataset: EliaDataset, namespace: str) -> None:
        self.realtime_dataset: EliaDataset = realtime_dataset
        self.history_dataset: EliaDataset = history_dataset
        self.namespace: str = namespace


class ForecastConfig:
    WIND: BaseForecastConfig = BaseForecastConfig(
        realtime_dataset=EliaDataset.WIND_REALTIME,
        history_dataset=EliaDataset.WIND_HISTORY,
        namespace="wind",
    )
    SOLAR: BaseForecastConfig = BaseForecastConfig(
        realtime_dataset=EliaDataset.SOLAR_REALTIME,
        history_dataset=EliaDataset.SOLAR_HISTORY,
        namespace="solar",
    )


class IODataManager:
    """Class to manage input/output data operations for wind and solar forecasts."""

    def __init__(self, forecast_config: BaseForecastConfig):
        self._frcst_config: BaseForecastConfig = forecast_config

    def update_realtime_data(self) -> None:
        """Update the realtime data file with new measurements."""
        dataset: EliaDataset = self._frcst_config.realtime_dataset
        realtime_data = (
            EliaAPIClient(dataset)
            .load_realtime_measurements()
            .sort("datetime")
            .with_columns(pl.col("datetime").dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
            .select(["datetime", "value"])
            .drop_nulls()
        )

        filepath = FilePath.actual_file(self._frcst_config.namespace)
        data = JSONUtils.load_json(filepath)
        data["data"] = json.loads(
            pl.concat([convert_json_to_actual_dataframe(data), realtime_data], how="vertical")
            .unique("datetime", keep="last")
            .sort("datetime")
            .write_json(),
        )
        JSONUtils.save_json(filepath, data)

    def update_forecast_file(self) -> None:
        """Update the forecast file with new data."""
        start, end = next_day_start_end("CET")
        hf_filepath = FilePath.hf_filename(self._frcst_config.namespace)
        next_day_forecast = (
            download_forecast_dataframe(hf_filepath)
            .rename({"valid_time": "datetime", "q10": "ci_lower", "q50": "forecast", "q90": "ci_upper"})
            .filter(pl.col("datetime").is_between(start, end, closed="left"))
            .sort("datetime")
            .with_columns(pl.col("datetime").dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
            .select(["datetime", "ci_lower", "forecast", "ci_upper"])
        )

        filepath = FilePath.forecast_file(self._frcst_config.namespace)
        data = JSONUtils.load_json(filepath)
        data["data"] = json.loads(
            pl.concat([convert_json_to_forecast_dataframe(data), next_day_forecast], how="vertical")
            .unique("datetime", keep="last")
            .sort("datetime")
            .write_json()
        )
        data["metadata"]["last_update"] = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

        JSONUtils.save_json(filepath, data)
        logger.info(f"Forecast data successfully written to {filepath}.")

    def update_benchmark_file(self) -> None:
        """Update the benchmark file with new data. This is called after the forecast file is updated."""
        start, end = next_day_start_end("CET")
        dataset: EliaDataset = self._frcst_config.realtime_dataset
        next_day_benchmark = (
            EliaAPIClient(dataset)
            .load_realtime_measurements()
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

        filepath = FilePath.benchmark_file(self._frcst_config.namespace)
        data = JSONUtils.load_json(filepath)
        data["data"] = json.loads(
            pl.concat([convert_json_to_forecast_dataframe(data), next_day_benchmark], how="vertical")
            .unique("datetime", keep="last")
            .sort("datetime")
            .write_json()
        )
        JSONUtils.save_json(filepath, data)
        logger.info(f"Benchmark forecast data successfully written to {filepath}.")

    def update_history_benchmark_file(
        self, start: datetime.datetime | None = None, end: datetime.datetime | None = None
    ) -> None:
        """Update the history benchmark file with new data."""
        if start is None or end is None:
            start, end = last_day_start_end("CET")

        dataset: EliaDataset = self._frcst_config.history_dataset
        history_benchmark = (
            EliaAPIClient(dataset)
            .load_history_measurements(start, end)
            .rename({
                "dayahead11hconfidence10": "ci_lower",
                "dayahead11hforecast": "forecast",
                "dayahead11hconfidence90": "ci_upper",
            })
            .filter(pl.col("datetime").is_between(start, end, closed="left"))
            .sort("datetime")
            .with_columns(pl.col("datetime").dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
            .select(["datetime", "ci_lower", "forecast", "ci_upper", "value"])
        )

        filepath = FilePath.benchmark_file(self._frcst_config.namespace)
        data = JSONUtils.load_json(filepath)
        data["data"] = json.loads(
            pl.concat(
                [
                    convert_json_to_forecast_dataframe(data),
                    history_benchmark.select(["datetime", "ci_lower", "forecast", "ci_upper"]).drop_nulls(),
                ],
                how="vertical",
            )
            .unique("datetime", keep="last")
            .sort("datetime")
            .write_json()
        )

        JSONUtils.save_json(filepath, data)
        logger.info(f"Forecast data successfully written to {filepath}.")

        filepath = FilePath.actual_file(self._frcst_config.namespace)
        data = JSONUtils.load_json(filepath)
        data["data"] = json.loads(
            pl.concat(
                [
                    convert_json_to_actual_dataframe(data),
                    history_benchmark.select(["datetime", "value"]).drop_nulls(),
                ],
                how="vertical",
            )
            .unique("datetime", keep="last")
            .sort("datetime")
            .write_json()
        )

        JSONUtils.save_json(filepath, data)
        logger.info(f"Actual data successfully written to {filepath}.")


def convert_json_to_forecast_dataframe(data: dict) -> pl.DataFrame:
    if data["data"]:
        return pl.DataFrame(data["data"]).select(["datetime", "ci_lower", "forecast", "ci_upper"])
    return pl.DataFrame(
        schema={"datetime": pl.String, "ci_lower": pl.Float64, "ci_upper": pl.Float64, "forecast": pl.Float64}
    )


def convert_json_to_actual_dataframe(data: dict) -> pl.DataFrame:
    if data["data"]:
        return pl.DataFrame(data["data"]).sort("datetime").select(["datetime", "value"])
    return pl.DataFrame(schema={"datetime": pl.String, "value": pl.Float64})
