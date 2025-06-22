import os


class FilePath:
    DATA_PATH = "./data"

    @classmethod
    def hf_filename(cls, prefix: str) -> str:
        return f"{prefix}-forecasts.parquet"

    @classmethod
    def forecast_file(cls, folder: str) -> str:
        return os.path.join(cls.DATA_PATH, folder, "forecast.json")

    @classmethod
    def benchmark_file(cls, folder: str) -> str:
        return os.path.join(cls.DATA_PATH, folder, "benchmark.json")

    @classmethod
    def actual_file(cls, folder: str) -> str:
        return os.path.join(cls.DATA_PATH, folder, "actual.json")
