import argparse

from wind_forecast_ui.io import (
    update_benchmark_file,
    update_forecast_file,
    update_history_benchmark_file,
    update_realtime_data,
)


def main(args: dict) -> None:
    """Main function to update the forecast file."""
    match args["update"]:
        case "forecast":
            update_forecast_file("./data/forecast.json")
            update_benchmark_file("./data/benchmark.json")
            update_history_benchmark_file(
                actual_filepath="./data/actual.json",
                forecast_filepath="./data/benchmark.json",
            )

        case "realtime":
            update_realtime_data("./data/actual.json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--update", help="Type of update", required=True, choices=["forecast", "realtime"])
    args = vars(parser.parse_args())

    main(args)
