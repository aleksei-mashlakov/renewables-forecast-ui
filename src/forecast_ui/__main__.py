import argparse

from loguru import logger

from forecast_ui.io import ForecastConfig, IODataManager


def main(args: dict) -> None:
    """Main function to update the forecast file."""
    for io_manager in [IODataManager(ForecastConfig.WIND), IODataManager(ForecastConfig.SOLAR)]:
        try:
            match args["update"]:
                case "forecast":
                    io_manager.update_forecast_file()
                    io_manager.update_benchmark_file()
                    io_manager.update_history_benchmark_file()

                case "realtime":
                    io_manager.update_realtime_data()
        except Exception as e:
            logger.error(f"Error updating {io_manager._frcst_config.namespace} data: {e}")
            continue
    logger.info("Update completed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--update", help="Type of update", required=True, choices=["forecast", "realtime"])
    args = vars(parser.parse_args())

    main(args)
