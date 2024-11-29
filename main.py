import logging
import time

import gadgetbridge
import models
import zepp

logging.basicConfig(
    format="%(asctime)s - %(pathname)s:%(lineno)d - %(message)s",
    level=logging.INFO,
)


def main() -> None:
    # Load settings from environment variables (can also use .env file)
    settings = models.Settings()  # type: ignore

    log_level = logging.INFO

    if settings.debug:
        logging.info("Running in debug mode")
        log_level = logging.DEBUG

    logging.getLogger().setLevel(log_level)
    logging.getLogger("influxdb_client").setLevel(log_level)
    logging.getLogger("influxdb_client.client.write_api").setLevel(log_level)
    logging.getLogger("urllib3").setLevel(log_level)

    logging.info(settings)

    # Initialize and run the exporter
    exporter = (
        gadgetbridge.MetricsExporter(settings)
        if settings.mode == "gadgetbridge"
        else zepp.MetricsExporter(settings)
    )
    exporter.export_metrics()

    # run indefinitely
    while settings.daemon:
        logging.info(f"Sleeping for {settings.run_interval} seconds")
        time.sleep(settings.run_interval)
        exporter.export_metrics()


if __name__ == "__main__":
    main()
