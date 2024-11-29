import csv
import logging
import tarfile
from datetime import datetime
from io import TextIOWrapper
from pathlib import Path

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

import google_drive
import models


def process_activity_archive(tar_path: Path) -> list[models.ActivitySample]:
    """
    Process a tar.gz archive containing activity-related CSV files from Zepp Life app export.
    """

    samples: list[models.ActivitySample] = []

    # Open the tar archive
    with tarfile.open(tar_path, "r:gz") as tar:
        # Extract and process each file
        for member in tar.getmembers():
            if not member.name.endswith(".csv"):
                continue

            filename = member.name.split("/")[-1]

            logging.info(f"Processing file: {filename}")

            # Open the file within the tar archive
            file = tar.extractfile(member)

            if not file:
                continue

            # Read CSV file
            csv_reader = csv.DictReader(TextIOWrapper(file))

            # Process based on filename
            if filename.startswith("ACTIVITY_MINUTE"):
                # Process steps from minute-level activity file
                for row in csv_reader:
                    logging.debug(row)
                    date_time = datetime.strptime(
                        f"{row['\ufeffdate']} {row['time']}", "%Y-%m-%d %H:%M"
                    )
                    timestamp = int(date_time.timestamp())

                    samples.append(
                        models.ActivitySample(
                            TIMESTAMP=timestamp,
                            STEPS=int(row["steps"]),
                        )
                    )
            elif filename.startswith("ACTIVITY_STAGE_"):
                continue
            elif filename.startswith("ACTIVITY_"):
                # Process calories from activity file
                for row in csv_reader:
                    logging.debug(row)
                    date = datetime.strptime(row["\ufeffdate"], "%Y-%m-%d")
                    timestamp = int(date.timestamp())

                    samples.append(
                        models.ActivitySample(
                            TIMESTAMP=timestamp,
                            CALORIES=int(row["calories"]),
                        )
                    )
            elif filename.startswith("HEARTRATE_AUTO_"):
                # Process heart rate from heart rate file
                for row in csv_reader:
                    logging.debug(row)
                    date_time = datetime.strptime(
                        f"{row['\ufeffdate']} {row['time']}", "%Y-%m-%d %H:%M"
                    )
                    timestamp = int(date_time.timestamp())

                    samples.append(
                        models.ActivitySample(
                            TIMESTAMP=timestamp,
                            HEART_RATE=int(row["heartRate"]),
                        )
                    )

    return samples


class MetricsExporter:
    def __init__(self, settings: models.Settings):
        self._settings = settings
        self._drive_handler = google_drive.GoogleDriveHandler(settings)

        # Initialize InfluxDB client
        self._client = InfluxDBClient(
            url=settings.influxdb_url,
            token=settings.influxdb_token,
            org=settings.influxdb_org,
        )
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)

    def export_metrics(self):
        """Download database and export all activity samples to InfluxDB."""

        logging.info("Starting database versions download")
        db_paths = (
            self._drive_handler.download_db_versions()
            if self._settings.google_drive_file_all_revisions
            else [self._drive_handler.download_db()]
        )

        for db_path in db_paths:
            logging.info(f"processing {db_path}")
            try:
                samples = process_activity_archive(db_path)
                points: list[Point] = []

                for sample in samples:
                    points.extend(sample.to_influxdb_points({"source": "zepp"}))

                # Write batch of points
                logging.info(f"Writing {len(points)} points to InfluxDB")
                self._write_api.write(
                    bucket=self._settings.influxdb_bucket, record=points
                )
                logging.info("Metrics export complete")
            except Exception as e:
                logging.error(f"Error exporting metrics: {e}")
                raise
            finally:
                # Clean up resources
                logging.info("Cleaning up resources")
                self._write_api.close()
                logging.info("InfluxDB write API closed")
                self._client.close()
                logging.info("InfluxDB client closed")
                db_path.unlink()
                logging.info("Temporary database file deleted")
