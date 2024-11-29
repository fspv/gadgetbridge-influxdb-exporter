import logging
import sqlite3
from typing import Iterator, Optional

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from pydantic import BaseModel, ConfigDict, Field

import google_drive
import models


class DeviceInfo(BaseModel):
    name: str
    manufacturer: str
    model: Optional[str]
    firmware: Optional[str]

    def to_tags(self) -> dict:
        return {
            "device_name": self.name,
            "device_manufacturer": self.manufacturer,
            "device_model": self.model or "unknown",
            "device_firmware": self.firmware or "unknown",
        }


class Device(BaseModel):
    id: int = Field(alias="_id")
    name: str = Field(alias="NAME")
    manufacturer: str = Field(alias="MANUFACTURER")
    identifier: str = Field(alias="IDENTIFIER")
    type: int = Field(alias="TYPE")
    model: Optional[str] = Field(alias="MODEL")
    alias: Optional[str] = Field(alias="ALIAS")
    parent_folder: Optional[str] = Field(alias="PARENT_FOLDER")

    model_config = ConfigDict(populate_by_name=True)


class DeviceAttributes(BaseModel):
    id: int = Field(alias="_id")
    firmware_version1: str = Field(alias="FIRMWARE_VERSION1")
    firmware_version2: Optional[str] = Field(alias="FIRMWARE_VERSION2")
    valid_from_utc: Optional[int] = Field(alias="VALID_FROM_UTC")
    valid_to_utc: Optional[int] = Field(alias="VALID_TO_UTC")
    device_id: int = Field(alias="DEVICE_ID")
    volatile_identifier: Optional[str] = Field(alias="VOLATILE_IDENTIFIER")

    model_config = ConfigDict(populate_by_name=True)


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

    def get_device_info(self, device_id: int, conn: sqlite3.Connection) -> DeviceInfo:
        """Get device information to use as tags in InfluxDB."""
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT d.NAME, d.MANUFACTURER, d.MODEL,
                   da.FIRMWARE_VERSION1
            FROM DEVICE d
            LEFT JOIN DEVICE_ATTRIBUTES da ON d._id = da.DEVICE_ID
            WHERE d._id = ?
        """,
            (device_id,),
        )
        row = cursor.fetchone()

        if row:
            device_info = DeviceInfo(
                name=row[0], manufacturer=row[1], model=row[2], firmware=row[3]
            )
            logging.debug(f"Device info: {device_info}")
            return device_info

        device_info = DeviceInfo(
            name="unknown", manufacturer="unknown", model=None, firmware=None
        )
        logging.debug(f"Device info: {device_info}")
        return device_info

    def get_huami_activity_samples(
        self, conn: sqlite3.Connection
    ) -> Iterator[models.ActivitySample]:
        """Get all activity samples from the database."""
        conn.row_factory = sqlite3.Row

        # check if table exists
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT name FROM sqlite_master WHERE type='table' AND name='HUAMI_EXTENDED_ACTIVITY_SAMPLE'
        """
        )
        if not cursor.fetchone():
            logging.info("HUAMI_EXTENDED_ACTIVITY_SAMPLE table not found")
            return

        logging.info("HUAMI_EXTENDED_ACTIVITY_SAMPLE table found")

        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM HUAMI_EXTENDED_ACTIVITY_SAMPLE
            ORDER BY TIMESTAMP ASC
        """
        )

        for row in cursor:
            try:
                activity_sample = models.ActivitySample.model_validate(dict(row))
                logging.debug(f"Activity sample: {activity_sample}")
                yield activity_sample
            except Exception as e:
                logging.error(f"Failed to parse activity sample: {e}")
                continue

    def get_xiaomi_activity_samples(
        self, conn: sqlite3.Connection
    ) -> Iterator[models.ActivitySample]:
        """Get all activity samples from the database."""
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # check if table exists
        cursor.execute(
            """
            SELECT name FROM sqlite_master WHERE type='table' AND name='XIAOMI_ACTIVITY_SAMPLE'
        """
        )
        if not cursor.fetchone():
            logging.info("XIAOMI_ACTIVITY_SAMPLE table not found")
            return

        logging.info("XIAOMI_ACTIVITY_SAMPLE table found")

        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM XIAOMI_ACTIVITY_SAMPLE
            ORDER BY TIMESTAMP ASC
        """
        )

        for row in cursor:
            try:
                activity_sample = models.ActivitySample.model_validate(dict(row))
                logging.debug(f"Activity sample: {activity_sample}")
                yield activity_sample
            except Exception as e:
                logging.error(f"Failed to parse activity sample: {e}")
                continue

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
                logging.info("Starting metrics export to InfluxDB")
                with sqlite3.connect(db_path) as conn:
                    # Cache device info to avoid repeated lookups
                    device_cache: dict[int, DeviceInfo] = {}

                    # Create points for each metric
                    points = []

                    for sample in list(self.get_huami_activity_samples(conn)) + list(
                        self.get_xiaomi_activity_samples(conn)
                    ):
                        device_id = sample.device_id
                        if not device_id:
                            logging.error("Sample has no device ID")
                            continue

                        # Get device tags
                        if device_id not in device_cache:
                            device_cache[device_id] = self.get_device_info(
                                device_id, conn
                            )

                        common_tags = {
                            "user_id": str(sample.user_id),
                            "source": "gadgetbridge",
                            **device_cache[device_id].to_tags(),
                        }

                        points.extend(sample.to_influxdb_points(common_tags))

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
