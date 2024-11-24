import io
import logging
import sqlite3
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional

from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    google_drive_file_id: str
    google_drive_file_all_revisions: bool = False
    credentials_path: Path = Path("credentials.json")
    service_account_path: Path = Path("service-account.json")
    token_path: Path = Path("token.json")
    influxdb_url: str = "http://localhost:8086"
    influxdb_token: str
    influxdb_org: str
    influxdb_bucket: str
    run_interval: int = 3600  # seconds
    daemon: bool = False  # run indefinitely

    # Define the scopes needed for Google Drive API
    scopes: list[str] = ["https://www.googleapis.com/auth/drive.readonly"]


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


class ActivitySample(BaseModel):
    timestamp: int = Field(alias="TIMESTAMP")
    device_id: int = Field(alias="DEVICE_ID")
    user_id: int = Field(alias="USER_ID")
    raw_intensity: int = Field(alias="RAW_INTENSITY")
    steps: int = Field(alias="STEPS")
    raw_kind: int = Field(alias="RAW_KIND")
    heart_rate: int = Field(alias="HEART_RATE")
    unknown1: Optional[int] = Field(alias="UNKNOWN1", default=None)
    sleep: Optional[int] = Field(alias="SLEEP", default=None)
    deep_sleep: Optional[int] = Field(alias="DEEP_SLEEP", default=None)
    rem_sleep: Optional[int] = Field(alias="REM_SLEEP", default=None)
    stress: Optional[int] = Field(alias="STRESS", default=None)
    spo2: Optional[int] = Field(alias="SPO2", default=None)

    model_config = ConfigDict(populate_by_name=True)

    @property
    def datetime(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp)

    @property
    def has_valid_heart_rate(self) -> bool:
        return self.heart_rate != 255 and self.heart_rate != 0


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


class GoogleDriveHandler:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._credentials = None
        self._init_credentials()

    def _init_credentials(self):
        """Initialize Google Drive credentials"""
        if self._settings.service_account_path.exists():
            self._credentials = service_account.Credentials.from_service_account_file(
                str(self._settings.service_account_path),
                scopes=self._settings.scopes,
            )
            return

        if self._settings.token_path.exists():
            self._credentials = Credentials.from_authorized_user_file(
                str(self._settings.token_path), self._settings.scopes
            )

        if not self._credentials or not self._credentials.valid:
            if (
                self._credentials
                and self._credentials.expired
                and self._credentials.refresh_token  # type: ignore
            ):
                self._credentials.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(self._settings.credentials_path), self._settings.scopes
                )
                self._credentials = flow.run_local_server(port=0)

            # Save the credentials for the next run
            with open(self._settings.token_path, "w") as token:
                token.write(
                    self._credentials.to_json(),  # type: ignore
                )

    def download_db_versions(self) -> list[Path]:
        """Download all versions of the database file from Google Drive and return their temporary paths"""
        service = build("drive", "v3", credentials=self._credentials)

        try:
            # Get all versions of the file
            versions = (
                service.revisions()
                .list(
                    fileId=self._settings.google_drive_file_id, fields="revisions(id)"
                )
                .execute()
            )

            if not versions.get("revisions"):
                logging.warning("No versions found for the file")
                return []

            paths = []
            total_versions = len(versions["revisions"])

            # Download each version
            for idx, version in enumerate(versions["revisions"], 1):
                logging.info(f"Downloading version {idx}/{total_versions}")

                # Create a temporary file for this version
                temp_file = tempfile.NamedTemporaryFile(
                    delete=False, suffix=f".v{idx}.db"
                )

                # Get the specific version from Google Drive
                request = service.revisions().get_media(
                    fileId=self._settings.google_drive_file_id, revisionId=version["id"]
                )

                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)

                # Download the file
                done = False
                while done is False:
                    status, done = downloader.next_chunk()
                    if status:
                        logging.info(
                            f"Version {idx}: Downloaded {int(status.progress() * 100)}%"
                        )

                # Save to temporary file
                fh.seek(0)
                with open(temp_file.name, "wb") as f:
                    f.write(fh.read())

                paths.append(Path(temp_file.name))

            return paths

        except Exception as e:
            logging.error(f"Error downloading database versions: {e}")
            raise

    def download_db(self) -> Path:
        """Download the database file from Google Drive and return its temporary path"""
        service = build("drive", "v3", credentials=self._credentials)

        try:
            # Create a temporary file to store the database
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".db")

            # Get the file from Google Drive
            request = service.files().get_media(
                fileId=self._settings.google_drive_file_id
            )
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)

            # Download the file
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                if status:
                    logging.info(f"Download {int(status.progress() * 100)}%")

            # Save to temporary file
            fh.seek(0)
            with open(temp_file.name, "wb") as f:
                f.write(fh.read())

            return Path(temp_file.name)

        except Exception as e:
            logging.error(f"Error downloading database: {e}")
            raise


class WristbandMetricsExporter:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._drive_handler = GoogleDriveHandler(settings)

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
    ) -> Iterator[ActivitySample]:
        """Get all activity samples from the database."""
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM HUAMI_EXTENDED_ACTIVITY_SAMPLE
            ORDER BY TIMESTAMP ASC
        """
        )

        for row in cursor:
            try:
                activity_sample = ActivitySample.model_validate(dict(row))
                logging.debug(f"Activity sample: {activity_sample}")
                yield activity_sample
            except Exception as e:
                logging.error(f"Failed to parse activity sample: {e}")
                continue

    def get_xiaomi_activity_samples(
        self, conn: sqlite3.Connection
    ) -> Iterator[ActivitySample]:
        """Get all activity samples from the database."""
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM XIAOMI_ACTIVITY_SAMPLE
            ORDER BY TIMESTAMP ASC
        """
        )

        for row in cursor:
            try:
                activity_sample = ActivitySample.model_validate(dict(row))
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
                        # Get device tags
                        if sample.device_id not in device_cache:
                            device_cache[sample.device_id] = self.get_device_info(
                                sample.device_id, conn
                            )

                        common_tags = {
                            "user_id": str(sample.user_id),
                            **device_cache[sample.device_id].to_tags(),
                        }

                        # Heart rate point
                        if sample.has_valid_heart_rate:
                            point = (
                                Point("wristband_heart_rate")
                                .time(sample.datetime)
                                .field("heart_rate", value=sample.heart_rate)
                            )
                            for key, value in common_tags.items():
                                point = point.tag(key, value)
                            points.append(point)

                        # Steps point
                        if sample.steps > 0:
                            point = (
                                Point("wristband_steps")
                                .time(sample.datetime)
                                .field("steps", value=sample.steps)
                            )
                            for key, value in common_tags.items():
                                point = point.tag(key, value)
                            points.append(point)

                        # Stress point
                        stress = sample.stress
                        if stress:
                            point = (
                                Point("wristband_stress")
                                .time(sample.datetime)
                                .field("steps", value=stress)
                            )
                            for key, value in common_tags.items():
                                point = point.tag(key, value)
                            points.append(point)

                        # SPO2 point
                        spo2 = sample.spo2
                        if spo2:
                            point = (
                                Point("wristband_spo2")
                                .time(sample.datetime)
                                .field("steps", value=spo2)
                            )
                            for key, value in common_tags.items():
                                point = point.tag(key, value)
                            points.append(point)

                        # Raw intensity point
                        point = (
                            Point("wristband_raw_intensity")
                            .time(sample.datetime)
                            .field("raw_intensity", value=sample.raw_intensity)
                        )
                        for key, value in common_tags.items():
                            point = point.tag(key, value)
                        points.append(point)

                        # Sleep states
                        if sample.sleep is not None:
                            sleep_states = {
                                "light": sample.sleep,  # TODO: check if this is correct
                                "deep": sample.deep_sleep,
                                "rem": sample.rem_sleep,
                            }
                            for sleep_type, value in sleep_states.items():
                                if value is not None:
                                    point = (
                                        Point("wristband_sleep_state")
                                        .time(sample.datetime)
                                        .tag("sleep_type", sleep_type)
                                        .field("duration", value=value)
                                    )
                                    for key, value in common_tags.items():
                                        point = point.tag(key, value)
                                    points.append(point)

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


def main():
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("influxdb_client").setLevel(logging.DEBUG)
    logging.getLogger("influxdb_client.client.write_api").setLevel(logging.DEBUG)
    logging.getLogger("urllib3").setLevel(logging.DEBUG)

    # Load settings from environment variables (can also use .env file)
    settings = Settings()  # type: ignore

    # Initialize and run the exporter
    exporter = WristbandMetricsExporter(settings)

    # run indefinitely
    while settings.daemon:
        exporter.export_metrics()
        logging.info(f"Sleeping for {settings.run_interval} seconds")
        time.sleep(settings.run_interval)


if __name__ == "__main__":
    main()
