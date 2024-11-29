import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from influxdb_client import Point
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
    debug: bool = False
    mode: str = "gadgetbridge"

    # Define the scopes needed for Google Drive API
    scopes: list[str] = [
        "https://www.googleapis.com/auth/drive",
    ]


class ActivitySample(BaseModel):
    timestamp: int = Field(alias="TIMESTAMP")
    device_id: int | None = Field(alias="DEVICE_ID", default=None)
    user_id: int | None = Field(alias="USER_ID", default=None)
    raw_intensity: int | None = Field(alias="RAW_INTENSITY", default=None)
    steps: int | None = Field(alias="STEPS", default=None)
    raw_kind: int | None = Field(alias="RAW_KIND", default=None)
    heart_rate: int | None = Field(alias="HEART_RATE", default=None)
    unknown1: Optional[int] = Field(alias="UNKNOWN1", default=None)
    sleep: Optional[int] = Field(alias="SLEEP", default=None)
    deep_sleep: Optional[int] = Field(alias="DEEP_SLEEP", default=None)
    rem_sleep: Optional[int] = Field(alias="REM_SLEEP", default=None)
    stress: Optional[int] = Field(alias="STRESS", default=None)
    spo2: Optional[int] = Field(alias="SPO2", default=None)
    calories: Optional[int] = Field(alias="CALORIES", default=None)

    model_config = ConfigDict(populate_by_name=True)

    @property
    def datetime(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp)

    @property
    def has_valid_heart_rate(self) -> bool:
        return self.heart_rate != 255 and self.heart_rate != 0

    def to_influxdb_points(self, tags: dict[str, str]) -> list[Point]:
        sample = self
        points: list[Point] = []

        if sample.has_valid_heart_rate:
            point = (
                Point("heart_rate")
                .time(sample.datetime)
                .field("heart_rate", value=sample.heart_rate)
            )
            for key, value in tags.items():
                point = point.tag(key, value)
            points.append(point)

        # Steps point
        steps = sample.steps
        if steps and steps > 0:
            point = Point("steps").time(sample.datetime).field("steps", value=steps)
            for key, value in tags.items():
                point = point.tag(key, value)
            points.append(point)

        # Stress point
        if sample.stress:
            stress = sample.stress
            if stress:
                point = (
                    Point("stress").time(sample.datetime).field("steps", value=stress)
                )
                for key, value in tags.items():
                    point = point.tag(key, value)
                points.append(point)

        # SPO2 point
        if sample.spo2:
            spo2 = sample.spo2
            if spo2:
                point = Point("spo2").time(sample.datetime).field("steps", value=spo2)
                for key, value in tags.items():
                    point = point.tag(key, value)
                points.append(point)

        # Raw intensity point
        if sample.raw_intensity:
            point = (
                Point("raw_intensity")
                .time(sample.datetime)
                .field("raw_intensity", value=sample.raw_intensity)
            )
            for key, value in tags.items():
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
                        Point("sleep_state")
                        .time(sample.datetime)
                        .tag("sleep_type", sleep_type)
                        .field("duration", value=value)
                    )
                    for key, value in tags.items():
                        point = point.tag(key, value)
                    points.append(point)

        for point in points:
            logging.debug(f"Point: {point.to_line_protocol()}")

        return points
