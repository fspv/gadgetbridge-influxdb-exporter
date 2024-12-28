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


class XiaomiDailySummarySample(BaseModel):
    timestamp: int = Field(alias="TIMESTAMP")
    device_id: int | None = Field(alias="DEVICE_ID", default=None)
    user_id: int | None = Field(alias="USER_ID", default=None)
    steps: int | None = Field(alias="STEPS", default=None)
    heart_rate_resting: int | None = Field(alias="HR_RESTING", default=None)
    heart_rate_max: int | None = Field(alias="HR_MAX", default=None)
    heart_rate_max_timestamp: int | None = Field(alias="HR_MAX_TIMESTAMP", default=None)
    heart_rate_min: int | None = Field(alias="HR_MIN", default=None)
    heart_rate_min_timestamp: int | None = Field(alias="HR_MIN_TIMESTAMP", default=None)
    heart_rate_average: int | None = Field(alias="HR_AVG", default=None)
    stress_max: int | None = Field(alias="STRESS_MAX", default=None)
    stress_min: int | None = Field(alias="STRESS_MIN", default=None)
    stress_average: int | None = Field(alias="STRESS_AVG", default=None)
    calories: int | None = Field(alias="CALORIES", default=None)
    spo2_max: int | None = Field(alias="SPO2_MAX", default=None)
    spo2_max_timestamp: int | None = Field(alias="SPO2_MAX_TS", default=None)
    spo2_min: int | None = Field(alias="SPO2_MIN", default=None)
    spo2_min_timestamp: int | None = Field(alias="SPO2_MIN_TS", default=None)
    spo2_average: int | None = Field(alias="SPO2_AVG", default=None)
    training_load_day: int | None = Field(alias="TRAINING_LOAD_DAY", default=None)
    training_load_week: int | None = Field(alias="TRAINING_LOAD_WEEK", default=None)
    vitality_increase_light: int | None = Field(alias="VITALITY_INCREASE_LIGHT", default=None)
    vitality_increase_moderate: int | None = Field(alias="VITALITY_INCREASE_MODERATE", default=None)
    vitality_increase_high: int | None = Field(alias="VITALITY_INCREASE_HIGH", default=None)
    vitality_current: int | None = Field(alias="VITALITY_CURRENT", default=None)

    def to_influxdb_points(self, tags: dict[str, str]) -> list[Point]:
        sample = self

        point = Point("xiaomi_daily_summary_sample").time(datetime.fromtimestamp(sample.timestamp / 1000))

        for key, value in tags.items():
            point = point.tag(key, value)

        if sample.steps:
            point = point.field("steps", value=sample.steps)

        if sample.heart_rate_resting:
            point = point.field("heart_rate_resting", value=sample.heart_rate_resting)

        if sample.heart_rate_max:
            point = point.field("heart_rate_max", value=sample.heart_rate_max)

        if sample.heart_rate_max_timestamp:
            point = point.field("heart_rate_max_timestamp", value=sample.heart_rate_max_timestamp)

        if sample.heart_rate_min:
            point = point.field("heart_rate_min", value=sample.heart_rate_min)

        if sample.heart_rate_min_timestamp:
            point = point.field("heart_rate_min_timestamp", value=sample.heart_rate_min_timestamp)

        if sample.heart_rate_average:
            point = point.field("heart_rate_average", value=sample.heart_rate_average)

        if sample.stress_max:
            point = point.field("stress_max", value=sample.stress_max)

        if sample.stress_min:
            point = point.field("stress_min", value=sample.stress_min)

        if sample.stress_average:
            point = point.field("stress_average", value=sample.stress_average)

        if sample.calories:
            point = point.field("calories", value=sample.calories)

        if sample.spo2_max:
            point = point.field("spo2_max", value=sample.spo2_max)

        if sample.spo2_max_timestamp:
            point = point.field("spo2_max_timestamp", value=sample.spo2_max_timestamp)

        if sample.spo2_min:
            point = point.field("spo2_min", value=sample.spo2_min)

        if sample.spo2_min_timestamp:
            point = point.field("spo2_min_timestamp", value=sample.spo2_min_timestamp)

        if sample.spo2_average:
            point = point.field("spo2_average", value=sample.spo2_average)

        if sample.training_load_day:
            point = point.field("training_load_day", value=sample.training_load_day)

        if sample.training_load_week:
            point = point.field("training_load_week", value=sample.training_load_week)

        if sample.vitality_increase_light:
            point = point.field("vitality_increase_light", value=sample.vitality_increase_light)

        if sample.vitality_increase_moderate:
            point = point.field("vitality_increase_moderate", value=sample.vitality_increase_moderate)

        if sample.vitality_increase_high:
            point = point.field("vitality_increase_high", value=sample.vitality_increase_high)


        if sample.vitality_current:
            point = point.field("vitality_current", value=sample.vitality_current)

        logging.debug(f"Daily summary sample point: {point.to_line_protocol()}")
        return [point]

class XiaomiSleepStageSample(BaseModel):
    timestamp: int = Field(alias="TIMESTAMP")
    device_id: int | None = Field(alias="DEVICE_ID", default=None)
    user_id: int | None = Field(alias="USER_ID", default=None)
    # https://github.com/Freeyourgadget/Gadgetbridge/blob/master/app/src/main/java/nodomain/freeyourgadget/gadgetbridge/service/devices/xiaomi/activity/impl/SleepStagesParser.java#L115
    stage: int | None = Field(alias="STAGE", default=None)

    def to_influxdb_points(self, tags: dict[str, str]) -> list[Point]:
        sample = self

        point = Point("xiaomi_sleep_stage_sample").time(datetime.fromtimestamp(sample.timestamp / 1000))

        for key, value in tags.items():
            point = point.tag(key, value)

        if sample.stage:
            point = point.field("stage", value=sample.stage)

        logging.debug(f"Sleep stage sample point: {point.to_line_protocol()}")
        return [point]

class XiaomiSleepTimeSample(BaseModel):
    timestamp: int = Field(alias="TIMESTAMP")
    device_id: int | None = Field(alias="DEVICE_ID", default=None)
    user_id: int | None = Field(alias="USER_ID", default=None)
    wakeup_time: int | None = Field(alias="WAKEUP_TIME", default=None)
    is_awake: int | None = Field(alias="IS_AWAKE", default=None)
    # in minutes
    total_duration: int | None = Field(alias="TOTAL_DURATION", default=None)
    # in minutes
    deep_sleep_duration: int | None = Field(alias="DEEP_SLEEP_DURATION", default=None)
    # in minutes
    light_sleep_duration: int | None = Field(alias="LIGHT_SLEEP_DURATION", default=None)
    # in minutes
    rem_sleep_duration: int | None = Field(alias="REM_SLEEP_DURATION", default=None)
    # in minutes
    awake_duration: int | None = Field(alias="AWAKE_DURATION", default=None)

    def to_influxdb_points(self, tags: dict[str, str]) -> list[Point]:
        sample = self

        point = Point("xiaomi_sleep_time_sample").time(datetime.fromtimestamp(sample.timestamp / 1000))

        for key, value in tags.items():
            point = point.tag(key, value)

        if sample.wakeup_time:
            point = point.field("wakeup_time", value=sample.wakeup_time)

        if sample.is_awake:
            point = point.field("is_awake", value=sample.is_awake)

        if sample.total_duration:
            point = point.field("total_duration", value=sample.total_duration)

        if sample.deep_sleep_duration:
            point = point.field("deep_sleep_duration", value=sample.deep_sleep_duration)

        if sample.light_sleep_duration:
            point = point.field("light_sleep_duration", value=sample.light_sleep_duration)

        if sample.rem_sleep_duration:
            point = point.field("rem_sleep_duration", value=sample.rem_sleep_duration)

        if sample.awake_duration:
            point = point.field("awake_duration", value=sample.awake_duration)

        logging.debug(f"Sleep time sample point: {point.to_line_protocol()}")
        return [point]

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
        point = Point("activity_sample").time(sample.datetime)

        for key, value in tags.items():
            point = point.tag(key, value)

        if sample.has_valid_heart_rate:
            heart_rate_point = Point("heart_rate").time(sample.datetime).field("heart_rate", value=sample.heart_rate)
            for key, value in tags.items():
                heart_rate_point = heart_rate_point.tag(key, value)
            points.append(heart_rate_point)
            logging.debug(f"Heart rate point: {heart_rate_point.to_line_protocol()}")
            point = point.field("heart_rate", value=sample.heart_rate)

        # Steps point
        steps = sample.steps
        if steps and steps > 0:
            steps_point = Point("steps").time(sample.datetime).field("steps", value=sample.steps)
            for key, value in tags.items():
                steps_point = steps_point.tag(key, value)
            points.append(steps_point)
            logging.debug(f"Steps point: {steps_point.to_line_protocol()}")
            point = point.field("steps", value=steps)

        # Stress point
        if sample.stress:
            stress = sample.stress
            if stress:
                point = point.field("stress", value=stress)

        # SPO2 point
        if sample.spo2:
            spo2 = sample.spo2
            if spo2:
                point = point.field("spo2", value=spo2)

        # Raw intensity point
        if sample.raw_intensity:
            point = point.field("raw_intensity", value=sample.raw_intensity)

        # Sleep states
        if sample.sleep:
            point = point.field("sleep", value=sample.sleep)

        if sample.deep_sleep:
            point = point.field("deep_sleep", value=sample.deep_sleep)

        if sample.rem_sleep:
            point = point.field("rem_sleep", value=sample.rem_sleep)

        # Kind of activity. Described here: https://github.com/Freeyourgadget/Gadgetbridge/blob/a0948ee1cbc2a870f91d313f8e37df5f524465f7/app/src/main/java/nodomain/freeyourgadget/gadgetbridge/devices/huami/HuamiExtendedSampleProvider.java#L32
        if sample.raw_kind:
            point = point.field("raw_kind", value=sample.raw_kind)

        logging.debug(f"Activity sample point: {point.to_line_protocol()}")
        points.append(point)

        return points
