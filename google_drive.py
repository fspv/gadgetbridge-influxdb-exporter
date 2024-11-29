import io
import logging
import tempfile
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

import models


class GoogleDriveHandler:
    def __init__(self, settings: models.Settings):
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

    def mark_db_versions_to_be_kept_forever(self) -> list[Path]:
        service = build("drive", "v3", credentials=self._credentials)

        # Get all versions of the file
        versions = (
            service.revisions()
            .list(fileId=self._settings.google_drive_file_id, fields="revisions(id)")
            .execute()
        )

        if not versions.get("revisions"):
            logging.warning("No versions found for the file")
            return []

        for idx, version in enumerate(versions["revisions"], 1):
            # Mark revision as kept forever if not already marked
            if not version.get("keepForever", False):
                service.revisions().update(
                    fileId=self._settings.google_drive_file_id,
                    revisionId=version["id"],
                    body={"keepForever": True},
                ).execute()
                logging.info(f"Version {idx}: Marked as kept forever")

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
                # Mark revision as kept forever if not already marked
                if not version.get("keepForever", False):
                    service.revisions().update(
                        fileId=self._settings.google_drive_file_id,
                        revisionId=version["id"],
                        body={"keepForever": True},
                    ).execute()
                    logging.info(f"Version {idx}: Marked as kept forever")

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
        self.mark_db_versions_to_be_kept_forever()

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
