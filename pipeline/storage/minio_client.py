import io
import logging
import uuid
from datetime import datetime, timezone

import pandas as pd
from minio import Minio
from minio.error import S3Error

from config import config

logger = logging.getLogger(__name__)


class StorageClient:
    def __init__(self):
        self._client = Minio(
            config.minio_endpoint,
            access_key=config.minio_access_key,
            secret_key=config.minio_secret_key,
            secure=config.minio_secure,
        )
        self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        if not self._client.bucket_exists(config.minio_bucket):
            self._client.make_bucket(config.minio_bucket)
            logger.info("Created bucket: %s", config.minio_bucket)

    def write_parquet(self, df: pd.DataFrame, object_path: str) -> None:
        """Serialise *df* to Parquet and upload to MinIO."""
        buf = io.BytesIO()
        df.to_parquet(buf, engine="pyarrow", index=False)
        buf.seek(0)
        size = buf.getbuffer().nbytes

        self._client.put_object(
            config.minio_bucket,
            object_path,
            buf,
            size,
            content_type="application/octet-stream",
        )
        logger.info(
            "Uploaded %d rows → s3://%s/%s",
            len(df),
            config.minio_bucket,
            object_path,
        )

    @staticmethod
    def dated_path(prefix: str, tag: str = "") -> str:
        """
        Build a Hive-style partitioned path so downstream tools (Spark,
        Athena, DuckDB) can prune partitions automatically.

        Example: raw/loans/year=2024/month=04/day=15/143022_abc123.parquet
        """
        now = datetime.now(timezone.utc)
        partition = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
        stem = f"{now.strftime('%H%M%S')}_{tag or uuid.uuid4().hex[:8]}"
        return f"{prefix}/{partition}/{stem}.parquet"


storage = StorageClient()
