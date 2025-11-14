from __future__ import annotations

import json
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from app.config import Settings
from app.models.report import ReportMetadata

try:
    from supabase import Client, create_client
except Exception:  # pragma: no cover - supabase optional during local dev
    Client = None  # type: ignore


class ReportStore:
    """Abstraction that hides Supabase vs local filesystem details."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.supabase_enabled = settings.supabase_enabled and Client is not None
        self.client: Optional[Client] = None
        if self.supabase_enabled:
            self.client = create_client(settings.supabase_url, settings.supabase_service_key)

        self.local_root = settings.local_store_dir
        self.local_raw = self.local_root / "raw"
        self.local_clean = self.local_root / "clean"
        self.local_meta = self.local_root / "metadata.json"
        for folder in (self.local_root, self.local_raw, self.local_clean):
            folder.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    def latest_dataset(self) -> Tuple[pd.DataFrame, Optional[ReportMetadata]]:
        if self.supabase_enabled and self.client:
            record = self._fetch_latest_metadata_supabase()
            if not record:
                raise FileNotFoundError("No report found in Supabase")
            clean_path = record["clean_storage_path"]
            data = self.client.storage.from_(self.settings.supabase_bucket_clean).download(clean_path)
            df = pd.read_parquet(BytesIO(data))
            return df, ReportMetadata.from_dict(record)

        record = self._fetch_latest_metadata_local()
        if not record:
            raise FileNotFoundError(
                "Belum ada dataset lokal. Jalankan scripts/bootstrap_sample.py atau unggah file terlebih dahulu."
            )
        clean_path = self.local_clean / record["clean_storage_path"]
        df = pd.read_parquet(clean_path)
        return df, ReportMetadata.from_dict(record)

    def list_reports(self, limit: int = 12) -> List[ReportMetadata]:
        if self.supabase_enabled and self.client:
            response = (
                self.client.table(self.settings.supabase_reports_table)
                .select("*")
                .order("uploaded_at", desc=True)
                .limit(limit)
                .execute()
            )
            return [ReportMetadata.from_dict(row) for row in response.data]
        metadata = self._read_local_metadata()
        return [ReportMetadata.from_dict(row) for row in metadata[-limit:]][::-1]

    def dataset_by_id(self, report_id: str) -> Tuple[pd.DataFrame, Optional[ReportMetadata]]:
        if self.supabase_enabled and self.client:
            response = (
                self.client.table(self.settings.supabase_reports_table)
                .select("*")
                .eq("id", report_id)
                .limit(1)
                .execute()
            )
            if not response.data:
                raise FileNotFoundError(f"Report {report_id} tidak ditemukan di Supabase")
            record = response.data[0]
            clean_path = record["clean_storage_path"]
            data = self.client.storage.from_(self.settings.supabase_bucket_clean).download(clean_path)
            df = pd.read_parquet(BytesIO(data))
            return df, ReportMetadata.from_dict(record)

        metadata = self._read_local_metadata()
        record = next((row for row in metadata if row.get("id") == report_id), None)
        if not record:
            raise FileNotFoundError(f"Report {report_id} tidak ditemukan di lokal store")
        clean_path = self.local_clean / record["clean_storage_path"]
        df = pd.read_parquet(clean_path)
        return df, ReportMetadata.from_dict(record)

    def persist_report(
        self,
        original_buffer: bytes,
        cleaned_df: pd.DataFrame,
        metadata: ReportMetadata,
    ) -> ReportMetadata:
        record = metadata.serialize()
        if metadata.metrics:
            record["metrics"] = metadata.metrics.as_dict()

        if self.supabase_enabled and self.client:
            raw_path = f"{metadata.report_id}/{metadata.source_filename}"
            clean_path = f"{metadata.report_id}/cleaned.parquet"

            self.client.storage.from_(self.settings.supabase_bucket_raw).upload(
                path=raw_path,
                file=BytesIO(original_buffer),
                file_options={"content-type": "application/octet-stream"},
                upsert=True,
            )
            clean_buffer = BytesIO()
            cleaned_df.to_parquet(clean_buffer, index=False)
            clean_buffer.seek(0)
            self.client.storage.from_(self.settings.supabase_bucket_clean).upload(
                path=clean_path,
                file=clean_buffer,
                file_options={"content-type": "application/octet-stream"},
                upsert=True,
            )
            record["raw_storage_path"] = raw_path
            record["clean_storage_path"] = clean_path
            self.client.table(self.settings.supabase_reports_table).upsert(record).execute()
            return ReportMetadata.from_dict(record)

        raw_filename = f"{metadata.report_id}_{metadata.source_filename}"
        clean_filename = f"{metadata.report_id}_cleaned.parquet"
        (self.local_raw / raw_filename).write_bytes(original_buffer)
        cleaned_df.to_parquet(self.local_clean / clean_filename, index=False)
        record["raw_storage_path"] = raw_filename
        record["clean_storage_path"] = clean_filename
        existing = self._read_local_metadata()
        existing.append(record)
        self.local_meta.write_text(json.dumps(existing, indent=2, default=str))
        return ReportMetadata.from_dict(record)

    # ------------------------------------------------------------------
    # Supabase specific helpers
    # ------------------------------------------------------------------
    def _fetch_latest_metadata_supabase(self) -> Optional[Dict]:
        assert self.client
        response = (
            self.client.table(self.settings.supabase_reports_table)
            .select("*")
            .order("uploaded_at", desc=True)
            .limit(1)
            .execute()
        )
        if not response.data:
            return None
        return response.data[0]

    # ------------------------------------------------------------------
    # Local helpers
    # ------------------------------------------------------------------
    def _read_local_metadata(self) -> List[Dict]:
        if not self.local_meta.exists():
            return []
        return json.loads(self.local_meta.read_text())

    def _fetch_latest_metadata_local(self) -> Optional[Dict]:
        metadata = self._read_local_metadata()
        return metadata[-1] if metadata else None
