from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


@dataclass(frozen=True)
class Settings:
    """Centralised configuration for Supabase + local fallbacks."""

    supabase_url: str = os.getenv("SUPABASE_URL", "")
    supabase_service_key: str = os.getenv("SUPABASE_SERVICE_KEY", "")
    supabase_bucket_raw: str = os.getenv("SUPABASE_BUCKET_RAW", "wa_reports_raw")
    supabase_bucket_clean: str = os.getenv("SUPABASE_BUCKET_CLEAN", "wa_reports_clean")
    supabase_reports_table: str = os.getenv("SUPABASE_REPORTS_TABLE", "reports")
    local_store_dir: Path = Path(os.getenv("LOCAL_DATA_DIR", "data/local_store"))
    sample_file: Path = Path(os.getenv("SAMPLE_DATA_FILE", "data/multichannel-transaction-1763082445610.csv"))

    @property
    def supabase_enabled(self) -> bool:
        return bool(self.supabase_url and self.supabase_service_key)


def get_settings() -> Settings:
    """Helper so Streamlit can cache a single Settings instance."""
    return Settings()
