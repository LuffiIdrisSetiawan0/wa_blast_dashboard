from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from app.config import get_settings
from app.data_processing.cleaning import clean_transactions, compute_kpis, load_transactions
from app.models.report import ReportMetadata, ReportMetrics
from app.services.supabase_service import ReportStore


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed local storage dengan sample laporan.")
    parser.add_argument(
        "--file",
        type=str,
        default=None,
        help="Path ke file CSV/XLSX sample. Default: data/multichannel-transaction-*.csv",
    )
    parser.add_argument("--period", type=str, default=datetime.utcnow().strftime("%Y-%m"), help="Label periode laporan.")
    args = parser.parse_args()

    load_dotenv()
    settings = get_settings()
    store = ReportStore(settings)
    sample_path = Path(args.file or settings.sample_file)
    if not sample_path.exists():
        raise FileNotFoundError(f"Sample file {sample_path} tidak ditemukan.")

    raw_df = load_transactions(sample_path)
    cleaned_df = clean_transactions(raw_df)
    kpis = compute_kpis(cleaned_df)
    metrics = ReportMetrics(
        total_messages=kpis["total"],
        delivered=kpis["delivered"],
        failed=kpis["failed"],
        read=kpis["read"],
        delivery_rate=kpis["delivery_rate"],
        read_rate=kpis["read_rate"],
        avg_rate_value=kpis["avg_rate"],
    )
    metadata = ReportMetadata(
        period_label=args.period,
        source_filename=sample_path.name,
        uploaded_by="bootstrap-script",
        metrics=metrics,
    )
    store.persist_report(sample_path.read_bytes(), cleaned_df, metadata)
    print(f"Dataset sample {args.period} selesai disimpan.")


if __name__ == "__main__":
    main()
