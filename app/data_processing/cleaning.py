from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import IO, Dict, Iterable, List, Optional

import pandas as pd


LOCAL_TIMEZONE = "Asia/Jakarta"

DATE_TIME_PAIRS = (
    ("created_date", "created_time", "created_at"),
    ("sent_date", "sent_time", "sent_at"),
    ("delivery_report_date", "delivery_report_time", "delivered_at"),
    ("delivery_report_read_date", "delivery_report_read_time", "read_at"),
)

REQUIRED_COLUMNS: Iterable[str] = [
    "transaction_id",
    "campaign_id",
    "msisdn",
    "status",
    "delivery_report_status",
    "rate",
    "category",
    "template_name",
    "user",
]


def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
    return df


def _normalise_time_component(series: pd.Series) -> pd.Series:
    normalized = series.fillna("").astype(str).str.strip()
    ampm_mask = normalized.str.contains(r"(?i)\b(?:am|pm)\b", na=False)
    if ampm_mask.any():
        ampm_values = normalized.loc[ampm_mask]
        parsed = pd.to_datetime(ampm_values, format="%I:%M:%S %p", errors="coerce")
        needs_alt = parsed.isna()
        if needs_alt.any():
            alt = pd.to_datetime(ampm_values.loc[needs_alt], format="%I:%M %p", errors="coerce")
            parsed = parsed.fillna(alt)
        formatted = parsed.dt.strftime("%H:%M:%S")
        normalized.loc[ampm_mask] = formatted.where(formatted.notna(), ampm_values)
    normalized = normalized.replace("", "00:00:00")
    return normalized


def _build_datetime(df: pd.DataFrame, date_col: str, time_col: str, target_col: str) -> None:
    if date_col not in df.columns:
        return
    date_series = df[date_col].fillna("").astype(str).str.strip()
    if time_col in df.columns:
        time_series = _normalise_time_component(df[time_col])
    else:
        time_series = pd.Series("00:00:00", index=df.index)

    combined = (date_series + " " + time_series).str.strip()
    ts = pd.to_datetime(combined, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    if ts.notna().any():
        ts = ts.dt.tz_localize(LOCAL_TIMEZONE, nonexistent="NaT", ambiguous="NaT")
    df[target_col] = ts


def load_transactions(source: IO[bytes] | Path | str, filename: Optional[str] = None) -> pd.DataFrame:
    """Read CSV/XLSX upload into a DataFrame."""
    if isinstance(source, (str, Path)):
        path = Path(source)
        filename = filename or path.name
        if path.suffix.lower() in (".xls", ".xlsx"):
            df = pd.read_excel(path)
        else:
            df = pd.read_csv(path, sep="|", engine="python")
    else:
        buffer = BytesIO(source.read())  # type: ignore[arg-type]
        suffix = Path(filename or "").suffix.lower()
        if suffix in (".xls", ".xlsx"):
            df = pd.read_excel(buffer)
        else:
            df = pd.read_csv(buffer, sep="|", engine="python")
    return df


def clean_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise column names, timestamps, and numeric fields."""
    df = _normalise_columns(df)

    for date_col, time_col, target in DATE_TIME_PAIRS:
        if date_col in df.columns:
            _build_datetime(df, date_col, time_col, target)

    if "msisdn" in df.columns:
        df["msisdn"] = df["msisdn"].astype(str).str.strip()

    if "rate" in df.columns:
        df["rate_value"] = pd.to_numeric(df["rate"], errors="coerce").fillna(0)
    elif "rate_value" not in df.columns:
        df["rate_value"] = 0

    if "delivery_report_status" in df.columns:
        df["delivery_report_status"] = df["delivery_report_status"].fillna("").str.lower()
    else:
        df["delivery_report_status"] = ""

    if "status" in df.columns:
        df["status"] = df["status"].fillna("").str.lower()
    else:
        df["status"] = ""

    if "category" in df.columns:
        df["category"] = df["category"].fillna("").str.strip()

    df["delivery_status"] = (
        df["delivery_report_status"]
        .where(df["delivery_report_status"].ne(""), df["status"])
        .str.strip()
        .str.lower()
    )

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    return df


def compute_kpis(df: pd.DataFrame) -> Dict[str, float]:
    total = len(df)
    status_series = df.get("status", pd.Series(dtype=str)).fillna("").str.lower()
    delivery_series = df.get("delivery_report_status", pd.Series(dtype=str)).fillna("").str.lower()
    category_series = df.get("category", pd.Series(dtype=str)).fillna("").str.lower()

    marketing_mask = category_series.eq("marketing")
    delivered = int(((status_series == "succeeded") & marketing_mask).sum())
    failed = int((status_series == "failed").sum())
    read = int(((delivery_series == "read") & marketing_mask).sum())
    unread = int(((delivery_series.isin(["delivered", "sent"])) & marketing_mask).sum())
    avg_rate = float(df["rate_value"].mean()) if "rate_value" in df.columns and total else 0.0
    total_cost = float(df["rate_value"].sum()) if "rate_value" in df.columns else 0.0

    return {
        "total": total,
        "delivered": delivered,
        "failed": failed,
        "read": read,
        "unread": unread,
        "delivery_rate": delivered / total if total else 0.0,
        "read_rate": read / total if total else 0.0,
        "avg_rate": avg_rate,
        "total_cost": total_cost,
    }


def read_vs_unread_by_hour(df: pd.DataFrame) -> pd.DataFrame:
    block_starts = list(range(0, 24, 3))
    bucket_counts = pd.Series(0, index=block_starts, dtype="float64")

    if "read_at" in df.columns:
        category_series = df.get("category", pd.Series(dtype=str)).fillna("").str.lower()
        read_subset = df.loc[category_series.eq("marketing")].dropna(subset=["read_at"])
        if not read_subset.empty:
            read_ts = pd.to_datetime(read_subset["read_at"], errors="coerce")
            if pd.api.types.is_datetime64tz_dtype(read_ts):
                read_ts = read_ts.dt.tz_convert(LOCAL_TIMEZONE)
            else:
                read_ts = read_ts.dt.tz_localize("UTC").dt.tz_convert(LOCAL_TIMEZONE)
            hours = read_ts.dt.hour.astype(int)
            hours = hours.apply(lambda h: h + 12 if 1 <= h <= 6 else h)
            buckets = (hours // 3) * 3
            counts = buckets.value_counts().reindex(block_starts, fill_value=0)
            bucket_counts = bucket_counts.add(counts, fill_value=0)

    result = pd.DataFrame({"block": block_starts})
    result["read"] = bucket_counts.sort_index().values
    result["label"] = result["block"].apply(lambda start: f"{start:02d}:00 - {min(start + 3, 24):02d}:00")
    return result


def build_status_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["status", "count"])

    status_series = df.get("status", pd.Series(dtype=str)).fillna("").str.lower()
    delivery_series = df.get("delivery_report_status", pd.Series(dtype=str)).fillna("").str.lower()
    category_series = df.get("category", pd.Series(dtype=str)).fillna("").str.lower()
    marketing_mask = category_series.eq("marketing")

    delivered = int(((status_series == "succeeded") & marketing_mask).sum())
    read = int(((delivery_series == "read") & marketing_mask).sum())
    unread = int(((delivery_series.isin(["delivered", "sent"])) & marketing_mask).sum())
    failed = int((status_series == "failed").sum())

    data = [
        {"status": "delivered", "count": delivered},
        {"status": "read", "count": read},
        {"status": "unread", "count": unread},
        {"status": "failed", "count": failed},
    ]
    return pd.DataFrame(data)


def category_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    if "category" not in df.columns:
        return pd.DataFrame(columns=["category", "total", "delivered", "rate_sum"])

    status_series = df.get("status", pd.Series(dtype=str)).fillna("").str.lower()
    category_series = df.get("category", pd.Series(dtype=str)).fillna("").str.lower()
    delivered_flag = ((status_series == "succeeded") & category_series.eq("marketing")).astype(int)

    working_df = df.assign(delivered_flag=delivered_flag)
    agg = (
        working_df.groupby("category")
        .agg(
            total=("transaction_id", "count"),
            delivered=("delivered_flag", "sum"),
            rate_sum=("rate_value", "sum"),
        )
        .reset_index()
        .sort_values("total", ascending=False)
    )
    return agg


def get_filter_options(df: pd.DataFrame) -> Dict[str, list]:
    def sorted_unique(column: str) -> list:
        if column not in df.columns:
            return []
        return sorted([val for val in df[column].dropna().unique() if str(val).strip()])

    return {
        "templates": sorted_unique("template_name"),
    }


def apply_filters(
    df: pd.DataFrame,
    template: str | None = None,
) -> pd.DataFrame:
    filtered = df.copy()
    if template and "template_name" in filtered.columns:
        filtered = filtered[filtered["template_name"] == template]
    return filtered


def annotate_period_columns(df: pd.DataFrame) -> pd.DataFrame:
    if "sent_at" not in df.columns:
        return df
    result = df.copy()
    ts = pd.to_datetime(result["sent_at"], errors="coerce")
    if pd.api.types.is_datetime64tz_dtype(ts):
        ts = ts.dt.tz_convert("UTC")
    else:
        ts = ts.dt.tz_localize("UTC")

    result["period_year"] = ts.dt.year
    result["period_month"] = ts.dt.month
    result["period_month_label"] = ts.dt.strftime("%B")
    week_series = ((ts.dt.day - 1) // 7 + 1).clip(upper=4)
    result["period_week"] = week_series
    return result


def get_period_options(df: pd.DataFrame) -> Dict[str, list]:
    years = []
    months_by_year: Dict[int, List[int]] = {}
    if "period_year" in df.columns:
        years = sorted(df["period_year"].dropna().astype(int).unique().tolist())
        if "period_month" in df.columns:
            for year in years:
                months = (
                    df.loc[df["period_year"] == year, "period_month"]
                    .dropna()
                    .astype(int)
                    .unique()
                    .tolist()
                )
                months_by_year[year] = sorted(months)
    return {"years": years, "months_by_year": months_by_year}


def apply_period_filters(
    df: pd.DataFrame,
    year: Optional[int] = None,
    month: Optional[int] = None,
    week: Optional[int] = None,
) -> pd.DataFrame:
    filtered = df.copy()
    if year is not None and "period_year" in filtered.columns:
        filtered = filtered[filtered["period_year"] == year]
    if month is not None and "period_month" in filtered.columns:
        filtered = filtered[filtered["period_month"] == month]
    if week is not None and "period_week" in filtered.columns:
        filtered = filtered[filtered["period_week"] == week]
    return filtered
