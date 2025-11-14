from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import IO, Dict, Iterable, List, Optional, Tuple

import pandas as pd


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
    df = df.rename(columns={"transaction_id": "transaction_id"})
    return df


def _build_datetime(df: pd.DataFrame, date_col: str, time_col: str, target_col: str) -> None:
    if date_col not in df.columns:
        return
    date_series = df[date_col].fillna("").astype(str)
    time_series = df[time_col].fillna("00:00:00").astype(str) if time_col in df.columns else "00:00:00"
    ts = pd.to_datetime(
        (date_series + " " + time_series).str.strip(),
        errors="coerce",
        utc=True,
    )
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
    status_series = df.get("delivery_report_status", pd.Series(dtype=str))
    delivered = int((status_series == "delivered").sum())
    failed = int((status_series == "failed").sum())
    read = int((status_series == "read").sum())
    avg_rate = float(df["rate_value"].mean()) if "rate_value" in df.columns and total else 0.0
    total_cost = float(df["rate_value"].sum()) if "rate_value" in df.columns else 0.0

    return {
        "total": total,
        "delivered": delivered,
        "failed": failed,
        "read": read,
        "delivery_rate": delivered / total if total else 0.0,
        "read_rate": read / total if total else 0.0,
        "avg_rate": avg_rate,
        "total_cost": total_cost,
    }


def build_time_series(df: pd.DataFrame) -> pd.DataFrame:
    if "sent_at" not in df.columns:
        return pd.DataFrame(columns=["date", "count"])
    daily = (
        df.dropna(subset=["sent_at"])
        .assign(date=lambda data: data["sent_at"].dt.tz_convert("UTC").dt.date)
        .groupby("date")
        .size()
        .reset_index(name="count")
    )
    return daily


def build_status_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    column = "delivery_report_status" if "delivery_report_status" in df.columns else "delivery_status"
    counts = (
        df[column]
        .replace("", "unknown")
        .fillna("unknown")
        .value_counts()
        .rename_axis("status")
        .reset_index(name="count")
    )
    return counts


def get_recent_messages(df: pd.DataFrame, limit: int = 20) -> pd.DataFrame:
    if "sent_at" not in df.columns:
        return df.head(limit)
    recent = (
        df.sort_values("sent_at", ascending=False)
        .loc[:, ["msisdn", "template_name", "status", "delivery_status", "sent_at"]]
        .head(limit)
    )
    return recent


def template_performance(df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    if "template_name" not in df.columns:
        return pd.DataFrame(columns=["template_name", "total", "delivery_rate", "read_rate"])
    agg = (
        df.groupby("template_name")
        .agg(
            total=("transaction_id", "count"),
            delivered=("delivery_status", lambda s: (s == "delivered").sum()),
            read=("delivery_status", lambda s: (s == "read").sum()),
        )
        .reset_index()
    )
    agg["delivery_rate"] = (agg["delivered"] / agg["total"]).fillna(0)
    agg["read_rate"] = (agg["read"] / agg["total"]).fillna(0)
    return agg.sort_values(["total", "delivery_rate"], ascending=[False, False]).head(top_n)


def area_performance(df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    if "area" not in df.columns:
        return pd.DataFrame(columns=["area", "total", "delivered"])
    agg = (
        df.groupby("area")
        .agg(
            total=("transaction_id", "count"),
            delivered=("delivery_status", lambda s: (s == "delivered").sum()),
        )
        .reset_index()
    )
    agg["delivery_rate"] = (agg["delivered"] / agg["total"]).fillna(0)
    return agg.sort_values(["total", "delivery_rate"], ascending=[False, False]).head(top_n)


def build_hourly_activity(df: pd.DataFrame) -> pd.DataFrame:
    if "sent_at" not in df.columns:
        return pd.DataFrame(columns=["hour", "count"])
    hourly = (
        df.dropna(subset=["sent_at"])
        .assign(hour=lambda data: data["sent_at"].dt.tz_convert("UTC").dt.hour)
        .groupby("hour")
        .size()
        .reindex(range(0, 24), fill_value=0)
        .reset_index()
        .rename(columns={"index": "hour", 0: "count"})
    )
    hourly["label"] = hourly["hour"].apply(lambda h: f"{h:02d}:00")
    return hourly


def category_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    if "category" not in df.columns:
        return pd.DataFrame(columns=["category", "total", "rate_sum"])
    agg = (
        df.groupby("category")
        .agg(total=("transaction_id", "count"), rate_sum=("rate_value", "sum"))
        .reset_index()
        .sort_values("total", ascending=False)
    )
    return agg


def read_vs_unread_by_hour(df: pd.DataFrame) -> pd.DataFrame:
    hours = pd.DataFrame({"hour": range(24)})
    read_counts = pd.Series(0, index=range(24), dtype="float64")
    unread_counts = pd.Series(0, index=range(24), dtype="float64")

    if "read_at" in df.columns:
        read_subset = df.dropna(subset=["read_at"])
        if not read_subset.empty:
            read_series = (
                read_subset.assign(hour=lambda data: data["read_at"].dt.tz_convert("UTC").dt.hour)
                .groupby("hour")
                .size()
            )
            read_counts = read_counts.add(read_series, fill_value=0)

    if "read_at" in df.columns:
        unread_subset = df[df["read_at"].isna()]
        if "sent_at" in df.columns:
            unread_subset = unread_subset.dropna(subset=["sent_at"])
            if not unread_subset.empty:
                unread_series = (
                    unread_subset.assign(hour=lambda data: data["sent_at"].dt.tz_convert("UTC").dt.hour)
                    .groupby("hour")
                    .size()
                )
                unread_counts = unread_counts.add(unread_series, fill_value=0)

    result = hours.copy()
    result["read"] = read_counts.values
    result["unread"] = unread_counts.values
    result["label"] = result["hour"].apply(lambda h: f"{h:02d}:00")
    return result


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
    ts = result["sent_at"]
    if not pd.api.types.is_datetime64_any_dtype(ts):
        ts = pd.to_datetime(ts, errors="coerce", utc=True)
    elif pd.api.types.is_datetime64tz_dtype(ts):
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
