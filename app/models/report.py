from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import uuid4


def _generate_id() -> str:
    return uuid4().hex


@dataclass
class ReportMetrics:
    total_messages: int
    delivered: int
    failed: int
    read: int
    delivery_rate: float
    read_rate: float
    avg_rate_value: float

    def as_dict(self) -> Dict[str, Any]:
        return {
            "total_messages": self.total_messages,
            "delivered": self.delivered,
            "failed": self.failed,
            "read": self.read,
            "delivery_rate": self.delivery_rate,
            "read_rate": self.read_rate,
            "avg_rate_value": self.avg_rate_value,
        }


@dataclass
class ReportMetadata:
    period_label: str
    source_filename: str
    uploaded_by: str = "system"
    uploaded_at: datetime = field(default_factory=datetime.utcnow)
    report_id: str = field(default_factory=_generate_id)
    raw_storage_path: Optional[str] = None
    clean_storage_path: Optional[str] = None
    notes: Optional[str] = None
    metrics: Optional[ReportMetrics] = None

    def serialize(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "id": self.report_id,
            "period_label": self.period_label,
            "source_filename": self.source_filename,
            "uploaded_by": self.uploaded_by,
            "uploaded_at": self.uploaded_at.isoformat(),
            "raw_storage_path": self.raw_storage_path,
            "clean_storage_path": self.clean_storage_path,
            "notes": self.notes,
        }
        if self.metrics:
            payload["metrics"] = self.metrics.as_dict()
        return payload

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "ReportMetadata":
        metrics = data.get("metrics")
        metrics_obj = ReportMetrics(**metrics) if metrics else None
        uploaded_at = (
            datetime.fromisoformat(data["uploaded_at"])
            if isinstance(data.get("uploaded_at"), str)
            else data.get("uploaded_at", datetime.utcnow())
        )
        return ReportMetadata(
            report_id=data.get("id", data.get("report_id", "")),
            period_label=data.get("period_label", ""),
            source_filename=data.get("source_filename", ""),
            uploaded_by=data.get("uploaded_by", "system"),
            uploaded_at=uploaded_at,
            raw_storage_path=data.get("raw_storage_path"),
            clean_storage_path=data.get("clean_storage_path"),
            notes=data.get("notes"),
            metrics=metrics_obj,
        )
