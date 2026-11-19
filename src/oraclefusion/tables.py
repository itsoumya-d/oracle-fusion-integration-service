from __future__ import annotations

from datetime import UTC, date, datetime

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base


def utcnow() -> datetime:
    return datetime.now(UTC)


class SupplierRow(Base):
    __tablename__ = "suppliers"

    id: Mapped[int] = mapped_column(primary_key=True)
    supplier_number: Mapped[str] = mapped_column(String(60), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(200))
    tax_id: Mapped[str] = mapped_column(String(60), default="")
    status: Mapped[str] = mapped_column(String(20), default="local")
    oracle_id: Mapped[str] = mapped_column(String(120), default="")
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class PurchaseOrderRow(Base):
    __tablename__ = "purchase_orders"

    id: Mapped[int] = mapped_column(primary_key=True)
    po_number: Mapped[str] = mapped_column(String(60), unique=True, index=True)
    supplier_number: Mapped[str] = mapped_column(String(60), index=True)
    amount_cents: Mapped[int] = mapped_column(Integer)
    currency: Mapped[str] = mapped_column(String(3), default="NZD")
    order_date: Mapped[date] = mapped_column(Date)
    status: Mapped[str] = mapped_column(String(20), default="local")
    oracle_id: Mapped[str] = mapped_column(String(120), default="")
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class InvoiceRow(Base):
    __tablename__ = "invoices"

    id: Mapped[int] = mapped_column(primary_key=True)
    invoice_number: Mapped[str] = mapped_column(String(60), unique=True, index=True)
    supplier_number: Mapped[str] = mapped_column(String(60), index=True)
    po_number: Mapped[str] = mapped_column(String(60), default="")
    amount_cents: Mapped[int] = mapped_column(Integer)
    currency: Mapped[str] = mapped_column(String(3), default="NZD")
    invoice_date: Mapped[date] = mapped_column(Date)
    status: Mapped[str] = mapped_column(String(20), default="local")
    oracle_id: Mapped[str] = mapped_column(String(120), default="")
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ReceiptRow(Base):
    __tablename__ = "receipts"

    id: Mapped[int] = mapped_column(primary_key=True)
    receipt_number: Mapped[str] = mapped_column(String(60), unique=True, index=True)
    po_number: Mapped[str] = mapped_column(String(60), index=True)
    quantity: Mapped[int] = mapped_column(Integer)
    received_date: Mapped[date] = mapped_column(Date)
    status: Mapped[str] = mapped_column(String(20), default="local")
    oracle_id: Mapped[str] = mapped_column(String(120), default="")
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class OutboxRow(Base):
    __tablename__ = "outbox"
    __table_args__ = (UniqueConstraint("entity_type", "entity_id", "operation", name="uq_outbox_entity"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    entity_type: Mapped[str] = mapped_column(String(30), index=True)
    entity_id: Mapped[int] = mapped_column(Integer, index=True)
    operation: Mapped[str] = mapped_column(String(30), default="create")
    idempotency_key: Mapped[str] = mapped_column(String(120), unique=True, index=True)
    status: Mapped[str] = mapped_column(String(20), default="pending", index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, default=5)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    last_error: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class AuditRow(Base):
    __tablename__ = "audit_events"

    id: Mapped[int] = mapped_column(primary_key=True)
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    action: Mapped[str] = mapped_column(String(80), index=True)
    actor: Mapped[str] = mapped_column(String(120), default="")
    reference: Mapped[str] = mapped_column(String(120), default="")
    detail: Mapped[str] = mapped_column(Text, default="")


class JobDefinitionRow(Base):
    __tablename__ = "job_definitions"

    id: Mapped[int] = mapped_column(primary_key=True)
    job_code: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(200))
    erp_domain: Mapped[str] = mapped_column(String(60), index=True)
    handler: Mapped[str] = mapped_column(String(120))
    cron_expression: Mapped[str] = mapped_column(String(60), default="")
    timeout_seconds: Mapped[int] = mapped_column(Integer, default=300)
    retry_limit: Mapped[int] = mapped_column(Integer, default=3)
    concurrency_limit: Mapped[int] = mapped_column(Integer, default=5)
    priority: Mapped[int] = mapped_column(Integer, default=2)
    dependencies_json: Mapped[str] = mapped_column(Text, default="[]")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class JobExecutionRow(Base):
    __tablename__ = "job_executions"

    id: Mapped[int] = mapped_column(primary_key=True)
    execution_id: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    job_code: Mapped[str] = mapped_column(String(80), index=True)
    priority: Mapped[int] = mapped_column(Integer, index=True, default=2)
    status: Mapped[str] = mapped_column(String(30), index=True, default="PENDING")
    attempt: Mapped[int] = mapped_column(Integer, default=1)
    max_attempts: Mapped[int] = mapped_column(Integer, default=3)
    worker_id: Mapped[str] = mapped_column(String(100), default="")
    scheduled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_message: Mapped[str] = mapped_column(Text, default="")
    payload_json: Mapped[str] = mapped_column(Text, default="{}")
    result_json: Mapped[str] = mapped_column(Text, default="{}")
    idempotency_key: Mapped[str] = mapped_column(String(120), unique=True, index=True)

