from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session, sessionmaker

from .auth import load_api_keys, require_role
from .db import SessionLocal, init_db
from .oracle import OracleFusionClient
from .scheduler import (
    DependencyUnmetError,
    EnterpriseJobScheduler,
    JobDefinition,
    JobNotFoundError,
    SchedulerError,
)
from .service import IntegrationService, NotFoundError, ValidationError


class SupplierIn(BaseModel):
    supplier_number: str
    name: str
    tax_id: str = ""


class PurchaseOrderIn(BaseModel):
    po_number: str
    supplier_number: str
    amount_cents: int
    order_date: date
    currency: str = "NZD"


class InvoiceIn(BaseModel):
    invoice_number: str
    supplier_number: str
    amount_cents: int
    invoice_date: date
    po_number: str = ""
    currency: str = "NZD"


class ReceiptIn(BaseModel):
    receipt_number: str
    po_number: str
    quantity: int
    received_date: date


class JobDefinitionIn(BaseModel):
    job_code: str
    name: str
    erp_domain: str
    handler: str
    cron_expression: str = ""
    timeout_seconds: int = 300
    retry_limit: int = 3
    concurrency_limit: int = 5
    priority: int = 2
    dependencies: list[str] = []
    is_active: bool = True


class JobTriggerIn(BaseModel):
    job_code: str
    payload: dict[str, Any] = {}
    priority: int | None = None
    idempotency_key: str | None = None


class JobHeartbeatIn(BaseModel):
    execution_id: str
    worker_id: str


class JobCompleteIn(BaseModel):
    execution_id: str
    result: dict[str, Any] = {}


class JobFailIn(BaseModel):
    execution_id: str
    error_message: str
    retryable: bool = True


def create_app(session_factory: sessionmaker = SessionLocal, api_keys: dict[str, str] | None = None, client: OracleFusionClient | None = None, init_database: bool = True) -> FastAPI:
    if init_database:
        init_db()
    keys = api_keys or load_api_keys()
    app = FastAPI(title="Oracle Fusion integration service", version="1.0.0")

    def db_session():
        session = session_factory()
        try:
            yield session
        finally:
            session.close()

    viewer = require_role("viewer", keys)
    operator = require_role("operator", keys)

    def handle(exc: Exception):
        if isinstance(exc, NotFoundError):
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/suppliers")
    def supplier(payload: SupplierIn, session: Session = Depends(db_session), actor: str = Depends(operator)) -> dict[str, Any]:
        try:
            row = IntegrationService(session, client, actor).create_supplier(payload.supplier_number, payload.name, payload.tax_id)
        except (NotFoundError, ValidationError) as exc:
            handle(exc)
        return {"id": row.id, "status": row.status}

    @app.post("/purchase-orders")
    def purchase_order(payload: PurchaseOrderIn, session: Session = Depends(db_session), actor: str = Depends(operator)) -> dict[str, Any]:
        try:
            row = IntegrationService(session, client, actor).create_purchase_order(payload.po_number, payload.supplier_number, payload.amount_cents, payload.order_date, payload.currency)
        except (NotFoundError, ValidationError) as exc:
            handle(exc)
        return {"id": row.id, "status": row.status}

    @app.post("/invoices")
    def invoice(payload: InvoiceIn, session: Session = Depends(db_session), actor: str = Depends(operator)) -> dict[str, Any]:
        try:
            row = IntegrationService(session, client, actor).create_invoice(payload.invoice_number, payload.supplier_number, payload.amount_cents, payload.invoice_date, payload.po_number, payload.currency)
        except (NotFoundError, ValidationError) as exc:
            handle(exc)
        return {"id": row.id, "status": row.status}

    @app.post("/receipts")
    def receipt(payload: ReceiptIn, session: Session = Depends(db_session), actor: str = Depends(operator)) -> dict[str, Any]:
        try:
            row = IntegrationService(session, client, actor).create_receipt(payload.receipt_number, payload.po_number, payload.quantity, payload.received_date)
        except (NotFoundError, ValidationError) as exc:
            handle(exc)
        return {"id": row.id, "status": row.status}

    @app.post("/outbox/flush")
    def flush(limit: int = 20, session: Session = Depends(db_session), actor: str = Depends(operator)) -> dict[str, Any]:
        try:
            return IntegrationService(session, client, actor).flush(limit)
        except (NotFoundError, ValidationError) as exc:
            handle(exc)

    @app.get("/summary")
    def summary(session: Session = Depends(db_session), _: str = Depends(viewer)) -> dict[str, Any]:
        return IntegrationService(session).summary()

    @app.get("/audit")
    def audit(session: Session = Depends(db_session), _: str = Depends(viewer)) -> dict[str, Any]:
        return {"audit": IntegrationService(session).audit()}

    @app.post("/jobs/definitions")
    def register_job(payload: JobDefinitionIn, session: Session = Depends(db_session), actor: str = Depends(operator)) -> dict[str, Any]:
        scheduler = EnterpriseJobScheduler(session, actor)
        try:
            defn = JobDefinition(
                job_code=payload.job_code,
                name=payload.name,
                erp_domain=payload.erp_domain,
                handler=payload.handler,
                cron_expression=payload.cron_expression,
                timeout_seconds=payload.timeout_seconds,
                retry_limit=payload.retry_limit,
                concurrency_limit=payload.concurrency_limit,
                priority=payload.priority,
                dependencies=payload.dependencies,
                is_active=payload.is_active,
            )
            row = scheduler.register_job(defn)
            session.commit()
            return {"job_code": row.job_code, "status": "registered"}
        except SchedulerError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.post("/jobs/trigger")
    def trigger_job(payload: JobTriggerIn, session: Session = Depends(db_session), actor: str = Depends(operator)) -> dict[str, Any]:
        scheduler = EnterpriseJobScheduler(session, actor)
        try:
            row = scheduler.trigger_job(
                job_code=payload.job_code,
                payload=payload.payload,
                priority=payload.priority,
                idempotency_key=payload.idempotency_key,
            )
            session.commit()
            return {"execution_id": row.execution_id, "status": row.status, "priority": row.priority}
        except JobNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except (DependencyUnmetError, SchedulerError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.post("/jobs/poll")
    def poll_jobs(worker_id: str, batch_size: int = 5, erp_domain: str | None = None, session: Session = Depends(db_session), _: str = Depends(operator)) -> dict[str, Any]:
        scheduler = EnterpriseJobScheduler(session)
        dispatched = scheduler.poll_and_dispatch(worker_id=worker_id, batch_size=batch_size, erp_domain=erp_domain)
        session.commit()
        return {
            "dispatched": [
                {
                    "execution_id": r.execution_id,
                    "job_code": r.job_code,
                    "priority": r.priority,
                    "attempt": r.attempt,
                }
                for r in dispatched
            ]
        }

    @app.post("/jobs/heartbeat")
    def heartbeat(payload: JobHeartbeatIn, session: Session = Depends(db_session), _: str = Depends(operator)) -> dict[str, Any]:
        scheduler = EnterpriseJobScheduler(session)
        ok = scheduler.record_heartbeat(payload.execution_id, payload.worker_id)
        session.commit()
        if not ok:
            raise HTTPException(status_code=404, detail="Active execution lease not found")
        return {"status": "heartbeat_acknowledged"}

    @app.post("/jobs/reap-stalled")
    def reap_stalled(timeout_seconds: int = 60, session: Session = Depends(db_session), actor: str = Depends(operator)) -> dict[str, Any]:
        scheduler = EnterpriseJobScheduler(session, actor)
        count = scheduler.reap_stalled_executions(timeout_seconds)
        session.commit()
        return {"reaped_count": count}

    @app.post("/jobs/complete")
    def complete_job(payload: JobCompleteIn, session: Session = Depends(db_session), actor: str = Depends(operator)) -> dict[str, Any]:
        scheduler = EnterpriseJobScheduler(session, actor)
        try:
            row = scheduler.complete_job(payload.execution_id, payload.result)
            session.commit()
            return {"execution_id": row.execution_id, "status": row.status, "duration_ms": row.duration_ms}
        except JobNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except SchedulerError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.post("/jobs/fail")
    def fail_job(payload: JobFailIn, session: Session = Depends(db_session), actor: str = Depends(operator)) -> dict[str, Any]:
        scheduler = EnterpriseJobScheduler(session, actor)
        try:
            row = scheduler.fail_job(payload.execution_id, payload.error_message, payload.retryable)
            session.commit()
            return {"execution_id": row.execution_id, "status": row.status, "attempt": row.attempt}
        except JobNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/jobs/metrics")
    def job_metrics(window_minutes: int = 60, session: Session = Depends(db_session), _: str = Depends(viewer)) -> dict[str, Any]:
        scheduler = EnterpriseJobScheduler(session)
        m = scheduler.get_metrics(window_minutes)
        return {
            "total_executions": m.total_executions,
            "completed_count": m.completed_count,
            "failed_count": m.failed_count,
            "dead_letter_count": m.dead_letter_count,
            "active_workers": m.active_workers,
            "p50_duration_ms": m.p50_duration_ms,
            "p95_duration_ms": m.p95_duration_ms,
            "p99_duration_ms": m.p99_duration_ms,
            "throughput_per_minute": m.throughput_per_minute,
        }

    return app


app = create_app()
