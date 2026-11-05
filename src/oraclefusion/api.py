from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session, sessionmaker

from .auth import load_api_keys, require_role
from .db import SessionLocal, init_db
from .oracle import OracleFusionClient
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

    return app


app = create_app()
