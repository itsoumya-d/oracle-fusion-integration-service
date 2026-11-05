from __future__ import annotations

from datetime import date

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from oraclefusion.api import create_app
from oraclefusion.db import Base
from oraclefusion.oracle import OracleConfig, OracleFusionClient
from oraclefusion.service import IntegrationService, NotFoundError, ValidationError
from oraclefusion.tables import OutboxRow

API_KEYS = {"admin-key": "admin", "operator-key": "operator", "viewer-key": "viewer"}
DAY = date(2026, 3, 2)


@pytest.fixture()
def session():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    db = factory()
    yield db
    db.close()


def make_client(status: int = 200, calls: list | None = None) -> OracleFusionClient:
    calls = calls if calls is not None else []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if request.url.path == "/oauth/token":
            return httpx.Response(200, json={"access_token": "token-1", "expires_in": 3600})
        if status >= 400:
            return httpx.Response(status, json={"error": "boom"})
        path = request.url.path
        ids = {
            "/fscmRestApi/resources/latest/suppliers": {"SupplierId": "SUP-ORACLE-1"},
            "/fscmRestApi/resources/latest/purchaseOrders": {"OrderId": "PO-ORACLE-1"},
            "/fscmRestApi/resources/latest/invoices": {"InvoiceId": "INV-ORACLE-1"},
            "/fscmRestApi/resources/latest/receivingReceipts": {"ReceiptId": "RCV-ORACLE-1"},
        }
        return httpx.Response(200, json=ids.get(path, {"id": "X"}))

    return OracleFusionClient(OracleConfig("https://oracle.example.com", "https://oracle.example.com/oauth/token", "id", "secret"), transport=httpx.MockTransport(handler))


def seed(service: IntegrationService):
    supplier = service.create_supplier("SUP-1", "Acme", "123-456")
    po = service.create_purchase_order("PO-1", "SUP-1", 100000, DAY)
    invoice = service.create_invoice("INV-1", "SUP-1", 100000, DAY, "PO-1")
    receipt = service.create_receipt("RCV-1", "PO-1", 10, DAY)
    return supplier, po, invoice, receipt


def test_create_supplier(session):
    row = IntegrationService(session).create_supplier("SUP-1", "Acme")
    assert row.status == "local"


def test_duplicate_supplier(session):
    service = IntegrationService(session)
    service.create_supplier("SUP-1", "Acme")
    with pytest.raises(ValidationError):
        service.create_supplier("SUP-1", "Again")


def test_create_purchase_order(session):
    service = IntegrationService(session)
    service.create_supplier("SUP-1", "Acme")
    row = service.create_purchase_order("PO-1", "SUP-1", 100000, DAY)
    assert row.amount_cents == 100000


def test_po_requires_supplier(session):
    with pytest.raises(NotFoundError):
        IntegrationService(session).create_purchase_order("PO-1", "NOPE", 100, DAY)


def test_create_invoice(session):
    service = IntegrationService(session)
    seed(service)
    assert service.summary()["invoices"] == 1


def test_invoice_requires_po(session):
    service = IntegrationService(session)
    service.create_supplier("SUP-1", "Acme")
    with pytest.raises(NotFoundError):
        service.create_invoice("INV-1", "SUP-1", 100, DAY, "NOPE")


def test_create_receipt(session):
    service = IntegrationService(session)
    seed(service)
    assert service.summary()["receipts"] == 1


def test_outbox_created_for_each_entity(session):
    service = IntegrationService(session)
    seed(service)
    rows = session.query(OutboxRow).all()
    assert len(rows) == 4
    assert all(r.status == "pending" for r in rows)


def test_token_is_cached(session):
    calls = []
    client = make_client(calls=calls)
    service = IntegrationService(session, client)
    service.create_supplier("SUP-1", "Acme")
    service.create_purchase_order("PO-1", "SUP-1", 100, DAY)
    service.flush()
    service.flush()
    assert calls.count("https://oracle.example.com/oauth/token") == 1


def test_flush_syncs_entities(session):
    client = make_client()
    service = IntegrationService(session, client)
    supplier, po, invoice, receipt = seed(service)
    result = service.flush()
    assert result == {"processed": 4, "sent": 4, "failed": 0}
    assert supplier.status == "synced"
    assert supplier.oracle_id == "SUP-ORACLE-1"
    assert po.oracle_id == "PO-ORACLE-1"


def test_flush_marks_failed(session):
    service = IntegrationService(session, make_client(status=500))
    service.create_supplier("SUP-1", "Acme")
    result = service.flush()
    assert result["failed"] == 1
    row = session.query(OutboxRow).first()
    assert row.status == "failed"
    assert row.attempts == 1


def test_flush_dead_letters_after_max_attempts(session):
    service = IntegrationService(session, make_client(status=500))
    service.create_supplier("SUP-1", "Acme")
    row = session.query(OutboxRow).first()
    row.max_attempts = 1
    session.commit()
    service.flush()
    assert row.status == "dead_letter"


def test_flush_requires_client(session):
    service = IntegrationService(session)
    service.create_supplier("SUP-1", "Acme")
    with pytest.raises(ValidationError):
        service.flush()


def test_summary(session):
    service = IntegrationService(session)
    seed(service)
    summary = service.summary()
    assert summary["suppliers"] == 1
    assert summary["purchase_orders"] == 1
    assert summary["outbox"]["pending"] == 4


def test_audit(session):
    service = IntegrationService(session)
    seed(service)
    actions = [e["action"] for e in service.audit()]
    assert "supplier_created" in actions
    assert "invoice_created" in actions


def test_missing_supplier(session):
    with pytest.raises(NotFoundError):
        IntegrationService(session).create_purchase_order("PO-1", "NOPE", 100, DAY)


def test_api_auth_and_workflow(session):
    factory = sessionmaker(bind=session.get_bind(), autoflush=False, expire_on_commit=False)
    client = make_client()
    app = create_app(session_factory=factory, api_keys=API_KEYS, client=client, init_database=False)
    http = TestClient(app)
    assert http.post("/suppliers", json={}).status_code == 401
    headers = {"X-API-Key": "operator-key"}
    http.post("/suppliers", json={"supplier_number": "SUP-1", "name": "Acme"}, headers=headers)
    http.post("/purchase-orders", json={
        "po_number": "PO-1", "supplier_number": "SUP-1", "amount_cents": 100000, "order_date": "2026-03-02",
    }, headers=headers)
    result = http.post("/outbox/flush", headers=headers).json()
    assert result["sent"] == 2
    summary = http.get("/summary", headers={"X-API-Key": "viewer-key"}).json()
    assert summary["outbox"]["sent"] == 2
