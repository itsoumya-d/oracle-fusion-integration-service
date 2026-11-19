from __future__ import annotations

from datetime import date, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .oracle import OracleError, OracleFusionClient
from .tables import (
    AuditRow,
    InvoiceRow,
    OutboxRow,
    PurchaseOrderRow,
    ReceiptRow,
    SupplierRow,
    utcnow,
)


class NotFoundError(Exception):
    pass


class ValidationError(Exception):
    pass


class IntegrationService:
    def __init__(self, session: Session, client: OracleFusionClient | None = None, actor: str = "system") -> None:
        self.session = session
        self.client = client
        self.actor = actor

    def _audit(self, action: str, reference: str, detail: str = "") -> None:
        self.session.add(AuditRow(action=action, actor=self.actor, reference=reference, detail=detail))

    def _enqueue(self, entity_type: str, entity_id: int, key: str) -> OutboxRow:
        existing = self.session.scalar(select(OutboxRow).where(OutboxRow.idempotency_key == key))
        if existing is not None:
            return existing
        row = OutboxRow(entity_type=entity_type, entity_id=entity_id, idempotency_key=key)
        self.session.add(row)
        return row

    def create_supplier(self, supplier_number: str, name: str, tax_id: str = "") -> SupplierRow:
        if not supplier_number.strip() or not name.strip():
            raise ValidationError("supplier number and name are required")
        if self.session.scalar(select(SupplierRow).where(SupplierRow.supplier_number == supplier_number)):
            raise ValidationError("supplier already exists")
        row = SupplierRow(supplier_number=supplier_number, name=name, tax_id=tax_id)
        self.session.add(row)
        self.session.flush()
        self._enqueue("supplier", row.id, f"supplier:{supplier_number}")
        self._audit("supplier_created", supplier_number)
        self.session.commit()
        return row

    def create_purchase_order(self, po_number: str, supplier_number: str, amount_cents: int, order_date: date, currency: str = "NZD") -> PurchaseOrderRow:
        if not po_number.strip() or not supplier_number.strip():
            raise ValidationError("PO number and supplier are required")
        if amount_cents <= 0 or len(currency) != 3:
            raise ValidationError("positive amount and 3-letter currency are required")
        if self.session.scalar(select(SupplierRow).where(SupplierRow.supplier_number == supplier_number)) is None:
            raise NotFoundError("supplier not found")
        if self.session.scalar(select(PurchaseOrderRow).where(PurchaseOrderRow.po_number == po_number)):
            raise ValidationError("purchase order already exists")
        row = PurchaseOrderRow(po_number=po_number, supplier_number=supplier_number, amount_cents=amount_cents, order_date=order_date, currency=currency.upper())
        self.session.add(row)
        self.session.flush()
        self._enqueue("purchase_order", row.id, f"po:{po_number}")
        self._audit("purchase_order_created", po_number)
        self.session.commit()
        return row

    def create_invoice(self, invoice_number: str, supplier_number: str, amount_cents: int, invoice_date: date, po_number: str = "", currency: str = "NZD") -> InvoiceRow:
        if not invoice_number.strip() or not supplier_number.strip():
            raise ValidationError("invoice number and supplier are required")
        if amount_cents <= 0 or len(currency) != 3:
            raise ValidationError("positive amount and 3-letter currency are required")
        if self.session.scalar(select(SupplierRow).where(SupplierRow.supplier_number == supplier_number)) is None:
            raise NotFoundError("supplier not found")
        if po_number and self.session.scalar(select(PurchaseOrderRow).where(PurchaseOrderRow.po_number == po_number)) is None:
            raise NotFoundError("purchase order not found")
        if self.session.scalar(select(InvoiceRow).where(InvoiceRow.invoice_number == invoice_number)):
            raise ValidationError("invoice already exists")
        row = InvoiceRow(invoice_number=invoice_number, supplier_number=supplier_number, po_number=po_number, amount_cents=amount_cents, invoice_date=invoice_date, currency=currency.upper())
        self.session.add(row)
        self.session.flush()
        self._enqueue("invoice", row.id, f"invoice:{invoice_number}")
        self._audit("invoice_created", invoice_number)
        self.session.commit()
        return row

    def create_receipt(self, receipt_number: str, po_number: str, quantity: int, received_date: date) -> ReceiptRow:
        if not receipt_number.strip() or not po_number.strip():
            raise ValidationError("receipt number and PO are required")
        if quantity <= 0:
            raise ValidationError("quantity must be positive")
        if self.session.scalar(select(PurchaseOrderRow).where(PurchaseOrderRow.po_number == po_number)) is None:
            raise NotFoundError("purchase order not found")
        if self.session.scalar(select(ReceiptRow).where(ReceiptRow.receipt_number == receipt_number)):
            raise ValidationError("receipt already exists")
        row = ReceiptRow(receipt_number=receipt_number, po_number=po_number, quantity=quantity, received_date=received_date)
        self.session.add(row)
        self.session.flush()
        self._enqueue("receipt", row.id, f"receipt:{receipt_number}")
        self._audit("receipt_created", receipt_number)
        self.session.commit()
        return row

    def _payload(self, outbox: OutboxRow) -> dict:
        if outbox.entity_type == "supplier":
            row = self.session.get(SupplierRow, outbox.entity_id)
            return {"SupplierNumber": row.supplier_number, "SupplierName": row.name, "TaxRegistrationNumber": row.tax_id}
        if outbox.entity_type == "purchase_order":
            row = self.session.get(PurchaseOrderRow, outbox.entity_id)
            return {"OrderNumber": row.po_number, "Supplier": row.supplier_number, "TransactionAmount": row.amount_cents / 100, "CurrencyCode": row.currency, "OrderDate": row.order_date.isoformat()}
        if outbox.entity_type == "invoice":
            row = self.session.get(InvoiceRow, outbox.entity_id)
            return {"InvoiceNumber": row.invoice_number, "Supplier": row.supplier_number, "PurchaseOrderNumber": row.po_number, "InvoiceAmount": row.amount_cents / 100, "CurrencyCode": row.currency, "InvoiceDate": row.invoice_date.isoformat()}
        if outbox.entity_type == "receipt":
            row = self.session.get(ReceiptRow, outbox.entity_id)
            return {"ReceiptNumber": row.receipt_number, "PurchaseOrderNumber": row.po_number, "Quantity": row.quantity, "ReceivedDate": row.received_date.isoformat()}
        raise ValidationError("unknown outbox entity")

    def _mark_synced(self, outbox: OutboxRow, response: dict) -> None:
        oracle_id = str(response.get("SupplierId") or response.get("OrderId") or response.get("InvoiceId") or response.get("ReceiptId") or response.get("id") or "")
        if outbox.entity_type == "supplier":
            row = self.session.get(SupplierRow, outbox.entity_id)
        elif outbox.entity_type == "purchase_order":
            row = self.session.get(PurchaseOrderRow, outbox.entity_id)
        elif outbox.entity_type == "invoice":
            row = self.session.get(InvoiceRow, outbox.entity_id)
        else:
            row = self.session.get(ReceiptRow, outbox.entity_id)
        row.status = "synced"
        row.oracle_id = oracle_id
        row.last_synced_at = utcnow()

    def flush(self, limit: int = 20) -> dict:
        if self.client is None:
            raise ValidationError("Oracle client is not configured")
        now = utcnow()
        rows = self.session.scalars(select(OutboxRow).where(OutboxRow.status.in_(["pending", "failed"]), OutboxRow.next_attempt_at <= now).order_by(OutboxRow.id).limit(limit)).all()
        sent = 0
        failed = 0
        for outbox in rows:
            outbox.attempts += 1
            try:
                payload = self._payload(outbox)
                if outbox.entity_type == "supplier":
                    response = self.client.create_supplier(payload, outbox.idempotency_key)
                elif outbox.entity_type == "purchase_order":
                    response = self.client.create_purchase_order(payload, outbox.idempotency_key)
                elif outbox.entity_type == "invoice":
                    response = self.client.create_invoice(payload, outbox.idempotency_key)
                else:
                    response = self.client.create_receipt(payload, outbox.idempotency_key)
                self._mark_synced(outbox, response)
                outbox.status = "sent"
                outbox.sent_at = now
                outbox.last_error = ""
                sent += 1
                self._audit("oracle_synced", outbox.entity_type, outbox.idempotency_key)
            except (OracleError, ValidationError) as exc:
                failed += 1
                outbox.last_error = str(exc)
                if outbox.attempts >= outbox.max_attempts:
                    outbox.status = "dead_letter"
                else:
                    outbox.status = "failed"
                    outbox.next_attempt_at = now + timedelta(seconds=min(300, 2 ** outbox.attempts))
                self._audit("oracle_failed", outbox.entity_type, str(exc))
        self.session.commit()
        return {"processed": len(rows), "sent": sent, "failed": failed}

    def summary(self) -> dict:
        outbox = self.session.execute(select(OutboxRow.status, func.count(OutboxRow.id)).group_by(OutboxRow.status)).all()
        return {
            "suppliers": self.session.scalar(select(func.count(SupplierRow.id))) or 0,
            "purchase_orders": self.session.scalar(select(func.count(PurchaseOrderRow.id))) or 0,
            "invoices": self.session.scalar(select(func.count(InvoiceRow.id))) or 0,
            "receipts": self.session.scalar(select(func.count(ReceiptRow.id))) or 0,
            "outbox": {status: count for status, count in outbox},
        }

    def audit(self) -> list[dict]:
        rows = self.session.scalars(select(AuditRow).order_by(AuditRow.id)).all()
        return [{"at": r.at.isoformat(), "action": r.action, "actor": r.actor, "reference": r.reference, "detail": r.detail} for r in rows]
