# Oracle Fusion Integration Service

Production-oriented integration backend for Oracle Fusion Cloud ERP/SCM:
suppliers, purchase orders, invoices and receiving receipts, with OAuth2 client
credentials, an idempotent outbox, retry/dead-letter handling, reconciliation
state and audit.

## Why this exists

Oracle Fusion integrations fail on the operational edges: expired tokens,
duplicate submissions, partial failures, retries and records that exist locally
but never reached Oracle. This service models the outbound integration path
explicitly instead of hiding it in a one-shot HTTP call.

## Features

- Supplier, purchase order, invoice and receiving-receipt records
- Oracle Fusion REST endpoints under `/fscmRestApi/resources/latest/...`
- OAuth2 client-credentials token acquisition with in-process caching
- Idempotent outbox keyed by business reference
- Bearer auth and `X-Idempotency-Key` on every Oracle write
- Retry scheduling with exponential backoff
- Dead-letter state after configurable max attempts
- Local-to-Oracle sync state and Oracle IDs
- Summary and append-only audit
- API-key RBAC: viewer, operator, admin
- PostgreSQL or SQLite, SQLAlchemy 2, Alembic, Docker, GitHub Actions

## Run locally

```bash
python -m venv .venv
.venv/bin/pip install -e ".[dev]"
DATABASE_URL=sqlite:///./oracle.db .venv/bin/alembic upgrade head
API_KEYS='{"dev-admin":"admin","dev-operator":"operator","dev-viewer":"viewer"}' \
  .venv/bin/uvicorn oraclefusion.api:app --reload
```

Configure credentials in the process environment or inject an
`OracleFusionClient` at startup. No credentials are stored in the repository.

## Test

```bash
.venv/bin/pytest
```

## Honest scope

This is a real, tested integration service, not a production Oracle deployment.
It has not been run against a live Oracle Fusion tenant. The test suite uses an
`httpx.MockTransport` to exercise the real client code, token caching, outbox,
retry and dead-letter paths. Oracle field names and endpoints must be validated
against the target tenant's API version before production use. The README does
not claim production Oracle usage.
