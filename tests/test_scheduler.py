from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from oraclefusion.api import create_app
from oraclefusion.db import Base
from oraclefusion.scheduler import (
    DependencyUnmetError,
    EnterpriseJobScheduler,
    JobDefinition,
    JobNotFoundError,
    JobPriority,
    JobStatus,
    SchedulerError,
)
from oraclefusion.tables import JobDefinitionRow, JobExecutionRow, OutboxRow

API_KEYS = {"admin-key": "admin", "operator-key": "operator", "viewer-key": "viewer"}


@pytest.fixture()
def session():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    db = factory()
    yield db
    db.close()


@pytest.fixture()
def client(session):
    factory = lambda: session
    app = create_app(session_factory=factory, api_keys=API_KEYS, init_database=False)
    return TestClient(app)


def test_register_job_definition(session):
    scheduler = EnterpriseJobScheduler(session)
    defn = JobDefinition(
        job_code="ERP_SYNC_GL_POSTINGS",
        name="ERP General Ledger Postings Sync",
        erp_domain="finance",
        handler="com.oracle.fusion.gl.SyncService",
        concurrency_limit=3,
        priority=JobPriority.CRITICAL,
    )
    row = scheduler.register_job(defn)
    assert row.job_code == "ERP_SYNC_GL_POSTINGS"
    assert row.priority == 4
    assert row.concurrency_limit == 3

    # Update definition
    defn.name = "Updated GL Sync"
    updated = scheduler.register_job(defn)
    assert updated.name == "Updated GL Sync"

    # Self-dependency error
    defn_invalid = JobDefinition(
        job_code="SELF_DEP",
        name="Self Dep",
        erp_domain="finance",
        handler="com.test",
        dependencies=["SELF_DEP"],
    )
    with pytest.raises(SchedulerError):
        scheduler.register_job(defn_invalid)


def test_trigger_job_and_idempotency(session):
    scheduler = EnterpriseJobScheduler(session)
    scheduler.register_job(
        JobDefinition(
            job_code="BATCH_SETTLEMENT",
            name="Batch Settlement",
            erp_domain="settlement",
            handler="com.oracle.settlement",
        )
    )

    exec1 = scheduler.trigger_job(
        job_code="BATCH_SETTLEMENT",
        payload={"batch_id": "B-101"},
        idempotency_key="idem-settle-101",
    )
    assert exec1.status == JobStatus.PENDING.value
    assert exec1.attempt == 1

    # Outbox message should be queued
    outbox = session.query(OutboxRow).filter_by(entity_type="job_execution").first()
    assert outbox is not None

    # Duplicate trigger with same idempotency key returns exact same execution
    exec2 = scheduler.trigger_job(
        job_code="BATCH_SETTLEMENT",
        payload={"batch_id": "B-101"},
        idempotency_key="idem-settle-101",
    )
    assert exec2.execution_id == exec1.execution_id


def test_dependency_unmet_and_resolution(session):
    scheduler = EnterpriseJobScheduler(session)
    scheduler.register_job(
        JobDefinition(job_code="EXTRACT_ERP_DATA", name="Extract ERP", erp_domain="data", handler="com.extract")
    )
    scheduler.register_job(
        JobDefinition(
            job_code="LOAD_WAREHOUSE",
            name="Load Warehouse",
            erp_domain="data",
            handler="com.load",
            dependencies=["EXTRACT_ERP_DATA"],
        )
    )

    # Triggering child should fail because parent has not completed
    with pytest.raises(DependencyUnmetError):
        scheduler.trigger_job("LOAD_WAREHOUSE")

    # Trigger parent and complete it
    parent_exec = scheduler.trigger_job("EXTRACT_ERP_DATA")
    scheduler.poll_and_dispatch("worker-1", batch_size=5)
    scheduler.complete_job(parent_exec.execution_id, result={"records": 5000})

    # Now child trigger succeeds
    child_exec = scheduler.trigger_job("LOAD_WAREHOUSE")
    assert child_exec.status == JobStatus.PENDING.value


def test_poll_and_dispatch_priority_ordering(session):
    scheduler = EnterpriseJobScheduler(session)
    scheduler.register_job(
        JobDefinition(job_code="JOB_LOW", name="Low Priority", erp_domain="ops", handler="h", priority=JobPriority.LOW)
    )
    scheduler.register_job(
        JobDefinition(job_code="JOB_HIGH", name="High Priority", erp_domain="ops", handler="h", priority=JobPriority.HIGH)
    )
    scheduler.register_job(
        JobDefinition(job_code="JOB_CRIT", name="Critical Priority", erp_domain="ops", handler="h", priority=JobPriority.CRITICAL)
    )

    scheduler.trigger_job("JOB_LOW")
    scheduler.trigger_job("JOB_HIGH")
    scheduler.trigger_job("JOB_CRIT")

    dispatched = scheduler.poll_and_dispatch(worker_id="worker-primary", batch_size=3)
    assert len(dispatched) == 3
    # Critical should be dispatched first, followed by High, then Low
    assert dispatched[0].job_code == "JOB_CRIT"
    assert dispatched[1].job_code == "JOB_HIGH"
    assert dispatched[2].job_code == "JOB_LOW"
    assert dispatched[0].status == JobStatus.RUNNING.value
    assert dispatched[0].worker_id == "worker-primary"


def test_concurrency_limit_enforcement(session):
    scheduler = EnterpriseJobScheduler(session)
    scheduler.register_job(
        JobDefinition(
            job_code="SINGLETON_JOB",
            name="Singleton Job",
            erp_domain="ops",
            handler="h",
            concurrency_limit=1,
        )
    )

    scheduler.trigger_job("SINGLETON_JOB", idempotency_key="k1")
    scheduler.trigger_job("SINGLETON_JOB", idempotency_key="k2")

    dispatched = scheduler.poll_and_dispatch(worker_id="worker-1", batch_size=5)
    # Only 1 execution should be dispatched because concurrency_limit is 1
    assert len(dispatched) == 1
    assert dispatched[0].idempotency_key == "k1"

    # Second poll should return empty while first is RUNNING
    dispatched2 = scheduler.poll_and_dispatch(worker_id="worker-2", batch_size=5)
    assert len(dispatched2) == 0

    # Complete the first execution
    scheduler.complete_job(dispatched[0].execution_id)

    # Now the second execution can be dispatched
    dispatched3 = scheduler.poll_and_dispatch(worker_id="worker-2", batch_size=5)
    assert len(dispatched3) == 1
    assert dispatched3[0].idempotency_key == "k2"


def test_heartbeat_and_stalled_worker_reaping(session):
    scheduler = EnterpriseJobScheduler(session)
    scheduler.register_job(
        JobDefinition(
            job_code="LONG_RUNNING_JOB",
            name="Long Running",
            erp_domain="batch",
            handler="h",
            retry_limit=2,
        )
    )

    scheduler.trigger_job("LONG_RUNNING_JOB")
    dispatched = scheduler.poll_and_dispatch(worker_id="worker-flakey", batch_size=1)
    exec_id = dispatched[0].execution_id

    # Heartbeat works
    assert scheduler.record_heartbeat(exec_id, "worker-flakey") is True
    assert scheduler.record_heartbeat(exec_id, "wrong-worker") is False

    # Simulate expired heartbeat by backdating heartbeat_at
    exec_row = session.query(JobExecutionRow).filter_by(execution_id=exec_id).one()
    exec_row.heartbeat_at = datetime.now(timezone.utc) - timedelta(seconds=120)
    session.flush()

    # Reap stalled executions
    reaped = scheduler.reap_stalled_executions(heartbeat_timeout_seconds=60)
    assert reaped == 1
    assert exec_row.status == JobStatus.RETRYING.value
    assert exec_row.attempt == 2

    # Expire heartbeat again for final attempt
    exec_row.status = JobStatus.RUNNING.value
    exec_row.heartbeat_at = datetime.now(timezone.utc) - timedelta(seconds=120)
    session.flush()

    reaped2 = scheduler.reap_stalled_executions(heartbeat_timeout_seconds=60)
    assert reaped2 == 1
    assert exec_row.status == JobStatus.DEAD_LETTER.value


def test_fail_job_exponential_backoff_and_cancel(session):
    scheduler = EnterpriseJobScheduler(session)
    scheduler.register_job(
        JobDefinition(job_code="RETRY_JOB", name="Retry Job", erp_domain="ops", handler="h", retry_limit=3)
    )

    ex = scheduler.trigger_job("RETRY_JOB")
    scheduler.poll_and_dispatch("worker-1")

    # Fail attempt 1 -> RETRYING
    failed1 = scheduler.fail_job(ex.execution_id, error_message="Network connection timeout")
    assert failed1.status == JobStatus.RETRYING.value
    assert failed1.attempt == 2

    # Fail attempt 2 -> RETRYING
    failed1.status = JobStatus.RUNNING.value
    session.flush()
    failed2 = scheduler.fail_job(ex.execution_id, error_message="DB Deadlock")
    assert failed2.status == JobStatus.RETRYING.value
    assert failed2.attempt == 3

    # Fail attempt 3 -> DEAD_LETTER
    failed2.status = JobStatus.RUNNING.value
    session.flush()
    failed3 = scheduler.fail_job(ex.execution_id, error_message="Out of memory")
    assert failed3.status == JobStatus.DEAD_LETTER.value

    # Test cancellation
    cancel_ex = scheduler.trigger_job("RETRY_JOB")
    cancelled = scheduler.cancel_job(cancel_ex.execution_id, reason="User requested abort")
    assert cancelled.status == JobStatus.CANCELLED.value


def test_scheduler_metrics(session):
    scheduler = EnterpriseJobScheduler(session)
    scheduler.register_job(
        JobDefinition(job_code="METRIC_JOB", name="Metric Job", erp_domain="ops", handler="h")
    )

    for i in range(5):
        e = scheduler.trigger_job("METRIC_JOB")
        scheduler.poll_and_dispatch(f"w-{i}")
        scheduler.complete_job(e.execution_id, result={"ok": True})

    metrics = scheduler.get_metrics(window_minutes=60)
    assert metrics.total_executions == 5
    assert metrics.completed_count == 5
    assert metrics.failed_count == 0
    assert metrics.throughput_per_minute > 0


def test_api_scheduler_endpoints(client):
    headers = {"x-api-key": "operator-key"}
    view_headers = {"x-api-key": "viewer-key"}

    # 1. Register definition
    res_def = client.post(
        "/jobs/definitions",
        headers=headers,
        json={
            "job_code": "ERP_PO_DISPATCH",
            "name": "ERP Purchase Order Dispatch",
            "erp_domain": "supply_chain",
            "handler": "com.oracle.po.DispatchHandler",
            "concurrency_limit": 4,
            "priority": 3,
        },
    )
    assert res_def.status_code == 200
    assert res_def.json()["status"] == "registered"

    # 2. Trigger execution
    res_trig = client.post(
        "/jobs/trigger",
        headers=headers,
        json={
            "job_code": "ERP_PO_DISPATCH",
            "payload": {"po_id": 99201},
            "idempotency_key": "api-idem-1",
        },
    )
    assert res_trig.status_code == 200
    exec_id = res_trig.json()["execution_id"]
    assert res_trig.json()["status"] == "PENDING"

    # 3. Poll jobs
    res_poll = client.post(
        "/jobs/poll?worker_id=worker-api-1&batch_size=5&erp_domain=supply_chain",
        headers=headers,
    )
    assert res_poll.status_code == 200
    dispatched = res_poll.json()["dispatched"]
    assert len(dispatched) == 1
    assert dispatched[0]["execution_id"] == exec_id

    # 4. Heartbeat
    res_hb = client.post(
        "/jobs/heartbeat",
        headers=headers,
        json={"execution_id": exec_id, "worker_id": "worker-api-1"},
    )
    assert res_hb.status_code == 200
    assert res_hb.json()["status"] == "heartbeat_acknowledged"

    # 5. Complete job
    res_comp = client.post(
        "/jobs/complete",
        headers=headers,
        json={"execution_id": exec_id, "result": {"dispatched_count": 14}},
    )
    assert res_comp.status_code == 200
    assert res_comp.json()["status"] == "COMPLETED"

    # 6. Check metrics
    res_met = client.get("/jobs/metrics", headers=view_headers)
    assert res_met.status_code == 200
    assert res_met.json()["completed_count"] >= 1
