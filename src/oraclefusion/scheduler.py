from __future__ import annotations

import json
import math
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .tables import AuditRow, JobDefinitionRow, JobExecutionRow, OutboxRow, utcnow


class JobStatus(str, Enum):
    PENDING = "PENDING"
    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    CANCELLED = "CANCELLED"
    DEAD_LETTER = "DEAD_LETTER"


class JobPriority(int, Enum):
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4


class SchedulerError(Exception):
    pass


class JobNotFoundError(SchedulerError):
    pass


class ConcurrencyLimitExceededError(SchedulerError):
    pass


class DependencyUnmetError(SchedulerError):
    pass


@dataclass
class JobDefinition:
    job_code: str
    name: str
    erp_domain: str
    handler: str
    cron_expression: str = ""
    timeout_seconds: int = 300
    retry_limit: int = 3
    concurrency_limit: int = 5
    priority: JobPriority | int = JobPriority.NORMAL
    dependencies: list[str] = field(default_factory=list)
    is_active: bool = True


@dataclass
class SchedulerMetrics:
    total_executions: int
    completed_count: int
    failed_count: int
    dead_letter_count: int
    active_workers: int
    p50_duration_ms: float
    p95_duration_ms: float
    p99_duration_ms: float
    throughput_per_minute: float


def to_utc(dt_val: datetime | None) -> datetime | None:
    if dt_val is None:
        return None
    if dt_val.tzinfo is None:
        return dt_val.replace(tzinfo=UTC)
    return dt_val.astimezone(UTC)


class EnterpriseJobScheduler:
    def __init__(self, session: Session, actor: str = "enterprise_scheduler") -> None:
        self.session = session
        self.actor = actor

    def _audit(self, action: str, reference: str, detail: str = "") -> None:
        self.session.add(AuditRow(action=action, actor=self.actor, reference=reference, detail=detail))

    def _enqueue_outbox(self, entity_type: str, entity_id: int, key: str, operation: str = "trigger") -> OutboxRow:
        existing = self.session.scalar(
            select(OutboxRow).where(
                (OutboxRow.idempotency_key == key)
                | (
                    (OutboxRow.entity_type == entity_type)
                    & (OutboxRow.entity_id == entity_id)
                    & (OutboxRow.operation == operation)
                )
            )
        )
        if existing is not None:
            return existing
        row = OutboxRow(entity_type=entity_type, entity_id=entity_id, operation=operation, idempotency_key=key)
        self.session.add(row)
        return row

    def register_job(self, defn: JobDefinition) -> JobDefinitionRow:
        if not defn.job_code.strip() or not defn.name.strip():
            raise SchedulerError("job_code and name are required")
        if defn.job_code in defn.dependencies:
            raise SchedulerError("A job cannot depend on itself")

        existing = self.session.scalar(select(JobDefinitionRow).where(JobDefinitionRow.job_code == defn.job_code))
        priority_val = int(defn.priority)
        deps_json = json.dumps(defn.dependencies)

        if existing is not None:
            existing.name = defn.name
            existing.erp_domain = defn.erp_domain
            existing.handler = defn.handler
            existing.cron_expression = defn.cron_expression
            existing.timeout_seconds = defn.timeout_seconds
            existing.retry_limit = defn.retry_limit
            existing.concurrency_limit = defn.concurrency_limit
            existing.priority = priority_val
            existing.dependencies_json = deps_json
            existing.is_active = defn.is_active
            row = existing
        else:
            row = JobDefinitionRow(
                job_code=defn.job_code,
                name=defn.name,
                erp_domain=defn.erp_domain,
                handler=defn.handler,
                cron_expression=defn.cron_expression,
                timeout_seconds=defn.timeout_seconds,
                retry_limit=defn.retry_limit,
                concurrency_limit=defn.concurrency_limit,
                priority=priority_val,
                dependencies_json=deps_json,
                is_active=defn.is_active,
            )
            self.session.add(row)

        self.session.flush()
        self._audit("register_job_definition", defn.job_code, f"domain={defn.erp_domain}, priority={priority_val}")
        return row

    def trigger_job(
        self,
        job_code: str,
        payload: dict[str, Any] | None = None,
        priority: JobPriority | int | None = None,
        idempotency_key: str | None = None,
        scheduled_at: datetime | None = None,
    ) -> JobExecutionRow:
        defn = self.session.scalar(select(JobDefinitionRow).where(JobDefinitionRow.job_code == job_code))
        if defn is None:
            raise JobNotFoundError(f"Job definition '{job_code}' not found")
        if not defn.is_active:
            raise SchedulerError(f"Job definition '{job_code}' is currently inactive")

        idem_key = idempotency_key or f"exec:{job_code}:{uuid.uuid4().hex}"
        existing_exec = self.session.scalar(select(JobExecutionRow).where(JobExecutionRow.idempotency_key == idem_key))
        if existing_exec is not None:
            return existing_exec

        # Check dependencies
        deps: list[str] = json.loads(defn.dependencies_json or "[]")
        for dep in deps:
            last_completed = self.session.scalar(
                select(JobExecutionRow)
                .where(JobExecutionRow.job_code == dep, JobExecutionRow.status == JobStatus.COMPLETED.value)
                .limit(1)
            )
            if last_completed is None:
                raise DependencyUnmetError(f"Prerequisite job '{dep}' has not completed successfully")

        exec_id = f"job_run_{uuid.uuid4().hex[:12]}"
        pri_val = int(priority) if priority is not None else defn.priority
        sched_time = scheduled_at or utcnow()

        execution = JobExecutionRow(
            execution_id=exec_id,
            job_code=job_code,
            priority=pri_val,
            status=JobStatus.PENDING.value,
            attempt=1,
            max_attempts=defn.retry_limit,
            scheduled_at=sched_time,
            payload_json=json.dumps(payload or {}),
            result_json="{}",
            idempotency_key=idem_key,
        )
        self.session.add(execution)
        self.session.flush()

        self._enqueue_outbox("job_execution", execution.id, f"trigger:{exec_id}", operation="trigger")
        self._audit("trigger_job", exec_id, f"job={job_code}, priority={pri_val}")
        self.session.flush()
        return execution

    def poll_and_dispatch(
        self,
        worker_id: str,
        batch_size: int = 5,
        erp_domain: str | None = None,
    ) -> list[JobExecutionRow]:
        now = utcnow()
        stmt = (
            select(JobExecutionRow)
            .where(
                JobExecutionRow.status.in_([JobStatus.PENDING.value, JobStatus.RETRYING.value]),
                JobExecutionRow.scheduled_at <= now,
            )
            .order_by(JobExecutionRow.priority.desc(), JobExecutionRow.scheduled_at.asc())
        )

        candidates = list(self.session.scalars(stmt).all())
        dispatched: list[JobExecutionRow] = []

        # Track active concurrency per job code
        running_counts: dict[str, int] = {}
        definitions: dict[str, JobDefinitionRow] = {}

        for exec_row in candidates:
            if len(dispatched) >= batch_size:
                break

            jcode = exec_row.job_code
            if jcode not in definitions:
                d = self.session.scalar(select(JobDefinitionRow).where(JobDefinitionRow.job_code == jcode))
                if d is None:
                    continue
                definitions[jcode] = d

            defn = definitions[jcode]
            if erp_domain and defn.erp_domain != erp_domain:
                continue

            if jcode not in running_counts:
                count = self.session.scalar(
                    select(func.count(JobExecutionRow.id)).where(
                        JobExecutionRow.job_code == jcode,
                        JobExecutionRow.status == JobStatus.RUNNING.value,
                    )
                ) or 0
                running_counts[jcode] = count

            if running_counts[jcode] >= defn.concurrency_limit:
                continue

            # Dispatch job to worker
            exec_row.status = JobStatus.RUNNING.value
            exec_row.worker_id = worker_id
            exec_row.started_at = now
            exec_row.heartbeat_at = now
            running_counts[jcode] += 1
            dispatched.append(exec_row)

            self._enqueue_outbox("job_execution", exec_row.id, f"dispatch:{exec_row.execution_id}:{worker_id}", operation="dispatch")

        self.session.flush()
        return dispatched

    def record_heartbeat(self, execution_id: str, worker_id: str) -> bool:
        exec_row = self.session.scalar(
            select(JobExecutionRow).where(
                JobExecutionRow.execution_id == execution_id,
                JobExecutionRow.worker_id == worker_id,
                JobExecutionRow.status == JobStatus.RUNNING.value,
            )
        )
        if exec_row is None:
            return False
        exec_row.heartbeat_at = utcnow()
        self.session.flush()
        return True

    def reap_stalled_executions(self, heartbeat_timeout_seconds: int = 60) -> int:
        cutoff = utcnow() - timedelta(seconds=heartbeat_timeout_seconds)
        stalled = list(
            self.session.scalars(
                select(JobExecutionRow).where(
                    JobExecutionRow.status == JobStatus.RUNNING.value,
                    JobExecutionRow.heartbeat_at < cutoff,
                )
            ).all()
        )

        reaped_count = 0
        for exec_row in stalled:
            reaped_count += 1
            if exec_row.attempt < exec_row.max_attempts:
                exec_row.attempt += 1
                backoff_seconds = min(300, 2 ** exec_row.attempt * 5)
                exec_row.status = JobStatus.RETRYING.value
                exec_row.scheduled_at = utcnow() + timedelta(seconds=backoff_seconds)
                exec_row.worker_id = ""
                exec_row.error_message = f"Stalled worker lease expired after {heartbeat_timeout_seconds}s"
                self._audit("reap_stalled_job", exec_row.execution_id, f"rescheduled_attempt={exec_row.attempt}")
            else:
                exec_row.status = JobStatus.DEAD_LETTER.value
                exec_row.completed_at = utcnow()
                exec_row.error_message = f"Stalled worker lease expired, max attempts ({exec_row.max_attempts}) reached"
                self._audit("dead_letter_job", exec_row.execution_id, "max_stalled_attempts_exceeded")

        self.session.flush()
        return reaped_count

    def complete_job(self, execution_id: str, result: dict[str, Any] | None = None) -> JobExecutionRow:
        exec_row = self.session.scalar(select(JobExecutionRow).where(JobExecutionRow.execution_id == execution_id))
        if exec_row is None:
            raise JobNotFoundError(f"Execution '{execution_id}' not found")
        if exec_row.status not in (JobStatus.RUNNING.value, JobStatus.PENDING.value):
            raise SchedulerError(f"Cannot complete job with status '{exec_row.status}'")

        now = utcnow()
        exec_row.status = JobStatus.COMPLETED.value
        exec_row.completed_at = now
        exec_row.result_json = json.dumps(result or {})

        if exec_row.started_at is not None:
            started = to_utc(exec_row.started_at)
            exec_row.duration_ms = max(0, int((now - started).total_seconds() * 1000))

        self._enqueue_outbox("job_execution", exec_row.id, f"complete:{execution_id}", operation="complete")
        self._audit("complete_job", execution_id, f"duration_ms={exec_row.duration_ms}")
        self.session.flush()
        return exec_row

    def fail_job(self, execution_id: str, error_message: str, retryable: bool = True) -> JobExecutionRow:
        exec_row = self.session.scalar(select(JobExecutionRow).where(JobExecutionRow.execution_id == execution_id))
        if exec_row is None:
            raise JobNotFoundError(f"Execution '{execution_id}' not found")

        now = utcnow()
        exec_row.error_message = error_message

        if exec_row.started_at is not None:
            started = to_utc(exec_row.started_at)
            exec_row.duration_ms = max(0, int((now - started).total_seconds() * 1000))

        if retryable and exec_row.attempt < exec_row.max_attempts:
            exec_row.attempt += 1
            backoff_sec = min(300, 2 ** exec_row.attempt * 5)
            exec_row.status = JobStatus.RETRYING.value
            exec_row.scheduled_at = now + timedelta(seconds=backoff_sec)
            exec_row.worker_id = ""
            self._audit("retry_job", execution_id, f"attempt={exec_row.attempt}, backoff={backoff_sec}s")
        else:
            exec_row.status = JobStatus.DEAD_LETTER.value
            exec_row.completed_at = now
            self._audit("dead_letter_job", execution_id, f"error={error_message[:100]}")

        self._enqueue_outbox(
            "job_execution",
            exec_row.id,
            f"fail:{execution_id}:{exec_row.status}:{exec_row.attempt}",
            operation=f"fail_{exec_row.attempt}",
        )
        self.session.flush()
        return exec_row

    def cancel_job(self, execution_id: str, reason: str = "") -> JobExecutionRow:
        exec_row = self.session.scalar(select(JobExecutionRow).where(JobExecutionRow.execution_id == execution_id))
        if exec_row is None:
            raise JobNotFoundError(f"Execution '{execution_id}' not found")
        if exec_row.status in (JobStatus.COMPLETED.value, JobStatus.DEAD_LETTER.value):
            raise SchedulerError(f"Cannot cancel terminal job '{execution_id}'")

        exec_row.status = JobStatus.CANCELLED.value
        exec_row.completed_at = utcnow()
        exec_row.error_message = reason or "Cancelled by operator"
        self._audit("cancel_job", execution_id, reason)
        self.session.flush()
        return exec_row

    def get_metrics(self, window_minutes: int = 60) -> SchedulerMetrics:
        since = utcnow() - timedelta(minutes=window_minutes)

        executions = list(
            self.session.scalars(
                select(JobExecutionRow).where(JobExecutionRow.scheduled_at >= since)
            ).all()
        )

        total = len(executions)
        completed = sum(1 for e in executions if e.status == JobStatus.COMPLETED.value)
        failed = sum(1 for e in executions if e.status in (JobStatus.FAILED.value, JobStatus.RETRYING.value))
        dead_letter = sum(1 for e in executions if e.status == JobStatus.DEAD_LETTER.value)
        active_workers = len(set(e.worker_id for e in executions if e.status == JobStatus.RUNNING.value and e.worker_id))

        durations = sorted(e.duration_ms for e in executions if e.duration_ms is not None)

        def percentile(data: list[int], pct: float) -> float:
            if not data:
                return 0.0
            k = (len(data) - 1) * pct
            f = math.floor(k)
            c = math.ceil(k)
            if f == c:
                return float(data[int(k)])
            d0 = data[f] * (c - k)
            d1 = data[c] * (k - f)
            return float(d0 + d1)

        p50 = percentile(durations, 0.50)
        p95 = percentile(durations, 0.95)
        p99 = percentile(durations, 0.99)
        tpm = round(completed / max(1, window_minutes), 2)

        return SchedulerMetrics(
            total_executions=total,
            completed_count=completed,
            failed_count=failed,
            dead_letter_count=dead_letter,
            active_workers=active_workers,
            p50_duration_ms=p50,
            p95_duration_ms=p95,
            p99_duration_ms=p99,
            throughput_per_minute=tpm,
        )
