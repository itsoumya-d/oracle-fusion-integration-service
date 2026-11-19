"""scheduler tables

Revision ID: 2c92e3d7a82b
Revises: 1b81d2a6541f
Create Date: 2026-11-19 02:20:00.000000

"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '2c92e3d7a82b'
down_revision: str | Sequence[str] | None = '1b81d2a6541f'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        'job_definitions',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('job_code', sa.String(length=80), nullable=False),
        sa.Column('name', sa.String(length=200), nullable=False),
        sa.Column('erp_domain', sa.String(length=60), nullable=False),
        sa.Column('handler', sa.String(length=120), nullable=False),
        sa.Column('cron_expression', sa.String(length=60), nullable=False, server_default=''),
        sa.Column('timeout_seconds', sa.Integer(), nullable=False, server_default='300'),
        sa.Column('retry_limit', sa.Integer(), nullable=False, server_default='3'),
        sa.Column('concurrency_limit', sa.Integer(), nullable=False, server_default='5'),
        sa.Column('priority', sa.Integer(), nullable=False, server_default='2'),
        sa.Column('dependencies_json', sa.Text(), nullable=False, server_default='[]'),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default='1'),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_job_definitions_job_code'), 'job_definitions', ['job_code'], unique=True)
    op.create_index(op.f('ix_job_definitions_erp_domain'), 'job_definitions', ['erp_domain'], unique=False)

    op.create_table(
        'job_executions',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('execution_id', sa.String(length=80), nullable=False),
        sa.Column('job_code', sa.String(length=80), nullable=False),
        sa.Column('priority', sa.Integer(), nullable=False, server_default='2'),
        sa.Column('status', sa.String(length=30), nullable=False, server_default='PENDING'),
        sa.Column('attempt', sa.Integer(), nullable=False, server_default='1'),
        sa.Column('max_attempts', sa.Integer(), nullable=False, server_default='3'),
        sa.Column('worker_id', sa.String(length=100), nullable=False, server_default=''),
        sa.Column('scheduled_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('heartbeat_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('duration_ms', sa.Integer(), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=False, server_default=''),
        sa.Column('payload_json', sa.Text(), nullable=False, server_default='{}'),
        sa.Column('result_json', sa.Text(), nullable=False, server_default='{}'),
        sa.Column('idempotency_key', sa.String(length=120), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_job_executions_execution_id'), 'job_executions', ['execution_id'], unique=True)
    op.create_index(op.f('ix_job_executions_job_code'), 'job_executions', ['job_code'], unique=False)
    op.create_index(op.f('ix_job_executions_priority'), 'job_executions', ['priority'], unique=False)
    op.create_index(op.f('ix_job_executions_status'), 'job_executions', ['status'], unique=False)
    op.create_index(op.f('ix_job_executions_scheduled_at'), 'job_executions', ['scheduled_at'], unique=False)
    op.create_index(op.f('ix_job_executions_idempotency_key'), 'job_executions', ['idempotency_key'], unique=True)


def downgrade() -> None:
    op.drop_index(op.f('ix_job_executions_idempotency_key'), table_name='job_executions')
    op.drop_index(op.f('ix_job_executions_scheduled_at'), table_name='job_executions')
    op.drop_index(op.f('ix_job_executions_status'), table_name='job_executions')
    op.drop_index(op.f('ix_job_executions_priority'), table_name='job_executions')
    op.drop_index(op.f('ix_job_executions_job_code'), table_name='job_executions')
    op.drop_index(op.f('ix_job_executions_execution_id'), table_name='job_executions')
    op.drop_table('job_executions')
    op.drop_index(op.f('ix_job_definitions_erp_domain'), table_name='job_definitions')
    op.drop_index(op.f('ix_job_definitions_job_code'), table_name='job_definitions')
    op.drop_table('job_definitions')
