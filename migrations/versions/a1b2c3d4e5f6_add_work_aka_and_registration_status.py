"""add work_aka table and registration_status to work

Revision ID: a1b2c3d4e5f6
Revises: eacd09f30544
Create Date: 2026-04-12 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = 'a1b2c3d4e5f6'
down_revision = 'eacd09f30544'
branch_labels = None
depends_on = None


def upgrade():
    # Add registration_status to work table
    with op.batch_alter_table('work', schema=None) as batch_op:
        batch_op.add_column(sa.Column('registration_status', sa.String(length=20),
                                      nullable=False, server_default='new'))
        batch_op.create_index('ix_work_registration_status', ['registration_status'])

    # Backfill: works that already appear in pro_registration → confirmed
    op.execute("""
        UPDATE work
        SET registration_status = 'confirmed'
        WHERE id IN (SELECT DISTINCT work_id FROM pro_registration)
    """)

    # Create work_aka table
    op.create_table(
        'work_aka',
        sa.Column('id',         sa.Integer(),      nullable=False),
        sa.Column('work_id',    sa.Integer(),      nullable=False),
        sa.Column('title',      sa.String(255),    nullable=False),
        sa.Column('normalized', sa.String(255),    nullable=False),
        sa.Column('source',     sa.String(50),     nullable=True),
        sa.Column('created_at', sa.DateTime(),     nullable=True),
        sa.ForeignKeyConstraint(['work_id'], ['work.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_work_aka_work_id',    'work_aka', ['work_id'])
    op.create_index('ix_work_aka_normalized', 'work_aka', ['normalized'])


def downgrade():
    op.drop_index('ix_work_aka_normalized', table_name='work_aka')
    op.drop_index('ix_work_aka_work_id',    table_name='work_aka')
    op.drop_table('work_aka')

    with op.batch_alter_table('work', schema=None) as batch_op:
        batch_op.drop_index('ix_work_registration_status')
        batch_op.drop_column('registration_status')
