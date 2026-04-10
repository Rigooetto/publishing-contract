"""add is_cover to track

Revision ID: e1a2b3c4d5e6
Revises: cd28d5b2b218
Branch Labels: None
Depends on: None

"""
from alembic import op
import sqlalchemy as sa


revision = 'e1a2b3c4d5e6'
down_revision = 'cd28d5b2b218'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('track', sa.Column('is_cover', sa.Boolean(), nullable=True))


def downgrade():
    op.drop_column('track', 'is_cover')
