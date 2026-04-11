"""add cover_writers to track

Revision ID: h4d5e6f7g8h9
Revises: g3c4d5e6f7g8
Branch Labels: None
Depends on: None

"""
from alembic import op
import sqlalchemy as sa


revision = 'h4d5e6f7g8h9'
down_revision = 'g3c4d5e6f7g8'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('track', sa.Column('cover_writers', sa.Text(), nullable=True, server_default=''))


def downgrade():
    op.drop_column('track', 'cover_writers')
