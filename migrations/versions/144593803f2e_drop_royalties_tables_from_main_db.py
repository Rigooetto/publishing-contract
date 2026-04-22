"""drop royalties tables from main db

Revision ID: 144593803f2e
Revises: 2ab2087b8792
Create Date: 2026-04-17 18:32:07.638832

These three tables are now in royalties_db (separate PostgreSQL instance).
"""
from alembic import op


revision = '144593803f2e'
down_revision = '2ab2087b8792'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_table('streaming_royalty')
    op.drop_table('artist_royalty_split')
    op.drop_table('streaming_import')


def downgrade():
    pass  # tables are now managed in royalties_db; no restore needed
