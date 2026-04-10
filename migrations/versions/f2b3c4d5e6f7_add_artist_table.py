"""add artist table

Revision ID: f2b3c4d5e6f7
Revises: e1a2b3c4d5e6
Branch Labels: None
Depends on: None

"""
from alembic import op
import sqlalchemy as sa

revision = 'f2b3c4d5e6f7'
down_revision = 'e1a2b3c4d5e6'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'artist',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('legal_name', sa.String(255), nullable=True),
        sa.Column('aka', sa.String(255), nullable=True),
        sa.Column('email', sa.String(255), nullable=True),
        sa.Column('phone_number', sa.String(50), nullable=True),
        sa.Column('address', sa.String(255), nullable=True),
        sa.Column('city', sa.String(100), nullable=True),
        sa.Column('state', sa.String(100), nullable=True),
        sa.Column('zip_code', sa.String(20), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name'),
    )
    op.create_index('ix_artist_name', 'artist', ['name'])
    op.create_index('ix_artist_email', 'artist', ['email'])


def downgrade():
    op.drop_index('ix_artist_email', table_name='artist')
    op.drop_index('ix_artist_name', table_name='artist')
    op.drop_table('artist')
