"""add artist_release join table

Revision ID: g3c4d5e6f7g8
Revises: f2b3c4d5e6f7
Branch Labels: None
Depends on: None

"""
from alembic import op
import sqlalchemy as sa

revision = 'g3c4d5e6f7g8'
down_revision = 'f2b3c4d5e6f7'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'artist_release',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('artist_id', sa.Integer(), nullable=False),
        sa.Column('release_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['artist_id'], ['artist.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['release_id'], ['release.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('artist_id', 'release_id'),
    )
    op.create_index('ix_artist_release_artist_id', 'artist_release', ['artist_id'])
    op.create_index('ix_artist_release_release_id', 'artist_release', ['release_id'])


def downgrade():
    op.drop_index('ix_artist_release_release_id', table_name='artist_release')
    op.drop_index('ix_artist_release_artist_id', table_name='artist_release')
    op.drop_table('artist_release')
