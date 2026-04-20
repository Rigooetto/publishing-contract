"""add artist_track table

Revision ID: 853f6020a7af
Revises: 170c117f21d8
Create Date: 2026-04-19 22:34:22.190099

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '853f6020a7af'
down_revision = '170c117f21d8'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('artist_track',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('artist_id', sa.Integer(), nullable=False),
    sa.Column('track_id', sa.Integer(), nullable=False),
    sa.Column('royalty_percentage', sa.Numeric(precision=5, scale=2), nullable=False),
    sa.ForeignKeyConstraint(['artist_id'], ['artist.id'], ),
    sa.ForeignKeyConstraint(['track_id'], ['track.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('artist_id', 'track_id')
    )
    with op.batch_alter_table('artist_track', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_artist_track_artist_id'), ['artist_id'], unique=False)
        batch_op.create_index(batch_op.f('ix_artist_track_track_id'), ['track_id'], unique=False)


def downgrade():
    with op.batch_alter_table('artist_track', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_artist_track_track_id'))
        batch_op.drop_index(batch_op.f('ix_artist_track_artist_id'))

    op.drop_table('artist_track')
