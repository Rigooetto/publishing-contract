"""phase3 pro registration and reports fields

Revision ID: i5j6k7l8m9n0
Revises: h4d5e6f7g8h9
Branch Labels: None
Depends on: None

"""
from alembic import op
import sqlalchemy as sa


revision = 'i5j6k7l8m9n0'
down_revision = 'h4d5e6f7g8h9'
branch_labels = None
depends_on = None


def upgrade():
    # Work — new catalog/PRO fields
    op.add_column('work', sa.Column('iswc', sa.String(20), nullable=True, server_default=''))
    op.add_column('work', sa.Column('mri_song_id', sa.String(50), nullable=True, server_default=''))
    op.add_column('work', sa.Column('aka_title', sa.String(255), nullable=True, server_default=''))
    op.add_column('work', sa.Column('aka_title_type_code', sa.String(5), nullable=True, server_default=''))

    # WorkWriter — role, territory, administrator
    op.add_column('work_writer', sa.Column('writer_role_code', sa.String(5), nullable=True, server_default='CA'))
    op.add_column('work_writer', sa.Column('territory_controlled', sa.String(50), nullable=True, server_default='World'))
    op.add_column('work_writer', sa.Column('administrator_name', sa.String(255), nullable=True, server_default=''))
    op.add_column('work_writer', sa.Column('administrator_ipi', sa.String(50), nullable=True, server_default=''))

    # Track — SoundExchange field
    op.add_column('track', sa.Column('country_of_recording', sa.String(100), nullable=True, server_default=''))

    # ProRegistration table
    op.create_table(
        'pro_registration',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('work_id', sa.Integer(), sa.ForeignKey('work.id'), nullable=False),
        sa.Column('pro', sa.String(20), nullable=False),
        sa.Column('pro_work_number', sa.String(100), nullable=True, server_default=''),
        sa.Column('mlc_song_code', sa.String(20), nullable=True, server_default=''),
        sa.Column('registered_at', sa.Date(), nullable=False),
        sa.Column('registered_by', sa.String(255), nullable=True, server_default=''),
        sa.Column('notes', sa.Text(), nullable=True, server_default=''),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_pro_registration_work_id', 'pro_registration', ['work_id'])

    # PublisherConfig table
    op.create_table(
        'publisher_config',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('publisher_name', sa.String(255), nullable=False),
        sa.Column('pro', sa.String(20), nullable=True, server_default=''),
        sa.Column('publisher_ipi', sa.String(50), nullable=True, server_default=''),
        sa.Column('mlc_publisher_number', sa.String(20), nullable=True, server_default=''),
        sa.Column('address', sa.String(255), nullable=True, server_default=''),
        sa.Column('city', sa.String(100), nullable=True, server_default=''),
        sa.Column('state', sa.String(10), nullable=True, server_default=''),
        sa.Column('zip_code', sa.String(20), nullable=True, server_default=''),
        sa.Column('contact_email', sa.String(255), nullable=True, server_default=''),
        sa.Column('contact_phone', sa.String(50), nullable=True, server_default=''),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('publisher_name'),
    )


def downgrade():
    op.drop_table('publisher_config')
    op.drop_index('ix_pro_registration_work_id', table_name='pro_registration')
    op.drop_table('pro_registration')
    op.drop_column('track', 'country_of_recording')
    op.drop_column('work_writer', 'administrator_ipi')
    op.drop_column('work_writer', 'administrator_name')
    op.drop_column('work_writer', 'territory_controlled')
    op.drop_column('work_writer', 'writer_role_code')
    op.drop_column('work', 'aka_title_type_code')
    op.drop_column('work', 'aka_title')
    op.drop_column('work', 'mri_song_id')
    op.drop_column('work', 'iswc')
