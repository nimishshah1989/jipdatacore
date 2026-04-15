"""Create de_sector_mapping table and seed 31 JIP sector → NSE index mappings.

Revision ID: 013_sector_mapping
Revises: 012_equity_fundamentals
Create Date: 2026-04-15
"""

from alembic import op
import sqlalchemy as sa

revision = "013_sector_mapping"
down_revision = "012_equity_fundamentals"
branch_labels = None
depends_on = None

SECTOR_MAPPINGS = [
    # (jip_sector_name, primary_nse_index, secondary_nse_indices, notes)
    ("Automobile", "NIFTY AUTO", None, None),
    ("Banking", "NIFTY BANK", ["NIFTY PSU BANK", "NIFTY PVT BANK"], None),
    ("Capital Goods", "NIFTY INDIA MFG", None, "No direct capital-goods sectoral index; manufacturing is closest proxy"),
    ("Capital Markets", "NIFTY CAPITAL MKT", None, None),
    ("Chemicals", "NIFTY CHEMICALS", None, None),
    ("Conglomerate", "NIFTYCONGLOMERATE", None, None),
    ("Consumer Durables", "NIFTY CONSR DURBL", None, None),
    ("Consumption", "NIFTY CONSUMPTION", ["NIFTY MS IND CONS", "NIFTY NEW CONSUMP"], None),
    ("Defence", "NIFTY IND DEFENCE", None, None),
    ("Digital", "NIFTY IND DIGITAL", ["NIFTY INTERNET"], None),
    ("Diversified", "NIFTY 500", None, "No sectoral index for diversified companies; broadest market proxy used"),
    ("Energy", "NIFTY ENERGY", None, None),
    ("EV & Auto", "NIFTY EV", ["NIFTY AUTO"], "Covers EV and new-age automotive; NIFTY AUTO as secondary for traditional auto overlap"),
    ("Financial Services", "NIFTY FIN SERVICE", ["NIFTY FINSEREXBNK", "NIFTY FINSRV25 50", "NIFTY MS FIN SERV"], None),
    ("FMCG", "NIFTY FMCG", None, None),
    ("Healthcare", "NIFTY HEALTHCARE", ["NIFTY500 HEALTH", "NIFTY MIDSML HLTH"], None),
    ("Housing", "NIFTY HOUSING", ["NIFTY COREHOUSING"], None),
    ("Infrastructure", "NIFTY INFRA", ["NIFTY INFRALOG", "NIFTY MULTI INFRA"], None),
    ("IT", "NIFTY IT", ["NIFTY MS IT TELCM"], None),
    ("Logistics", "NIFTY TRANS LOGIS", ["NIFTY INFRALOG"], None),
    ("Media", "NIFTY MEDIA", None, None),
    ("Metal", "NIFTY METAL", None, None),
    ("MNC", "NIFTY MNC", None, None),
    ("Oil & Gas", "NIFTY OIL AND GAS", None, None),
    ("Pharma", "NIFTY PHARMA", ["NIFTY HEALTHCARE", "NIFTY MIDSML HLTH"], None),
    ("Power", "NIFTY ENERGY", None, "No dedicated power index; energy is closest proxy"),
    ("Realty", "NIFTY REALTY", None, None),
    ("Rural", "NIFTY RURAL", None, None),
    ("Services", "NIFTY SERV SECTOR", None, None),
    ("Telecom", "NIFTY MS IT TELCM", None, "No standalone telecom index; IT & Telecom composite is closest"),
    ("Tourism", "NIFTY IND TOURISM", None, None),
]


def upgrade() -> None:
    op.create_table(
        "de_sector_mapping",
        sa.Column("jip_sector_name", sa.String(50), primary_key=True),
        sa.Column(
            "primary_nse_index",
            sa.String(50),
            sa.ForeignKey("de_index_master.index_code"),
            nullable=False,
        ),
        sa.Column("secondary_nse_indices", sa.ARRAY(sa.Text), nullable=True),
        sa.Column("notes", sa.Text, nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )

    tbl = sa.table(
        "de_sector_mapping",
        sa.column("jip_sector_name", sa.String),
        sa.column("primary_nse_index", sa.String),
        sa.column("secondary_nse_indices", sa.ARRAY(sa.Text)),
        sa.column("notes", sa.Text),
    )
    op.bulk_insert(
        tbl,
        [
            {
                "jip_sector_name": name,
                "primary_nse_index": primary,
                "secondary_nse_indices": secondary,
                "notes": notes,
            }
            for name, primary, secondary, notes in SECTOR_MAPPINGS
        ],
    )


def downgrade() -> None:
    op.drop_table("de_sector_mapping")
