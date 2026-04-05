"""Dashboard configuration constants."""

DASHBOARD_PORT: int = 8099
API_BASE_URL: str = "http://localhost:8010/api/v1"
REFRESH_INTERVAL: int = 30  # seconds

# SLA deadlines — pipeline name → HH:MM IST
SLA_DEADLINES: dict[str, str] = {
    "pre_market": "08:00",
    "equity_eod": "19:30",
    "mf_nav": "22:30",
    "fii_dii_flows": "20:00",
    "rs_computation": "23:00",
    "regime_update": "23:30",
}

# Display labels for pipeline names
PIPELINE_LABELS: dict[str, str] = {
    "pre_market": "Pre-Market",
    "equity_eod": "Equity EOD",
    "mf_nav": "MF NAV",
    "fii_dii_flows": "FII/DII Flows",
    "rs_computation": "RS Computation",
    "regime_update": "Regime Update",
    "indices": "Indices",
    "fo_data": "F&O Data",
    "qualitative": "Qualitative",
    "reconciliation": "Reconciliation",
}

# All pipeline tracks shown in the status grid
ALL_PIPELINE_TRACKS: list[str] = [
    "pre_market",
    "equity_eod",
    "mf_nav",
    "fii_dii_flows",
    "rs_computation",
    "regime_update",
    "indices",
    "fo_data",
    "qualitative",
    "reconciliation",
]
