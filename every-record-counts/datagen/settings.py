"""Fixed parameters of the synthetic Larkspur world.

Everything that shapes the data lives here so a reader can see the whole
world in one file. Volumes are a scaled-down sample of the story's company:
the real Larkspur has ~600 stores doing thousands of baskets a day; the lab
keeps all 600 stores and samples the baskets.
"""

from dataclasses import dataclass
from datetime import date

# Activity window: June gives Q3 refunds something to refer back to, and the
# first days of October give the legacy report late refunds and late uploads.
START = date(2026, 6, 1)
END = date(2026, 10, 4)  # inclusive
Q3_START = date(2026, 7, 1)
Q3_END = date(2026, 9, 30)  # inclusive
REPORT_DATE = date(2026, 10, 5)  # the day the board-deck numbers were pulled
CLICKSTREAM_START = Q3_START  # clickstream is only kept for the quarter
INVENTORY_START = Q3_START

REPORTING_CURRENCY = "GBP"


@dataclass(frozen=True)
class Country:
    code: str
    currency: str
    tz: str
    stores: int
    baskets_per_store_day: float  # at scale 1.0, for a "super" store
    online_orders_per_day: float  # at scale 1.0
    price_factor: float  # local minor units per GBP penny of base price
    tender_mix: dict[str, float]  # tender type -> share
    cities: tuple[str, ...]


COUNTRIES: dict[str, Country] = {
    "IN": Country(
        code="IN",
        currency="INR",
        tz="Asia/Kolkata",
        stores=300,
        baskets_per_store_day=30.0,
        online_orders_per_day=600.0,
        price_factor=60.0,  # 1 GBP penny of base price ~ 60 paise (PPP-ish, not FX)
        tender_mix={"cash": 0.40, "card": 0.25, "upi": 0.35},
        cities=("Bengaluru", "Mumbai", "Delhi", "Pune", "Hyderabad", "Chennai", "Kolkata", "Ahmedabad"),
    ),
    "GB": Country(
        code="GB",
        currency="GBP",
        tz="Europe/London",
        stores=200,
        baskets_per_store_day=30.0,
        online_orders_per_day=600.0,
        price_factor=1.0,
        # Larkspur UK went cashless in 2024: every UK payment goes through the PSP.
        tender_mix={"card": 0.78, "wallet": 0.22},
        cities=("London", "Manchester", "Birmingham", "Leeds", "Glasgow", "Bristol", "Edinburgh", "Cardiff"),
    ),
    "SG": Country(
        code="SG",
        currency="SGD",
        tz="Asia/Singapore",
        stores=100,
        baskets_per_store_day=30.0,
        online_orders_per_day=200.0,
        price_factor=1.8,
        tender_mix={"card": 0.55, "wallet": 0.35, "cash": 0.10},
        cities=("Singapore",),
    ),
}

# Tenders settled by the payment service provider (PSP); cash never reaches it.
PSP_TENDERS = {"card", "wallet", "upi"}

# Share of UK online orders paid in euros by EU customers (cross-border).
GB_EUR_ORDER_SHARE = 0.07

# Store formats: share of stores, size multiplier on baskets, tracked SKUs.
FORMATS = {
    "hyper": (0.15, 2.2, 360),
    "super": (0.50, 1.0, 220),
    "express": (0.35, 0.45, 110),
}
SHARE_24H = 0.06
SHARE_MORNING_SNAPSHOT = 0.35  # stores whose WMS snapshot runs after the overnight delivery
OUTAGE_PRODUCT_SHARE = 0.24  # lines per country hit by a 2-4 week supply outage in the window

N_PRODUCTS = 2000
N_SUPPLIERS = 40
N_PROMOTIONS = 48

CATEGORIES = {
    # category: (share of SKUs, min base price pence, max base price pence, chilled share)
    "Fresh Produce": (0.10, 40, 450, 0.6),
    "Dairy & Chilled": (0.10, 60, 650, 1.0),
    "Bakery": (0.06, 50, 400, 0.0),
    "Pantry": (0.20, 50, 900, 0.0),
    "Beverages": (0.12, 60, 1800, 0.15),
    "Household": (0.10, 90, 1500, 0.0),
    "Personal Care": (0.10, 120, 2500, 0.0),
    "Home & Kitchen": (0.08, 300, 6000, 0.0),
    "Electronics": (0.05, 900, 25000, 0.0),
    "Apparel": (0.09, 400, 6000, 0.0),
}

# Behaviour rates (per basket / order unless stated).
REFUND_RATE_POS = 0.025
REFUND_RATE_ONLINE = 0.06
REFUND_MAX_LAG_DAYS = 45
ONLINE_CANCEL_RATE = 0.035  # cancelled before fulfilment, then hard-deleted
OFFLINE_TILL_SHARE = 0.03  # baskets buffered on a till that lost its connection
CHARGEBACK_RATE = 0.0015
PSP_FEE_BPS = {"card": 120, "wallet": 140, "upi": 0}  # basis points of gross
PSP_FEE_FIXED_MINOR = {"card": 10, "wallet": 10, "upi": 0}

# Calibration targets for the incident (see docs/injected-defects.md).
TARGET_UK_REVENUE_GAP = 0.0070  # legacy board-deck number vs correct, as a share of correct
TARGET_LEGACY_AVAILABILITY = 0.97  # roughly; the stock defect is calibrated to land near it
TARGET_CONVERSION_GAP_PTS = 0.9  # roughly; product vs marketing, in percentage points
