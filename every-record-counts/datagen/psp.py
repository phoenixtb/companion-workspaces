"""Daily settlement files from Larkspur's payment service provider, 'Kestrel Payments'.

One CSV per country per settlement day. Amounts are decimal strings in major
units, as PSPs send them. Sales settle two weekdays after capture, refunds two
weekdays after the refund, chargebacks arrive weeks later. Fees are deducted
per sale. EUR payments settle in GBP at the reference rate of the capture date.
Only files dated on or before the report date exist when the board deck is built.
"""

import numpy as np
import polars as pl

from datagen import settings as s
from datagen.incident import _convert, fx_table
from datagen.sales import Sales

CHARGEBACK_FEE_MINOR = 1500


def build(seed: int, sales: Sales, fx_raw: pl.DataFrame) -> pl.DataFrame:
    rng = np.random.default_rng([seed, 8])
    pay = sales.payments.filter(pl.col("status") == "captured")
    sale = pay.select(
        "country",
        "currency",
        "tender_type",
        pl.col("captured_at").alias("transaction_ts"),
        "amount_minor",
        merchant_reference="reference",
        record_type=pl.lit("SALE"),
    )
    refund = sales.refunds.filter(pl.col("tender_type").is_in(list(s.PSP_TENDERS))).select(
        "country",
        "currency",
        "tender_type",
        pl.col("refund_ts").alias("transaction_ts"),
        "amount_minor",
        merchant_reference="original_reference",
        record_type=pl.lit("REFUND"),
    )
    disputed = sale.filter(pl.Series(rng.random(sale.height) < s.CHARGEBACK_RATE))
    lag = pl.Series(rng.uniform(20 * 86400, 60 * 86400, disputed.height).astype(np.int64))
    chargeback = disputed.with_columns(
        record_type=pl.lit("CHARGEBACK"), transaction_ts=pl.col("transaction_ts") + pl.duration(seconds=lag)
    )
    rows = pl.concat([sale, refund, chargeback])

    # Settlement is two weekdays after the transaction's local date.
    local = []
    for c in s.COUNTRIES.values():
        local.append(
            rows.filter(pl.col("country") == c.code).with_columns(
                local_date=pl.col("transaction_ts").dt.convert_time_zone(c.tz).dt.date()
            )
        )
    rows = (
        pl.concat(local)
        .with_columns(
            settlement_date=pl.col("local_date").dt.add_business_days(2, roll="forward"),
            utc_date=pl.col("transaction_ts").dt.date(),
        )
        .filter(pl.col("settlement_date") <= s.REPORT_DATE)
    )

    fx = (
        fx_table(fx_raw)
        .filter(pl.col("base_currency") == "EUR")
        .select(pl.col("rate_date").alias("utc_date"), "rate_e8")
    )
    rows = rows.sort("utc_date").join_asof(fx, on="utc_date", strategy="backward")
    local_currency = {c.code: c.currency for c in s.COUNTRIES.values()}
    gross = (
        pl.when(pl.col("currency") == "EUR")
        .then(_convert(pl.col("amount_minor"), pl.col("rate_e8")))
        .otherwise(pl.col("amount_minor"))
    )
    sign = pl.when(pl.col("record_type") == "SALE").then(1).otherwise(-1)
    bps = pl.col("tender_type").replace_strict(s.PSP_FEE_BPS, return_dtype=pl.Int64)
    fixed = pl.col("tender_type").replace_strict(s.PSP_FEE_FIXED_MINOR, return_dtype=pl.Int64)
    rows = rows.with_columns(
        settlement_currency=pl.col("country").replace_strict(local_currency),
        gross_minor=gross * sign,
        presentment_minor=pl.col("amount_minor") * sign,
    )
    rows = rows.with_columns(
        fee_minor=pl.when(pl.col("record_type") == "SALE")
        .then((pl.col("gross_minor") * bps + 5000) // 10000 + fixed)
        .when(pl.col("record_type") == "CHARGEBACK")
        .then(pl.lit(CHARGEBACK_FEE_MINOR))
        .otherwise(pl.lit(0))
    )
    rows = rows.sort("settlement_date", "transaction_ts")
    rows = rows.with_columns(
        psp_reference=pl.format("KP{}", pl.int_range(1, rows.height + 1).cast(pl.String).str.zfill(10)),
        net_minor=pl.col("gross_minor") - pl.col("fee_minor"),
    )
    return rows.select(
        "country",
        "settlement_date",
        "psp_reference",
        "merchant_reference",
        "record_type",
        pl.col("tender_type").alias("payment_method"),
        "transaction_ts",
        pl.col("currency").alias("presentment_currency"),
        "presentment_minor",
        "settlement_currency",
        "gross_minor",
        "fee_minor",
        "net_minor",
    )
