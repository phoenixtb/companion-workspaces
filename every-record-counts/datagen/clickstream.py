"""App and web clickstream for Q3, with bots, schema drift and two ways to count a session.

The product team sessionises per anonymous_id with a 30-minute inactivity
timeout and drops bot user agents. Marketing counts one session per
anonymous_id per UTC calendar day and keeps every hit. Both divide sessions
with a purchase by all sessions, and get different conversion rates.
"""

from dataclasses import dataclass
from datetime import UTC, date, datetime

import numpy as np
import polars as pl

from datagen import settings as s
from datagen.sales import Sales, _sec, _to_ts

CONSENT_FROM = date(2026, 8, 1)  # web and app start sending a consent block
RENAME_FROM = date(2026, 8, 15)  # `page` becomes `page_path` (web on the day, app as users update)
BOT_UA_PATTERN = "(?i)bot|crawl|spider|headless"
HUMAN_CONVERSION = 0.065  # sessions with a purchase, before idle gaps and revisits split them
IDLE_GAP_SHARE = 0.09
RESEARCH_SESSION_SHARE = 0.25  # buyers who browsed earlier the same day in a separate visit

USER_AGENTS = {
    "web": [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 15_6) AppleWebKit/605.1.15 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/141.0 Safari/537.36",
        "Mozilla/5.0 (Linux; Android 16) AppleWebKit/537.36 Chrome/141.0 Mobile Safari/537.36",
    ],
    "app": ["Larkspur/4.9.2 (iOS 26.0)", "Larkspur/4.9.2 (Android 16)"],
}
BOT_AGENTS = [
    "Mozilla/5.0 (compatible; PriceWatchBot/3.1; +https://example.com/bot)",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 HeadlessChrome/141.0 Safari/537.36",
    "Mozilla/5.0 (compatible; SEOspider/2.0)",
]


@dataclass
class Clickstream:
    events: pl.DataFrame
    metrics: dict[str, float]
    defect_counts: dict[str, int]


def _day_start(d: date) -> int:
    return _sec(datetime.combine(d, datetime.min.time(), tzinfo=UTC))


def build(seed: int, sales: Sales) -> Clickstream:
    rng = np.random.default_rng([seed, 7])
    o = sales.orders.filter(pl.col("created_at").dt.date().is_between(s.Q3_START, s.Q3_END))
    purchase_t = np.array([_sec(x) for x in o["created_at"].to_list()], dtype=np.float64)
    n_b = o.height
    countries = o["country"].to_numpy()
    platform_b = o["channel"].to_numpy()
    sessions = []

    # Buying sessions: browse, add to cart, check out, purchase.
    m = 3 + rng.poisson(2.5, n_b)
    start = purchase_t - rng.uniform(4 * 60, 25 * 60, n_b)
    sessions.append(
        dict(
            anon=np.array([f"a-{c}" for c in o["customer_id"].to_list()]),
            start=start,
            end=purchase_t,
            n=m,
            buyer=np.ones(n_b, bool),
            order=o["order_id"].to_numpy(),
            user=o["customer_id"].to_numpy(),
            country=countries,
            platform=platform_b,
        )
    )

    # Research visits: some buyers browsed earlier the same day, in a separate visit.
    r = rng.random(n_b) < RESEARCH_SESSION_SHARE
    day0 = np.array([_day_start(x.date()) for x in o["created_at"].to_list()], dtype=np.float64)
    r_start = purchase_t - rng.uniform(2 * 3600, 7 * 3600, n_b)
    r &= r_start > day0
    k = int(r.sum())
    sessions.append(
        dict(
            anon=sessions[0]["anon"][r],
            start=r_start[r],
            end=r_start[r] + rng.uniform(60, 600, k),
            n=1 + rng.poisson(2, k),
            buyer=np.zeros(k, bool),
            order=np.full(k, None),
            user=np.full(k, None),
            country=countries[r],
            platform=platform_b[r],
        )
    )

    # Non-buying visits.
    n_nb = int(n_b * (1 / HUMAN_CONVERSION - 1))
    days = (s.Q3_END - s.Q3_START).days + 1
    t0 = _day_start(s.Q3_START) + rng.integers(0, days, n_nb) * 86400.0
    tod = np.mod(
        np.where(rng.random(n_nb) < 0.4, rng.normal(13 * 3600, 7200, n_nb), rng.normal(21 * 3600, 6000, n_nb)), 86400
    )
    nb_start = t0 + tod
    nb_n = 1 + np.minimum(rng.geometric(0.45, n_nb) - 1, 11)
    pool = max(1000, int(n_nb * 0.6))
    nb_anon = np.array([f"v-{x:08d}" for x in rng.integers(0, pool, n_nb)])
    c_choice = rng.choice(list(s.COUNTRIES), size=n_nb, p=[0.43, 0.43, 0.14])
    sessions.append(
        dict(
            anon=nb_anon,
            start=nb_start,
            end=nb_start,
            n=nb_n,
            buyer=np.zeros(n_nb, bool),
            order=np.full(n_nb, None),
            user=np.full(n_nb, None),
            country=c_choice,
            platform=rng.choice(["web", "app"], size=n_nb, p=[0.5, 0.5]),
        )
    )

    human = _events(rng, sessions)
    product = _product_conversion(human)
    marketing_sessions, marketing_converted = _marketing_counts(human)
    target = product - s.TARGET_CONVERSION_GAP_PTS / 100
    n_bots = max(0, int(round(marketing_converted / target - marketing_sessions)))
    bots = _bots(rng, n_bots)
    events = pl.concat([human, bots], how="diagonal_relaxed")
    dupes = events.filter(pl.Series(rng.random(events.height) < 0.003))  # SDK retries resend the same event
    events = pl.concat([events, dupes]).sort("event_ts")
    m_sessions, m_converted = _marketing_counts(events)
    metrics = {
        "product_conversion": product,
        "marketing_conversion": m_converted / m_sessions,
        "human_sessions_product": float(_product_sessions(human).height),
    }
    counts = {
        "bot_hits": bots.height,
        "sdk_retry_duplicates": dupes.height,
        "events_with_page_path": int(events["page_path"].is_not_null().sum()),
        "events_with_consent": int(events["consent_analytics"].is_not_null().sum()),
    }
    return Clickstream(events, metrics, counts)


def _events(rng, sessions: list[dict]) -> pl.DataFrame:
    frames = []
    for sess in sessions:
        n = sess["n"]
        k = len(n)
        idx = np.repeat(np.arange(k), n)
        pos = np.concatenate([np.arange(x) for x in n]) if k else np.array([], int)
        last = pos == n[idx] - 1
        buyer = sess["buyer"][idx]
        # Event times: walk forward from the start with ~70 s gaps; buyers end exactly at the purchase.
        gaps = rng.exponential(70, len(idx))
        gaps[pos == 0] = 0
        idle = rng.random(k) < IDLE_GAP_SHARE  # a tab left open: one gap of 35-80 minutes mid-session
        idle_at = np.where(n > 2, rng.integers(1, np.maximum(n - 1, 2)), 10_000)
        gaps += np.where(idle[idx] & (pos == idle_at[idx]), rng.uniform(35 * 60, 80 * 60, len(idx)), 0)
        cum = _segment_cumsum(gaps, idx)
        t = sess["start"][idx] + cum
        span = np.bincount(idx, weights=gaps, minlength=k)
        # Buying sessions are anchored on the purchase time: shift so the last event is the purchase.
        shift = np.where(sess["buyer"], sess["end"] - (sess["start"] + span), 0)
        t = t + shift[idx]
        etype = np.where(pos == 0, "page_view", "product_view").astype(object)
        etype[buyer & last] = "purchase"
        etype[buyer & (pos == n[idx] - 2)] = "checkout_start"
        etype[buyer & (pos == n[idx] - 3) & (pos > 0)] = "add_to_cart"
        browse_cart = (~buyer) & last & (pos > 0) & (rng.random(len(idx)) < 0.25)
        etype[browse_cart] = "add_to_cart"
        sku = np.array([f"SKU-{x:05d}" for x in rng.integers(1, s.N_PRODUCTS + 1, len(idx))], dtype=object)
        page = np.select(
            [etype == "page_view", etype == "product_view", etype == "add_to_cart", etype == "checkout_start"],
            ["/", "/p/" + sku.astype(str), "/cart", "/checkout"],
            "/order/confirmed",
        ).astype(object)
        sku[(etype == "page_view") | (etype == "checkout_start") | (etype == "purchase")] = None
        platform = sess["platform"][idx]
        ua = np.where(
            platform == "app", rng.choice(USER_AGENTS["app"], len(idx)), rng.choice(USER_AGENTS["web"], len(idx))
        )
        frames.append(
            pl.DataFrame(
                {
                    "anonymous_id": sess["anon"][idx],
                    "user_id": pl.Series(sess["user"][idx].tolist(), dtype=pl.String),
                    "event_ts_s": t,
                    "event_type": etype.tolist(),
                    "platform": platform,
                    "user_agent": ua,
                    "country": sess["country"][idx],
                    "page": page.tolist(),
                    "sku": pl.Series(sku.tolist(), dtype=pl.String),
                    "order_id": pl.Series(
                        np.where(etype == "purchase", sess["order"][idx], None).tolist(), dtype=pl.String
                    ),
                }
            )
        )
    ev = pl.concat(frames)
    return _finish(rng, ev)


def _segment_cumsum(values: np.ndarray, seg: np.ndarray) -> np.ndarray:
    total = np.cumsum(values)
    starts = np.r_[0, np.flatnonzero(np.diff(seg)) + 1]
    base = np.repeat(total[starts] - values[starts], np.diff(np.r_[starts, len(values)]))
    return total - base


def _finish(rng, ev: pl.DataFrame) -> pl.DataFrame:
    n = ev.height
    ev = ev.with_columns(
        event_ts=_to_ts(ev["event_ts_s"].to_numpy()),
        event_id=pl.Series([f"{x:016x}" for x in rng.integers(0, 2**63, n)]),
    )
    # App users move to 5.0 (and `page_path`) gradually over two weeks after the web release.
    upgrade = {
        a: _sec(datetime.combine(RENAME_FROM, datetime.min.time(), tzinfo=UTC)) + int(x)
        # maintain_order: Polars 2 doesn't promise unique()'s order, and the seed must decide everything
        for a, x in zip(
            ev["anonymous_id"].unique(maintain_order=True).to_list(),
            rng.uniform(0, 14 * 86400, ev["anonymous_id"].n_unique()),
            strict=True,
        )
    }
    rename_web = _sec(datetime.combine(RENAME_FROM, datetime.min.time(), tzinfo=UTC))
    t = ev["event_ts_s"].to_numpy()
    up = np.array([upgrade[a] for a in ev["anonymous_id"].to_list()])
    renamed = np.where(ev["platform"].to_numpy() == "web", t >= rename_web, t >= up)
    consent = t >= _sec(datetime.combine(CONSENT_FROM, datetime.min.time(), tzinfo=UTC))
    is_app = ev["platform"].to_numpy() == "app"
    return (
        ev.with_columns(
            page_path=pl.when(pl.Series(renamed)).then(pl.col("page")),
            page=pl.when(pl.Series(~renamed)).then(pl.col("page")),
            app_version=pl.when(pl.Series(is_app)).then(
                pl.when(pl.Series(renamed)).then(pl.lit("5.0.1")).otherwise(pl.lit("4.9.2"))
            ),
            consent_analytics=pl.when(pl.Series(consent)).then(pl.Series(rng.random(n) < 0.82)),
            consent_ads=pl.when(pl.Series(consent)).then(pl.Series(rng.random(n) < 0.55)),
        )
        .with_columns(
            user_agent=pl.when(pl.col("platform") == "app")
            .then(pl.col("user_agent").str.replace("4.9.2", pl.col("app_version"), literal=True))
            .otherwise("user_agent")
        )
        .drop("event_ts_s")
    )


def _bots(rng, n: int) -> pl.DataFrame:
    """Crawlers keep no cookies: every hit arrives with a fresh anonymous_id."""
    days = (s.Q3_END - s.Q3_START).days + 1
    t = _day_start(s.Q3_START) + rng.uniform(0, days * 86400, n)
    sku = [f"SKU-{x:05d}" for x in rng.integers(1, s.N_PRODUCTS + 1, n)]
    rename_web = _sec(datetime.combine(RENAME_FROM, datetime.min.time(), tzinfo=UTC))
    renamed = t >= rename_web
    pages = ["/p/" + x for x in sku]
    return pl.DataFrame(
        {
            "anonymous_id": [f"b-{x:016x}" for x in rng.integers(0, 2**63, n)],
            "event_ts": _to_ts(t),
            "event_type": "page_view",
            "platform": "web",
            "user_agent": rng.choice(BOT_AGENTS, n),
            "country": rng.choice(list(s.COUNTRIES), n),
            "page": pl.Series([None if r else p for r, p in zip(renamed, pages, strict=True)], dtype=pl.String),
            "page_path": pl.Series([p if r else None for r, p in zip(renamed, pages, strict=True)], dtype=pl.String),
            "event_id": [f"{x:016x}" for x in rng.integers(0, 2**63, n)],
        }
    )


def _product_sessions(ev: pl.DataFrame) -> pl.DataFrame:
    human = ev.filter(~pl.col("user_agent").str.contains(BOT_UA_PATTERN)).unique("event_id")
    return (
        human.sort("anonymous_id", "event_ts")
        .with_columns(gap=pl.col("event_ts").diff().over("anonymous_id"))
        .with_columns(new=(pl.col("gap").is_null() | (pl.col("gap") > pl.duration(minutes=30))).cast(pl.Int32))
        .with_columns(session_no=pl.col("new").cum_sum().over("anonymous_id"))
        .group_by("anonymous_id", "session_no")
        .agg(converted=(pl.col("event_type") == "purchase").any())
    )


def _product_conversion(ev: pl.DataFrame) -> float:
    return float(_product_sessions(ev)["converted"].mean())


def _marketing_counts(ev: pl.DataFrame) -> tuple[int, int]:
    g = ev.group_by("anonymous_id", pl.col("event_ts").dt.date().alias("d")).agg(
        converted=(pl.col("event_type") == "purchase").any()
    )
    return g.height, int(g["converted"].sum())
