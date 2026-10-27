"""Generate the Larkspur raw zone.

uv run python -m datagen                      # full lab size into data/raw
uv run python -m datagen --scale 0.1 --store-fraction 0.1 --out /tmp/raw
"""

import argparse
import time
from pathlib import Path

from datagen import clickstream, incident, inventory, manifest, output, psp, reference, sales, suppliers


def main() -> None:
    p = argparse.ArgumentParser(prog="datagen", description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--scale", type=float, default=1.0, help="multiplier on baskets, orders and sessions")
    p.add_argument("--store-fraction", type=float, default=1.0, help="share of the 600 stores to generate")
    p.add_argument("--out", type=Path, default=Path("data/raw"))
    p.add_argument("--docs", type=Path, help="also write the defect manifest as Markdown here")
    a = p.parse_args()

    started = time.monotonic()

    def step(name: str) -> None:
        print(f"[{time.monotonic() - started:6.1f}s] {name}", flush=True)

    step("reference data")
    ref = reference.build(a.seed, a.store_fraction)
    step("sales")
    sal = sales.build(a.seed, a.scale, ref, a.store_fraction)
    step("POS events and the UK revenue calibration")
    events, waterfall = incident.calibrate(a.seed, sal, ref.fx_rates)
    step("inventory")
    inv = inventory.build(a.seed, ref)
    step("suppliers")
    sup = suppliers.build(a.seed, ref)
    step("clickstream")
    clk = clickstream.build(a.seed, sal)
    step("PSP settlements")
    settle = psp.build(a.seed, sal, ref.fx_rates)
    step(f"writing {a.out}")
    output.write_all(a.out, ref, sal, events, inv, sup, clk, settle)
    m = manifest.build(
        {"seed": a.seed, "scale": a.scale, "store_fraction": a.store_fraction},
        ref,
        sal,
        events,
        waterfall,
        inv,
        sup,
        clk,
        settle,
    )
    manifest.write_json(m, a.out / "_manifest.json")
    if a.docs:
        manifest.write_markdown(m, a.docs)
    r = m["uk_net_revenue_q3"]
    step(
        f"done: UK Q3 net revenue legacy £{r['legacy_board_deck_minor'] / 100:,.2f} vs "
        f"correct £{r['correct_minor'] / 100:,.2f} ({r['gap_share']:+.3%})"
    )


if __name__ == "__main__":
    main()
