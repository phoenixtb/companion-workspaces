"""Support tickets and product reviews (Season 3, Part 32): Larkspur's unstructured text.

Tickets and reviews are written by customers about real orders in the generator's history (seed 42): a ticket names
the order, one product on it and the delivery city; a review rates one product the customer bought. A ticket is a
thread of messages ("Customer: …", "Agent: …") separated by blank lines. Some customers paste their e-mail address or
phone number into the text, as real customers do, so the text itself holds personal data.

Deterministic for the seed and independent of `make data`: nothing in the raw zone changes. The loader applies
Part 23's suppression list, so an erased customer's tickets and reviews never reach the database.

The evaluation set comes from the same facts with different words: for 60 tickets, a query a support agent might
type, and the IDs of every ticket about the same problem with the same product (one to three tickets).
"""

import random
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import polars as pl

SEED = 42
N_TICKETS = 4_000
N_REVIEWS = 3_000
N_EVAL = 60
FIRST_DAY = datetime(2026, 6, 1, tzinfo=UTC)
LAST_TS = datetime(2026, 10, 4, 23, 59, tzinfo=UTC)  # the lab's history ends with the 5 October extract

# issue -> (subject templates, opening message templates, agent reply templates, eval query templates)
ISSUES: dict[str, tuple[list[str], list[str], list[str], list[str]]] = {
    "late_delivery": (
        ["Order {order_id} is late", "Still waiting for order {order_id}", "Where is my delivery?"],
        ["My order {order_id} was due on {due} and has still not arrived in {city}. It contains {product}, which I "
         "needed for the weekend. The tracking page has said 'out for delivery' for two days.",
         "I ordered {product} and a few other things for delivery to {city}. The promised slot came and went and "
         "nobody turned up. Order number {order_id}."],
        ["Sorry for the wait. The courier for {city} had a backlog after a van breakdown; your parcel is now on "
         "today's route.", "I have checked with the delivery hub: order {order_id} missed its slot and is booked "
         "for the next van. I've refunded the delivery charge."],
        ["parcel with {product} never turned up on the promised day in {city}",
         "customer chasing a {city} delivery that is days overdue ({product})"],
    ),
    "damaged_item": (
        ["{product} arrived damaged", "Broken item in order {order_id}"],
        ["The box was crushed and the {product} inside was broken when it arrived in {city}. I have photos of the "
         "packaging.", "When I opened order {order_id} the {product} was cracked and unusable. The outer box "
         "looked fine, so it must have been packed badly."],
        ["I'm sorry the {product} arrived damaged. A replacement is on its way and you don't need to return the "
         "broken one.", "Thanks for the photos. I've issued a refund for the {product} to your original payment "
         "method."],
        ["{product} came smashed inside a crushed box", "breakage on delivery: {product} unusable"],
    ),
    "arrived_warm": (
        ["Chilled items arrived warm", "{product} was not cold on delivery"],
        ["The {product} in order {order_id} was warm when the driver handed it over in {city}. The cool bag was "
         "missing and the ice packs had melted. I don't feel safe eating it.",
         "Our delivery in {city} came three hours late and the {product} was at room temperature. Isn't this "
         "supposed to stay chilled the whole way?"],
        ["Food safety comes first: please throw the {product} away. I've refunded it and reported the van's "
         "temperature log to the cold-chain team.", "You're right to be concerned. The van's chiller logged a "
         "temperature excursion on that route; a full refund for the chilled items is on its way."],
        ["{product} handed over at room temperature, cold chain broken",
         "chilled {product} no longer cold when delivered in {city}"],
    ),
    "wrong_item": (
        ["Wrong item received", "I got {other} instead of {product}"],
        ["I ordered {product} but the bag contained {other}. Order {order_id}, delivered to {city}.",
         "Order {order_id} had {other} in it, which I never ordered, and the {product} was missing."],
        ["Apologies for the mix-up. The picker scanned {other} by mistake; the correct {product} will come with "
         "tomorrow's van and you can keep the other item.", "I've refunded the {product} and arranged a "
         "collection for the {other}."],
        ["substitution error: sent {other} rather than the {product} that was ordered",
         "customer received {other} although they bought {product}"],
    ),
    "missing_item": (
        ["Item missing from order {order_id}", "{product} not in my delivery"],
        ["The receipt for order {order_id} lists {product}, but it wasn't in any of the bags.",
         "Everything arrived except the {product}. I was charged for it."],
        ["Sorry about that. I've refunded the {product}; it should show on your statement in 3-5 working days.",
         "The packing record shows the {product} was out of stock at picking and should have been removed from "
         "the bill. I've refunded it."],
        ["charged for {product} but it was not in the bags", "{product} listed on the receipt yet never delivered"],
    ),
    "refund_delay": (
        ["Refund not received", "Still no refund for order {order_id}"],
        ["You confirmed a refund for the {product} on order {order_id} two weeks ago, but nothing has reached my "
         "account.", "I returned the {product} at the {city} store and was told the money would be back in five "
         "days. It has been twelve."],
        ["I can see the refund was sent to the payment provider on time; it can take up to 10 working days to "
         "appear. Reference for your bank: RF-{order_id}.", "The refund was stuck because the card had expired. "
         "I've re-issued it as store credit plus a goodwill voucher."],
        ["money back for {product} still missing weeks after it was promised",
         "refund for returned {product} not credited to the customer's account"],
    ),
    "promo_not_applied": (
        ["Promotion not applied", "Discount missing on {product}"],
        ["The app showed {product} at the promotional price, but I was charged the full price on order "
         "{order_id}.", "The offer on {product} didn't come off at checkout even though the banner was still "
         "showing."],
        ["The promotion was edited after launch and the app cached the old banner. I've refunded the "
         "difference.", "You're right, the offer should have applied. I've credited the discount to your "
         "account."],
        ["offer price for {product} shown in the app but full price was taken",
         "promotional discount on {product} did not apply at checkout"],
    ),
    "double_charge": (
        ["Charged twice", "Two payments for order {order_id}"],
        ["My bank shows two payments for order {order_id}. I only placed it once, for the {product} and a few "
         "other things.", "I was charged twice when the app froze at payment. Only one order arrived ({order_id})."],
        ["The second payment was an authorisation that will drop off within a few days; it was never captured.",
         "I can see a duplicate capture on order {order_id}. I've refunded the second payment."],
        ["customer billed two times for one order that included {product}",
         "duplicate payment taken after the app hung (order with {product})"],
    ),
    "loyalty_points": (
        ["Loyalty points missing", "No points for order {order_id}"],
        ["I didn't get any loyalty points for order {order_id}, which included {product}.",
         "My points balance hasn't changed since my last order ({order_id}). Are online orders excluded?"],
        ["Points for online orders are added when the order is fulfilled. I've added them manually now.",
         "Your loyalty card wasn't linked to the app account. I've linked it and back-dated the points."],
        ["no reward points credited for an online order with {product}",
         "loyalty balance unchanged after buying {product} online"],
    ),
    "quality_issue": (
        ["Poor quality {product}", "Problem with {product}"],
        ["The {product} stopped working properly after a week. I expected better.",
         "The {product} was past its best on arrival, well before the date on the label."],
        ["Sorry the {product} didn't meet the standard. I've refunded it and passed your notes to the buying "
         "team.", "Thanks for flagging this; we've asked the supplier to check that batch of {product}."],
        ["{product} faulty or spoiled long before it should be", "complaint about the quality of {product}"],
    ),
}  # fmt: skip

CONTACT = [
    "You can reach me on {phone} if that's easier.",
    "Please reply to {email}, not the address on the account.",
    "Call me on {phone} or e-mail {email}.",
]
FOLLOW_UP = [
    "Thanks, but I'd also like to know why this happened, because it's the second time this month.",
    "I've checked my account and nothing has changed yet. Can you confirm when it will be sorted?",
    "Thank you for the quick reply. One more thing: the delivery driver was very polite.",
]
CLOSING = [
    "Glad we could sort this out. I've added a note to your account so the team can see the history.",
    "I'll close this ticket now; reply to it within 14 days and it reopens.",
]
REVIEW = {
    5: (["Excellent", "Love it", "Exactly what I wanted"],
        ["The {product} is excellent. {aspect_good} Will buy again.", "Really pleased with the {product}. "
         "{aspect_good}"]),
    4: (["Very good", "Good value"],
        ["Good {product} for the price. {aspect_good} Only small niggle: {aspect_bad_lc}",
         "{aspect_good} I'd give five stars if the {product} were a bit cheaper."]),
    3: (["OK", "Average"],
        ["The {product} is fine, nothing special. {aspect_bad}", "Mixed feelings about the {product}. "
         "{aspect_good} {aspect_bad}"]),
    2: (["Disappointing", "Not great"],
        ["Disappointed with the {product}. {aspect_bad}", "Expected more from the {product}. {aspect_bad}"]),
    1: (["Avoid", "Terrible"],
        ["Would not recommend the {product}. {aspect_bad} Asked for a refund.", "The {product} was a waste of "
         "money. {aspect_bad}"]),
}  # fmt: skip
ASPECT = {  # category -> (good, bad)
    "Bakery": ("Fresh and soft, lasted the whole week.", "It went stale within two days."),
    "Dairy & Chilled": ("Tastes fresh and the date was a long way off.", "It arrived close to its use-by date."),
    "Fresh Produce": ("Ripe and tasty, no bruising.", "Half of it was bruised or overripe."),
    "Beverages": ("Great flavour, not too sweet.", "Far too sweet and the cap leaked."),
    "Pantry": ("Good quality and the pack lasts ages.", "The pack was half air."),
    "Household": ("Works well and smells nice.", "Didn't clean as well as my usual brand."),
    "Personal Care": ("Gentle on my skin.", "Gave me a rash after a few uses."),
    "Home & Kitchen": ("Sturdy and well made.", "The handle came loose in the first week."),
    "Apparel": ("Fits true to size and washes well.", "The colour ran in the first wash."),
    "Electronics": ("Battery life is better than advertised.", "Stopped charging after a month."),
}


@dataclass(frozen=True)
class Support:
    tickets: pl.DataFrame
    reviews: pl.DataFrame
    eval_queries: pl.DataFrame  # query_id, query, expected_ids (list), issue, sku


def _due(created: datetime) -> str:
    return (created + timedelta(days=2)).strftime("%-d %B")


def build(contact: Callable[[str], dict], seed: int = SEED) -> Support:
    """Tickets, reviews and the evaluation queries. `contact(customer_id)` gives the e-mail and phone a customer
    might paste into a message (Part 23's deterministic CRM fields)."""
    from datagen import reference, sales

    ref = reference.build(42, 1.0)
    s = sales.build(42, 1.0, ref)
    rng = random.Random(seed)
    products = {r["sku"]: r for r in ref.products.select("sku", "product_name", "category").to_dicts()}
    chilled = {r["sku"] for r in ref.products.filter(pl.col("is_chilled")).select("sku").to_dicts()}
    lines = s.order_lines.group_by("order_id").agg(pl.col("sku"))
    orders = (
        s.orders.filter(~pl.col("cancelled") & pl.col("fulfilled_at").is_not_null())
        .filter(pl.col("fulfilled_at") < LAST_TS - timedelta(days=3))
        .join(lines, on="order_id")
        .sort("order_id")
        .select("order_id", "customer_id", "country", "created_at", "fulfilled_at", "sku")
        .to_dicts()
    )
    cities = {cc: sorted(set(ref.stores.filter(pl.col("country") == cc)["city"])) for cc in ("GB", "IN", "SG")}
    all_skus = sorted(products)

    tickets = []
    for i, o in enumerate(rng.sample(orders, N_TICKETS)):
        skus = o["sku"]
        issue = rng.choice(sorted(ISSUES))
        if issue == "arrived_warm":  # only chilled goods arrive warm; otherwise the item is damaged
            warm = [k for k in skus if k in chilled]
            issue, sku = ("arrived_warm", warm[0]) if warm else ("damaged_item", skus[0])
        else:
            sku = rng.choice(skus)
        other = products[rng.choice(all_skus)]["product_name"]
        city = rng.choice(cities[o["country"]])
        created = min(o["fulfilled_at"].replace(tzinfo=UTC) + timedelta(hours=rng.randint(2, 96)), LAST_TS)
        slots = {"order_id": o["order_id"], "product": products[sku]["product_name"], "other": other, "city": city,
                 "due": _due(o["created_at"].replace(tzinfo=UTC))}  # fmt: skip
        subj, opening, reply, _ = ISSUES[issue]
        msgs = ["Customer: " + rng.choice(opening).format(**slots)]
        if rng.random() < 0.3:  # personal data inside the text
            c = contact(o["customer_id"])
            msgs[0] += " " + rng.choice(CONTACT).format(email=c["email"], phone=c["phone"])
        msgs.append("Agent: " + rng.choice(reply).format(**slots))
        if rng.random() < 0.5:
            msgs.append("Customer: " + rng.choice(FOLLOW_UP))
            msgs.append("Agent: " + rng.choice(CLOSING))
        age = LAST_TS - created
        status = "closed" if age > timedelta(days=7) else rng.choice(["open", "pending", "closed"])
        tickets.append({
            "ticket_id": f"T{o['country']}{i + 1:06d}", "customer_id": o["customer_id"], "order_id": o["order_id"],
            "country": o["country"], "city": city, "issue": issue, "sku": sku,
            "subject": rng.choice(subj).format(**slots),
            "body": "\n\n".join(msgs), "status": status, "created_at": created,
            "updated_at": created + timedelta(minutes=rng.randint(5, 600)),
        })  # fmt: skip

    reviews = []
    for i, o in enumerate(rng.sample(orders, N_REVIEWS)):
        sku = rng.choice(o["sku"])
        p = products[sku]
        rating = rng.choices([5, 4, 3, 2, 1], weights=[40, 25, 15, 10, 10])[0]
        good, bad = ASPECT[p["category"]]
        titles, bodies = REVIEW[rating]
        created = min(o["fulfilled_at"].replace(tzinfo=UTC) + timedelta(days=rng.randint(1, 14)), LAST_TS)
        body = rng.choice(bodies).format(product=p["product_name"], aspect_good=good, aspect_bad=bad,
                                         aspect_bad_lc=bad[0].lower() + bad[1:])  # fmt: skip
        reviews.append({
            "review_id": f"R{o['country']}{i + 1:06d}", "customer_id": o["customer_id"], "sku": sku,
            "country": o["country"], "rating": rating, "title": rng.choice(titles), "body": body,
            "status": "published", "created_at": created, "updated_at": created,
        })  # fmt: skip

    t = pl.DataFrame(tickets)
    groups = t.group_by("issue", "sku").agg(pl.col("ticket_id").sort())
    expected = {(g["issue"], g["sku"]): g["ticket_id"] for g in groups.to_dicts()}
    evals = []  # targets: problems at most three tickets describe, so recall@5 can reach 1
    targets = [r for r in tickets if len(expected[(r["issue"], r["sku"])]) <= 3]
    for j, row in enumerate(rng.sample(targets, N_EVAL)):
        templates = ISSUES[row["issue"]][3]
        q = rng.choice(templates).format(product=products[row["sku"]]["product_name"], other="another product",
                                         city=row["city"])  # fmt: skip
        evals.append({"query_id": f"Q{j + 1:03d}", "query": q, "country": row["country"], "issue": row["issue"],
                      "sku": row["sku"], "expected_ids": expected[(row["issue"], row["sku"])]})  # fmt: skip
    return Support(t, pl.DataFrame(reviews), pl.DataFrame(evals))
