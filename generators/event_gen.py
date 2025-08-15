import json, random, time, uuid, click
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

SKUS = [f"SKU-{i:05d}" for i in range(1, 501)]

def now_iso(dt=None):
    return (dt or datetime.now(timezone.utc)).isoformat()

def make_page_view(user_id, session_hint, ts, ref=None):
    return {
        "event_type": "page_view",
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "ts": ts,
        "url": fake.uri_path(deep=2),
        "referrer": ref or random.choice(["google", "facebook", "twitter", "direct", "newsletter", "affiliate"]),
        "ua": fake.user_agent(),
        "session_hint": session_hint
    }

def make_add_to_cart(user_id, ts):
    sku = random.choice(SKUS)
    qty = random.choice([1,1,1,2,3])
    price = round(random.uniform(5, 200), 2)
    return {
        "event_type": "add_to_cart",
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "ts": ts,
        "sku": sku,
        "qty": qty,
        "price": price
    }

def make_order(user_id, ts, cart_items=None):
    if not cart_items:
        # make 1-3 line items if cart is empty
        cart_items = []
        for _ in range(random.randint(1,3)):
            price = round(random.uniform(5, 200), 2)
            cart_items.append({
                "sku": random.choice(SKUS),
                "qty": random.choice([1,1,2,3]),
                "price": price
            })
    total = round(sum(it["qty"] * it["price"] for it in cart_items), 2)
    return {
        "event_type": "order_placed",
        "event_id": str(uuid.uuid4()),
        "order_id": str(uuid.uuid4()),
        "user_id": user_id,
        "ts": ts,
        "total": total,
        "line_items": cart_items
    }

def jitter_ts(base_dt, skew_sec):
    return base_dt + timedelta(seconds=random.randint(-skew_sec, skew_sec))

@click.command()
@click.option("--bootstrap", default="localhost:19092", show_default=True, help="Kafka bootstrap servers")
@click.option("--topic", default="events.raw", show_default=True)
@click.option("--eps", default=120, show_default=True, help="Events per minute (approx)")
@click.option("--users", default=200, show_default=True, help="Distinct users to rotate through")
@click.option("--dup_pct", default=1.5, show_default=True, help="Percent of events to duplicate")
@click.option("--skew", default=300, show_default=True, help="Max timestamp skew in seconds (±)")
@click.option("--backdate", default=None, help="Backdate base day (YYYY-MM-DD) for events (for backfill drills)")
def main(bootstrap, topic, eps, users, dup_pct, skew, backdate):
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap],
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
        retries=10,
        acks='all'
    )
    print(f"Producing to {bootstrap} topic={topic} … Ctrl+C to stop")

    # simple per-user session/cart state
    sessions = {}  # user_id -> session_hint
    carts = {}     # user_id -> list of line_items

    base = datetime.strptime(backdate, "%Y-%m-%d").replace(tzinfo=timezone.utc) if backdate else None

    # target sleep between events
    interval = 60.0 / max(1, eps)

    try:
        while True:
            # pick a user
            user_id = f"user-{random.randint(1, users)}"

            # ensure a session_hint per user (rotate every ~30 min)
            if user_id not in sessions or random.random() < 1/1800.0:
                sessions[user_id] = str(uuid.uuid4())

            # choose an event type with realistic proportions
            r = random.random()
            base_dt = base or datetime.now(timezone.utc)
            ts = now_iso(jitter_ts(base_dt, skew))

            if r < 0.75:
                event = make_page_view(user_id, sessions[user_id], ts)
            elif r < 0.95:
                event = make_add_to_cart(user_id, ts)
                # stash to cart
                carts.setdefault(user_id, []).append({
                    "sku": event["sku"],
                    "qty": event["qty"],
                    "price": event["price"]
                })
            else:
                # ~5% orders overall; if cart has items, use them
                items = carts.get(user_id, [])
                event = make_order(user_id, ts, items if items else None)
                carts[user_id] = []  # clear cart after purchase

            # send main event
            producer.send(topic, key=event["event_id"], value=event)

            # occasional duplicate (1–2%)
            if random.random() < (dup_pct / 100.0):
                producer.send(topic, key=event["event_id"], value=event)

            # keep the pipe even
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nStopping…")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
