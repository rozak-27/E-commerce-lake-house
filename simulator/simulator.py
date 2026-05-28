"""
E-Commerce Fake Data Simulator
Generates realistic e-commerce events and publishes them to Kafka topics.

Usage:
    python simulator.py                         # default 5 events/sec
    python simulator.py --rate 10               # 10 events/sec
    python simulator.py --rate 5 --duration 60  # run for 60s then stop
    python simulator.py --dry-run               # print to terminal, no Kafka needed
"""
import json
import random
import time
import uuid
import argparse
import os
from datetime import datetime
from faker import Faker
from dotenv import load_dotenv

load_dotenv()
fake = Faker("id_ID")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPICS = {
    "user":    "user-events",
    "product": "product-events",
    "order":   "order-events",
    "payment": "payment-events",
    "search":  "search-events",
}

CATEGORIES = ["Electronics", "Fashion", "Food", "Books", "Beauty"]
PRODUCTS = {
    "Electronics": ["iPhone 15", "Samsung Galaxy S24", "Laptop ASUS", "Headphone Sony", "Tablet iPad"],
    "Fashion":     ["Batik Kemeja", "Sepatu Nike Air", "Tas Gucci", "Dompet Coach", "Jaket Levi's"],
    "Food":        ["Kopi Torabika", "Mie Instan Indomie", "Coklat Silverqueen", "Susu Ultra", "Keripik Piattos"],
    "Books":       ["Laskar Pelangi", "Atomic Habits", "Pemrograman Python", "Novel Tere Liye", "Clean Code"],
    "Beauty":      ["Skincare Wardah", "Lipstik Emina", "Parfum Zara", "Serum Skintific", "Sunscreen Erha"],
}
EVENT_WEIGHTS   = {"view": 40, "search": 25, "add_to_cart": 20, "checkout": 10, "review": 5}
PAYMENT_METHODS = ["credit_card", "debit_card", "bank_transfer", "gopay", "ovo", "dana", "shopeepay"]
PLATFORMS       = ["web", "mobile_android", "mobile_ios", "app"]


# ── Kafka Producer ───────────────────────────────────────
def make_producer():
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable

    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                api_version=(3, 6, 0),
            )
            print(f"   ✅ Kafka connected: {KAFKA_BOOTSTRAP}")
            return producer
        except NoBrokersAvailable:
            wait = attempt * 3
            print(f"   ⏳ Kafka not ready, retrying in {wait}s... ({attempt}/{max_retries})")
            time.sleep(wait)

    raise RuntimeError(f"❌ Cannot connect to Kafka after {max_retries} attempts. "
                       "Pastikan Kafka sudah jalan atau gunakan --dry-run untuk testing.")


# ── Event Generators ─────────────────────────────────────
def now_iso():
    return datetime.utcnow().isoformat() + "Z"

def base_event(event_type: str, user_id: str) -> dict:
    """Field yang selalu ada di setiap event"""
    return {
        "event_id":   str(uuid.uuid4()),
        "event_type": event_type,
        "user_id":    user_id,
        "session_id": str(uuid.uuid4())[:8],
        "timestamp":  now_iso(),
        "platform":   random.choice(PLATFORMS),
        "ip_address": fake.ipv4(),
        "user_agent": fake.user_agent(),
    }

def gen_view_event(user_id: str, category: str, product: str, price: float) -> dict:
    return {
        **base_event("view", user_id),
        "product_id":   f"p_{random.randint(100, 999)}",
        "product_name": product,
        "category":     category,
        "price":        price,
    }

def gen_search_event(user_id: str) -> dict:
    return {
        **base_event("search", user_id),
        "query":        fake.word(),
        "result_count": random.randint(0, 200),
    }

def gen_add_to_cart_event(user_id: str, category: str, product: str, price: float) -> dict:
    return {
        **base_event("add_to_cart", user_id),
        "product_id":   f"p_{random.randint(100, 999)}",
        "product_name": product,
        "category":     category,
        "price":        price,
        "quantity":     random.randint(1, 5),
    }

def gen_checkout_event(user_id: str, category: str, product: str, price: float) -> dict:
    qty = random.randint(1, 5)
    return {
        **base_event("checkout", user_id),
        "product_id":     f"p_{random.randint(100, 999)}",
        "product_name":   product,
        "category":       category,
        "price":          price,
        "order_id":       f"ord_{uuid.uuid4().hex[:8]}",
        "quantity":       qty,
        "total_amount":   round(price * qty, 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "shipping_city":  fake.city(),
    }

def gen_review_event(user_id: str, category: str, product: str, price: float) -> dict:
    return {
        **base_event("review", user_id),
        "product_id":   f"p_{random.randint(100, 999)}",
        "product_name": product,
        "category":     category,
        "price":        price,
        "rating":       random.randint(1, 5),
        "review_text":  fake.sentence(),
    }

def gen_payment_event(order_id: str, user_id: str, amount: float, method: str) -> dict:
    """Dibuat otomatis setiap ada checkout"""
    return {
        "event_id":       str(uuid.uuid4()),
        "order_id":       order_id,
        "user_id":        user_id,
        "payment_method": method,
        "amount":         amount,
        # checkout 75% sukses, 15% gagal, 10% pending
        "status":         random.choices(
                              ["success", "failed", "pending"],
                              weights=[75, 15, 10]
                          )[0],
        "timestamp":      now_iso(),
    }


# ── Event Router ─────────────────────────────────────────
TOPIC_MAP = {
    "view":        TOPICS["user"],
    "search":      TOPICS["search"],
    "add_to_cart": TOPICS["product"],
    "checkout":    TOPICS["order"],
    "review":      TOPICS["product"],
}

GENERATORS = {
    "view":        gen_view_event,
    "search":      gen_search_event,
    "add_to_cart": gen_add_to_cart_event,
    "checkout":    gen_checkout_event,
    "review":      gen_review_event,
}

def generate_events() -> list[tuple[str, str, dict]]:
    """
    Return list of (topic, key, payload).
    Checkout menghasilkan 2 events: order + payment.
    """
    event_type = random.choices(
        list(EVENT_WEIGHTS.keys()),
        weights=list(EVENT_WEIGHTS.values())
    )[0]

    user_id  = f"u_{random.randint(1000, 9999)}"
    category = random.choice(CATEGORIES)
    product  = random.choice(PRODUCTS[category])
    price    = round(random.uniform(15_000, 8_000_000), 2)

    # search tidak butuh product/price
    if event_type == "search":
        event = gen_search_event(user_id)
    else:
        event = GENERATORS[event_type](user_id, category, product, price)

    results = [(TOPIC_MAP[event_type], user_id, event)]

    # checkout → otomatis tambah payment event
    if event_type == "checkout":
        payment = gen_payment_event(
            order_id=event["order_id"],
            user_id=user_id,
            amount=event["total_amount"],
            method=event["payment_method"],
        )
        results.append((TOPICS["payment"], user_id, payment))

    return results


# ── Main Runner ──────────────────────────────────────────
def run(events_per_second: int = 5, duration_seconds: int = None, dry_run: bool = False):
    mode = "🖥️  DRY RUN (print only)" if dry_run else f"📡 Kafka → {KAFKA_BOOTSTRAP}"
    print(f"\n🚀 E-Commerce Simulator")
    print(f"   Mode    : {mode}")
    print(f"   Rate    : {events_per_second} events/sec")
    print(f"   Duration: {'unlimited' if not duration_seconds else f'{duration_seconds}s'}")
    print("─" * 50)

    producer = None if dry_run else make_producer()

    count = 0
    start = time.time()

    try:
        while True:
            batch_start = time.time()

            for _ in range(events_per_second):
                for topic, key, payload in generate_events():
                    if dry_run:
                        etype = payload.get("event_type", "payment")
                        print(f"  [{etype:12s}] → {topic:20s} | user: {key} | {payload['timestamp']}")
                    else:
                        producer.send(topic, key=key, value=payload)
                    count += 1

            if not dry_run:
                producer.flush()

            elapsed = time.time() - start
            if not dry_run:
                print(f"  [{elapsed:6.1f}s] total sent: {count} events")

            if duration_seconds and elapsed >= duration_seconds:
                print(f"\n✅ Duration reached ({duration_seconds}s). Total: {count} events")
                break

            # jaga supaya tetap 1 batch per detik
            sleep_time = max(0, 1.0 - (time.time() - batch_start))
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print(f"\n⛔ Stopped. Total events sent: {count}")
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-Commerce Event Simulator")
    parser.add_argument("--rate",     type=int,  default=5,     help="Events per second (default: 5)")
    parser.add_argument("--duration", type=int,  default=None,  help="Stop after N seconds")
    parser.add_argument("--dry-run",  action="store_true",      help="Print events to terminal only, no Kafka needed")
    args = parser.parse_args()

    run(
        events_per_second=args.rate,
        duration_seconds=args.duration,
        dry_run=args.dry_run,
    )