import os
import random
from datetime import datetime, timedelta, timezone

from faker import Faker
from pymongo import MongoClient


MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "etl_final")


def get_db():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB]


def generate_user_sessions(fake: Faker, n: int = 1000):
    sessions = []
    for i in range(n):
        user_id = f"user_{random.randint(1, 200)}"
        start = fake.date_time_between(
            start_date="-30d", end_date="now", tzinfo=timezone.utc
        )
        duration_minutes = random.randint(1, 120)
        end = start + timedelta(minutes=duration_minutes)

        pages_pool = [
            "/",
            "/home",
            "/products",
            "/products/42",
            "/products/101",
            "/cart",
            "/checkout",
            "/support",
        ]
        pages_visited = random.sample(
            pages_pool, k=random.randint(2, min(5, len(pages_pool)))
        )

        actions_pool = [
            "login",
            "view_product",
            "add_to_cart",
            "remove_from_cart",
            "checkout_start",
            "checkout_finish",
            "logout",
        ]
        actions = random.sample(
            actions_pool, k=random.randint(2, min(6, len(actions_pool)))
        )

        device = random.choice(
            [
                {"type": "mobile", "os": "iOS", "browser": "Safari"},
                {"type": "mobile", "os": "Android", "browser": "Chrome"},
                {"type": "desktop", "os": "Windows", "browser": "Chrome"},
                {"type": "desktop", "os": "macOS", "browser": "Safari"},
            ]
        )

        session = {
            "session_id": f"sess_{i+1:05d}",
            "user_id": user_id,
            "start_time": start.isoformat(),
            "end_time": end.isoformat(),
            "pages_visited": pages_visited,
            "device": device,
            "actions": actions,
        }
        sessions.append(session)
    return sessions


def generate_event_logs(fake: Faker, n: int = 2000):
    logs = []
    event_types = ["click", "view", "scroll", "purchase", "error", "login", "logout"]
    for i in range(n):
        timestamp = fake.date_time_between(
            start_date="-30d", end_date="now", tzinfo=timezone.utc
        )
        event_type = random.choice(event_types)
        details = {
            "path": random.choice(
                ["/", "/home", "/products", "/products/42", "/checkout", "/support"]
            ),
            "info": fake.sentence(nb_words=6),
        }
        logs.append(
            {
                "event_id": f"evt_{i+1:05d}",
                "timestamp": timestamp.isoformat(),
                "event_type": event_type,
                "details": details,
            }
        )
    return logs


def generate_support_tickets(fake: Faker, n: int = 500):
    tickets = []
    statuses = ["open", "in_progress", "resolved", "closed"]
    issue_types = ["payment", "delivery", "product_quality", "account", "other"]

    for i in range(n):
        user_id = f"user_{random.randint(1, 200)}"
        status = random.choice(statuses)
        issue_type = random.choice(issue_types)

        created_at = fake.date_time_between(
            start_date="-30d", end_date="-1d", tzinfo=timezone.utc
        )

        # Для открытых тикетов updated_at близко к now, для закрытых — позже created_at
        if status in ("resolved", "closed"):
            resolution_delta = timedelta(hours=random.randint(1, 72))
            updated_at = created_at + resolution_delta
        else:
            updated_at = fake.date_time_between(
                start_date=created_at, end_date="now", tzinfo=timezone.utc
            )

        messages = []
        n_messages = random.randint(1, 5)
        current_time = created_at
        for j in range(n_messages):
            sender = "user" if j % 2 == 0 else "support"
            current_time += timedelta(hours=random.randint(1, 12))
            messages.append(
                {
                    "sender": sender,
                    "message": fake.sentence(nb_words=10),
                    "timestamp": current_time.isoformat(),
                }
            )

        tickets.append(
            {
                "ticket_id": f"ticket_{i+1:05d}",
                "user_id": user_id,
                "status": status,
                "issue_type": issue_type,
                "messages": messages,
                "created_at": created_at.isoformat(),
                "updated_at": updated_at.isoformat(),
            }
        )
    return tickets


def generate_user_recommendations(fake: Faker, n_users: int = 200):
    recs = []
    for i in range(1, n_users + 1):
        user_id = f"user_{i}"
        n_products = random.randint(3, 10)
        products = [f"prod_{random.randint(1, 500)}" for _ in range(n_products)]
        last_updated = fake.date_time_between(
            start_date="-7d", end_date="now", tzinfo=timezone.utc
        )
        recs.append(
            {
                "user_id": user_id,
                "recommended_products": products,
                "last_updated": last_updated.isoformat(),
            }
        )
    return recs


def generate_moderation_queue(fake: Faker, n: int = 500):
    reviews = []
    moderation_statuses = ["pending", "approved", "rejected"]
    flags_pool = ["contains_images", "contains_links", "toxicity_suspected"]

    for i in range(n):
        user_id = f"user_{random.randint(1, 200)}"
        product_id = f"prod_{random.randint(1, 500)}"
        rating = random.randint(1, 5)
        submitted_at = fake.date_time_between(
            start_date="-30d", end_date="now", tzinfo=timezone.utc
        )
        moderation_status = random.choice(moderation_statuses)
        n_flags = random.choice([0, 1, 2])
        flags = random.sample(flags_pool, k=n_flags) if n_flags > 0 else []

        reviews.append(
            {
                "review_id": f"rev_{i+1:05d}",
                "user_id": user_id,
                "product_id": product_id,
                "review_text": fake.text(max_nb_chars=120),
                "rating": rating,
                "moderation_status": moderation_status,
                "flags": flags,
                "submitted_at": submitted_at.isoformat(),
            }
        )
    return reviews


def seed_mongo():
    fake = Faker()
    db = get_db()

    print(f"Using MongoDB at {MONGO_URI}, database '{MONGO_DB}'")

    collections = [
        "UserSessions",
        "EventLogs",
        "SupportTickets",
        "UserRecommendations",
        "ModerationQueue",
    ]
    for name in collections:
        db[name].delete_many({})

    user_sessions = generate_user_sessions(fake)
    event_logs = generate_event_logs(fake)
    support_tickets = generate_support_tickets(fake)
    user_recommendations = generate_user_recommendations(fake)
    moderation_queue = generate_moderation_queue(fake)

    db.UserSessions.insert_many(user_sessions)
    db.EventLogs.insert_many(event_logs)
    db.SupportTickets.insert_many(support_tickets)
    db.UserRecommendations.insert_many(user_recommendations)
    db.ModerationQueue.insert_many(moderation_queue)

    print("Inserted documents:")
    print(f"  UserSessions: {len(user_sessions)}")
    print(f"  EventLogs: {len(event_logs)}")
    print(f"  SupportTickets: {len(support_tickets)}")
    print(f"  UserRecommendations: {len(user_recommendations)}")
    print(f"  ModerationQueue: {len(moderation_queue)}")


if __name__ == "__main__":
    seed_mongo()

