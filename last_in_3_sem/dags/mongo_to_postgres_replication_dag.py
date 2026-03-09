import json
import os
from datetime import datetime
from typing import Any, Dict, List
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pymongo import MongoClient
import pandas as pd


MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "etl_final")
POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_analytics")


default_args = {
    "owner": "etl_final_project",
    "depends_on_past": False,
    "retries": 1,
}


def _get_mongo_collection(name: str):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    return db[name]


def _ensure_raw_tables():
    """Создание таблиц в схеме raw, если их ещё нет."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql_statements = [
        """
        CREATE SCHEMA IF NOT EXISTS raw;
        """,
        """
        CREATE TABLE IF NOT EXISTS raw.user_sessions (
            session_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            start_time TIMESTAMPTZ NOT NULL,
            end_time TIMESTAMPTZ NOT NULL,
            session_duration_seconds INTEGER NOT NULL,
            pages_visited JSONB,
            device JSONB,
            actions JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS raw.event_logs (
            event_id TEXT PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL,
            event_type TEXT NOT NULL,
            details JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS raw.support_tickets (
            ticket_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            status TEXT NOT NULL,
            issue_type TEXT NOT NULL,
            messages JSONB,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            resolution_time_seconds INTEGER,
            messages_count INTEGER
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS raw.user_recommendations (
            user_id TEXT PRIMARY KEY,
            recommended_products JSONB,
            last_updated TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS raw.moderation_queue (
            review_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            review_text TEXT,
            rating INTEGER,
            moderation_status TEXT,
            flags JSONB,
            submitted_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        """,
    ]

    for sql in sql_statements:
        hook.run(sql)


with DAG(
    dag_id="mongo_to_postgres_replication",
    start_date=datetime(2026, 3, 3),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["mongo", "postgres", "replication"],
) as dag:
    @task
    def ensure_raw_schema():
        _ensure_raw_tables()

    def extract_user_sessions() -> List[Dict[str, Any]]:
        col = _get_mongo_collection("UserSessions")
        docs = list(col.find({}))
        for d in docs:
            d.pop("_id", None)
        return docs

    def transform_user_sessions(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not docs:
            return []

        df = pd.DataFrame(docs)
        df["start_time"] = pd.to_datetime(df["start_time"], utc=True)
        df["end_time"] = pd.to_datetime(df["end_time"], utc=True)
        df["session_duration_seconds"] = (
            (df["end_time"] - df["start_time"]).dt.total_seconds().astype("Int64")
        )
        df = df.drop_duplicates(subset=["session_id"])

        for col_name in ["pages_visited", "device", "actions"]:
            if col_name in df.columns:
                df[col_name] = df[col_name].apply(
                    lambda x: json.dumps(x) if x is not None else None
                )

        records = df.to_dict(orient="records")

        normalized: List[Dict[str, Any]] = []
        for rec in records:
            for key, value in list(rec.items()):
                if hasattr(value, "to_pydatetime"):
                    rec[key] = value.to_pydatetime()
                    continue
                try:
                    if value is not None and pd.isna(value):
                        rec[key] = None
                except TypeError:
                    pass
            normalized.append(rec)

        return normalized

    def load_user_sessions(records: List[Dict[str, Any]]):
        if not records:
            return
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
        INSERT INTO raw.user_sessions (
            session_id,
            user_id,
            start_time,
            end_time,
            session_duration_seconds,
            pages_visited,
            device,
            actions,
            updated_at
        )
        VALUES (
            %(session_id)s,
            %(user_id)s,
            %(start_time)s,
            %(end_time)s,
            %(session_duration_seconds)s,
            %(pages_visited)s::jsonb,
            %(device)s::jsonb,
            %(actions)s::jsonb,
            NOW()
        )
        ON CONFLICT (session_id) DO UPDATE SET
            user_id = EXCLUDED.user_id,
            start_time = EXCLUDED.start_time,
            end_time = EXCLUDED.end_time,
            session_duration_seconds = EXCLUDED.session_duration_seconds,
            pages_visited = EXCLUDED.pages_visited,
            device = EXCLUDED.device,
            actions = EXCLUDED.actions,
            updated_at = NOW();
        """
        for rec in records:
            hook.run(sql, parameters=rec)

    def extract_event_logs() -> List[Dict[str, Any]]:
        col = _get_mongo_collection("EventLogs")
        docs = list(col.find({}))
        for d in docs:
            d.pop("_id", None)
        return docs

    def transform_event_logs(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not docs:
            return []

        df = pd.DataFrame(docs)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.drop_duplicates(subset=["event_id"])
        if "details" in df.columns:
            df["details"] = df["details"].apply(
                lambda x: json.dumps(x) if x is not None else None
            )

        records = df.to_dict(orient="records")

        normalized: List[Dict[str, Any]] = []
        for rec in records:
            for key, value in list(rec.items()):
                if hasattr(value, "to_pydatetime"):
                    rec[key] = value.to_pydatetime()
                    continue
                try:
                    if value is not None and pd.isna(value):  # type: ignore[attr-defined]
                        rec[key] = None
                except TypeError:
                    pass
            normalized.append(rec)

        return normalized

    def load_event_logs(records: List[Dict[str, Any]]):
        if not records:
            return
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
        INSERT INTO raw.event_logs (
            event_id,
            timestamp,
            event_type,
            details,
            updated_at
        )
        VALUES (
            %(event_id)s,
            %(timestamp)s,
            %(event_type)s,
            %(details)s::jsonb,
            NOW()
        )
        ON CONFLICT (event_id) DO UPDATE SET
            timestamp = EXCLUDED.timestamp,
            event_type = EXCLUDED.event_type,
            details = EXCLUDED.details,
            updated_at = NOW();
        """
        for rec in records:
            hook.run(sql, parameters=rec)

    def extract_support_tickets() -> List[Dict[str, Any]]:
        col = _get_mongo_collection("SupportTickets")
        docs = list(col.find({}))
        for d in docs:
            d.pop("_id", None)
        return docs

    def transform_support_tickets(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not docs:
            return []

        df = pd.DataFrame(docs)
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
        df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)

        # время решения только для закрытых/решённых тикетов
        closed_mask = df["status"].isin(["resolved", "closed"])
        df["resolution_time_seconds"] = None
        df.loc[closed_mask, "resolution_time_seconds"] = (
            (df.loc[closed_mask, "updated_at"] - df.loc[closed_mask, "created_at"])
            .dt.total_seconds()
            .astype("Int64")
        )

        # количество сообщений
        if "messages" in df.columns:
            df["messages_count"] = df["messages"].apply(
                lambda x: len(x) if isinstance(x, list) else 0
            )
            df["messages"] = df["messages"].apply(
                lambda x: json.dumps(x) if x is not None else None
            )
        else:
            df["messages_count"] = 0

        df = df.drop_duplicates(subset=["ticket_id"])

        records = df.to_dict(orient="records")

        normalized: List[Dict[str, Any]] = []
        for rec in records:
            for key, value in list(rec.items()):
                if hasattr(value, "to_pydatetime"):
                    rec[key] = value.to_pydatetime()
                    continue
                try:
                    if value is not None and pd.isna(value):  # type: ignore[attr-defined]
                        rec[key] = None
                except TypeError:
                    pass
            normalized.append(rec)

        return normalized

    def load_support_tickets(records: List[Dict[str, Any]]):
        if not records:
            return
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
        INSERT INTO raw.support_tickets (
            ticket_id,
            user_id,
            status,
            issue_type,
            messages,
            created_at,
            updated_at,
            resolution_time_seconds,
            messages_count
        )
        VALUES (
            %(ticket_id)s,
            %(user_id)s,
            %(status)s,
            %(issue_type)s,
            %(messages)s::jsonb,
            %(created_at)s,
            %(updated_at)s,
            %(resolution_time_seconds)s,
            %(messages_count)s
        )
        ON CONFLICT (ticket_id) DO UPDATE SET
            user_id = EXCLUDED.user_id,
            status = EXCLUDED.status,
            issue_type = EXCLUDED.issue_type,
            messages = EXCLUDED.messages,
            created_at = EXCLUDED.created_at,
            resolution_time_seconds = EXCLUDED.resolution_time_seconds,
            messages_count = EXCLUDED.messages_count,
            updated_at = NOW();
        """
        for rec in records:
            hook.run(sql, parameters=rec)

    def extract_user_recommendations() -> List[Dict[str, Any]]:
        col = _get_mongo_collection("UserRecommendations")
        docs = list(col.find({}))
        for d in docs:
            d.pop("_id", None)
        return docs

    def transform_user_recommendations(
        docs: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        if not docs:
            return []

        df = pd.DataFrame(docs)
        df["last_updated"] = pd.to_datetime(df["last_updated"], utc=True)
        if "recommended_products" in df.columns:
            df["recommended_products"] = df["recommended_products"].apply(
                lambda x: json.dumps(x) if x is not None else None
            )
        df = df.drop_duplicates(subset=["user_id"])

        records = df.to_dict(orient="records")

        normalized: List[Dict[str, Any]] = []
        for rec in records:
            for key, value in list(rec.items()):
                if hasattr(value, "to_pydatetime"):
                    rec[key] = value.to_pydatetime()
                    continue
                try:
                    if value is not None and pd.isna(value):
                        rec[key] = None
                except TypeError:
                    pass
            normalized.append(rec)

        return normalized

    def load_user_recommendations(records: List[Dict[str, Any]]):
        if not records:
            return
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
        INSERT INTO raw.user_recommendations (
            user_id,
            recommended_products,
            last_updated,
            updated_at
        )
        VALUES (
            %(user_id)s,
            %(recommended_products)s::jsonb,
            %(last_updated)s,
            NOW()
        )
        ON CONFLICT (user_id) DO UPDATE SET
            recommended_products = EXCLUDED.recommended_products,
            last_updated = EXCLUDED.last_updated,
            updated_at = NOW();
        """
        for rec in records:
            hook.run(sql, parameters=rec)

    def extract_moderation_queue() -> List[Dict[str, Any]]:
        col = _get_mongo_collection("ModerationQueue")
        docs = list(col.find({}))
        for d in docs:
            d.pop("_id", None)
        return docs

    def transform_moderation_queue(
        docs: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        if not docs:
            return []

        df = pd.DataFrame(docs)
        df["submitted_at"] = pd.to_datetime(df["submitted_at"], utc=True)
        if "flags" in df.columns:
            df["flags"] = df["flags"].apply(
                lambda x: json.dumps(x) if x is not None else None
            )
        df = df.drop_duplicates(subset=["review_id"])

        records = df.to_dict(orient="records")

        normalized: List[Dict[str, Any]] = []
        for rec in records:
            for key, value in list(rec.items()):
                if hasattr(value, "to_pydatetime"):
                    rec[key] = value.to_pydatetime()
                    continue
                try:
                    if value is not None and pd.isna(value):
                        rec[key] = None
                except TypeError:
                    pass
            normalized.append(rec)

        return normalized

    def load_moderation_queue(records: List[Dict[str, Any]]):
        if not records:
            return
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
        INSERT INTO raw.moderation_queue (
            review_id,
            user_id,
            product_id,
            review_text,
            rating,
            moderation_status,
            flags,
            submitted_at,
            updated_at
        )
        VALUES (
            %(review_id)s,
            %(user_id)s,
            %(product_id)s,
            %(review_text)s,
            %(rating)s,
            %(moderation_status)s,
            %(flags)s::jsonb,
            %(submitted_at)s,
            NOW()
        )
        ON CONFLICT (review_id) DO UPDATE SET
            user_id = EXCLUDED.user_id,
            product_id = EXCLUDED.product_id,
            review_text = EXCLUDED.review_text,
            rating = EXCLUDED.rating,
            moderation_status = EXCLUDED.moderation_status,
            flags = EXCLUDED.flags,
            submitted_at = EXCLUDED.submitted_at,
            updated_at = NOW();
        """
        for rec in records:
            hook.run(sql, parameters=rec)

    @task
    def replicate_user_sessions():
        col = _get_mongo_collection("UserSessions")
        docs = list(col.find({}))
        for d in docs:
            d.pop("_id", None)
        if not docs:
            return
        transformed = transform_user_sessions(docs)
        load_user_sessions(transformed)

    @task
    def replicate_event_logs():
        col = _get_mongo_collection("EventLogs")
        docs = list(col.find({}))
        for d in docs:
            d.pop("_id", None)
        if not docs:
            return
        transformed = transform_event_logs(docs)
        load_event_logs(transformed)

    @task
    def replicate_support_tickets():
        col = _get_mongo_collection("SupportTickets")
        docs = list(col.find({}))
        for d in docs:
            d.pop("_id", None)
        if not docs:
            return
        transformed = transform_support_tickets(docs)
        load_support_tickets(transformed)

    @task
    def replicate_user_recommendations():
        col = _get_mongo_collection("UserRecommendations")
        docs = list(col.find({}))
        for d in docs:
            d.pop("_id", None)
        if not docs:
            return
        transformed = transform_user_recommendations(docs)
        load_user_recommendations(transformed)

    @task
    def replicate_moderation_queue():
        col = _get_mongo_collection("ModerationQueue")
        docs = list(col.find({}))
        for d in docs:
            d.pop("_id", None)
        if not docs:
            return
        transformed = transform_moderation_queue(docs)
        load_moderation_queue(transformed)

    ensure = ensure_raw_schema()

    us = replicate_user_sessions()
    el = replicate_event_logs()
    st = replicate_support_tickets()
    ur = replicate_user_recommendations()
    mq = replicate_moderation_queue()

    ensure >> [us, el, st, ur, mq]

