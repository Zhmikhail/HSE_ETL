from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    "owner": "etl_final_project",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="analytics_marts",
    start_date=datetime(2026, 3, 3),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["analytics", "marts"],
) as dag:
    create_user_activity_mart = PostgresOperator(
        task_id="create_user_activity_mart",
        postgres_conn_id="postgres_analytics",
        sql="""
        CREATE SCHEMA IF NOT EXISTS marts;

        CREATE TABLE IF NOT EXISTS marts.user_activity_mart (
            activity_date DATE NOT NULL,
            user_id TEXT NOT NULL,
            sessions_count INTEGER NOT NULL,
            avg_session_duration_seconds NUMERIC(12, 2),
            total_actions INTEGER,
            unique_pages_visited INTEGER,
            PRIMARY KEY (activity_date, user_id)
        );
        """,
    )

    populate_user_activity_mart = PostgresOperator(
        task_id="populate_user_activity_mart",
        postgres_conn_id="postgres_analytics",
        sql="""
        DELETE FROM marts.user_activity_mart
        WHERE activity_date = '{{ ds }}'::date;

        INSERT INTO marts.user_activity_mart (
            activity_date,
            user_id,
            sessions_count,
            avg_session_duration_seconds,
            total_actions,
            unique_pages_visited
        )
        SELECT
            rs.start_time::date AS activity_date,
            rs.user_id,
            COUNT(*) AS sessions_count,
            AVG(rs.session_duration_seconds)::NUMERIC(12, 2) AS avg_session_duration_seconds,
            SUM(
                COALESCE(
                    jsonb_array_length(rs.actions),
                    0
                )
            ) AS total_actions,
            COUNT(DISTINCT p.page) AS unique_pages_visited
        FROM raw.user_sessions rs
        LEFT JOIN LATERAL jsonb_array_elements_text(rs.pages_visited) AS p(page) ON TRUE
        WHERE rs.start_time::date = '{{ ds }}'::date
        GROUP BY
            rs.start_time::date,
            rs.user_id;
        """,
    )

    create_support_efficiency_mart = PostgresOperator(
        task_id="create_support_efficiency_mart",
        postgres_conn_id="postgres_analytics",
        sql="""
        CREATE SCHEMA IF NOT EXISTS marts;

        CREATE TABLE IF NOT EXISTS marts.support_efficiency_mart (
            snapshot_date DATE NOT NULL,
            issue_type TEXT NOT NULL,
            status TEXT NOT NULL,
            tickets_count INTEGER NOT NULL,
            avg_resolution_time_hours NUMERIC(12, 2),
            open_tickets_count INTEGER,
            PRIMARY KEY (snapshot_date, issue_type, status)
        );
        """,
    )

    populate_support_efficiency_mart = PostgresOperator(
        task_id="populate_support_efficiency_mart",
        postgres_conn_id="postgres_analytics",
        sql="""
        DELETE FROM marts.support_efficiency_mart
        WHERE snapshot_date = '{{ ds }}'::date;

        INSERT INTO marts.support_efficiency_mart (
            snapshot_date,
            issue_type,
            status,
            tickets_count,
            avg_resolution_time_hours,
            open_tickets_count
        )
        SELECT
            '{{ ds }}'::date AS snapshot_date,
            st.issue_type,
            st.status,
            COUNT(*) AS tickets_count,
            AVG(
                CASE
                    WHEN st.resolution_time_seconds IS NOT NULL
                    THEN st.resolution_time_seconds / 3600.0
                    ELSE NULL
                END
            )::NUMERIC(12, 2) AS avg_resolution_time_hours,
            SUM(
                CASE
                    WHEN st.status IN ('open', 'in_progress')
                    THEN 1
                    ELSE 0
                END
            ) AS open_tickets_count
        FROM raw.support_tickets st
        WHERE st.created_at::date <= '{{ ds }}'::date
        GROUP BY
            st.issue_type,
            st.status;
        """,
    )

    (
        create_user_activity_mart
        >> populate_user_activity_mart
        >> create_support_efficiency_mart
        >> populate_support_efficiency_mart
    )

