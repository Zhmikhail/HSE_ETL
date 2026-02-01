from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import json

DB_CONFIG = {
    "host": "postgres",
    "dbname": "pets_db",
    "user": "airflow",
    "password": "airflow"
}

def load_json_pets_to_flat():
    """Переносим данные из JSON в плоскую таблицу flat_pets"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT 
            item.value->>'name',
            item.value->>'species',
            item.value->>'birthYear',
            item.value->>'photo',
            item.value->'favFoods'
        FROM raw_json,
             jsonb_array_elements(json_data->'pets') AS item(value)
    """)

    for row in cursor.fetchall():
        name, species, birth_year, photo, fav_foods = row
        cursor.execute("""
            INSERT INTO flat_pets (name, species, birth_year, photo, fav_foods)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            name,
            species,
            int(birth_year) if birth_year else None,
            photo,
            json.dumps(fav_foods) if fav_foods else None
        ))

    conn.commit()
    cursor.close()
    conn.close()


def load_xml_foods_to_flat():
    """Переносим данные из XML в таблицу flat_foods"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO flat_foods (
            food_name, manufacturer, serving_size, calories_total, calories_fat,
            total_fat, saturated_fat, cholesterol, sodium, carb, fiber, protein,
            vitamin_a, vitamin_c, calcium, iron
        )
        SELECT
            (xpath('//name/text()', node))[1]::text AS food_name,
            (xpath('//mfr/text()', node))[1]::text AS manufacturer,
            (xpath('//serving/text()', node))[1]::text AS serving_size,
            NULLIF((xpath('//calories/@total', node))[1]::text, '')::int AS calories_total,
            NULLIF((xpath('//calories/@fat', node))[1]::text, '')::int AS calories_fat,
            (xpath('//total-fat/text()', node))[1]::text AS total_fat,
            (xpath('//saturated-fat/text()', node))[1]::text AS saturated_fat,
            (xpath('//cholesterol/text()', node))[1]::text AS cholesterol,
            (xpath('//sodium/text()', node))[1]::text AS sodium,
            (xpath('//carb/text()', node))[1]::text AS carb,
            (xpath('//fiber/text()', node))[1]::text AS fiber,
            (xpath('//protein/text()', node))[1]::text AS protein,
            COALESCE((xpath('//vitamins/a/text()', node))[1]::text, '0') AS vitamin_a,
            COALESCE((xpath('//vitamins/c/text()', node))[1]::text, '0') AS vitamin_c,
            COALESCE((xpath('//minerals/ca/text()', node))[1]::text, '0') AS calcium,
            COALESCE((xpath('//minerals/fe/text()', node))[1]::text, '0') AS iron
        FROM raw_xml,
             unnest(xpath('//food', xml_data::xml)) AS node;
    """)

    conn.commit()
    cursor.close()
    conn.close()


default_args = {
    "start_date": datetime(2026, 1, 23)
}

with DAG(
    dag_id="parasha_v14",
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["etl", "pets", "foods"]
) as dag:

    task_json_flat = PythonOperator(
        task_id="load_json_pets",
        python_callable=load_json_pets_to_flat
    )

    task_xml_flat = PythonOperator(
        task_id="load_xml_foods",
        python_callable=load_xml_foods_to_flat
    )

    task_json_flat >> task_xml_flat
