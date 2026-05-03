"""
Пример DAG: ETL на Spark с динамическими worker-контейнерами.

Worker-контейнеры поднимаются перед задачей и убиваются после неё.
Ресурсы (кол-во экзекьюторов, память, ядра) задаются прямо в коде.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Параметры Spark-кластера — меняй под свои нужды
SPARK_CONF = {
    "app_name": "temperature_spark_etl",
    "num_executors": 2,
    "executor_memory": "1g",
    "executor_cores": 2,
}

DATA_PATH = "/opt/spark_data/temperature_dataset.csv"


def download_dataset():
    """Скачать датасет с Kaggle и положить в shared volume."""
    import os
    if os.path.exists(DATA_PATH):
        print(f"Датасет уже есть: {DATA_PATH}")
        return

    import kagglehub
    from pathlib import Path
    import shutil

    path = kagglehub.dataset_download("atulanandjha/temperature-readings-iot-devices")
    csv_files = list(Path(path).rglob("*.csv"))
    if not csv_files:
        raise FileNotFoundError("CSV файлы не найдены в датасете")

    os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True)
    shutil.copy(str(csv_files[0]), DATA_PATH)
    print(f"Датасет скопирован в {DATA_PATH}")


def run_spark_etl():
    """Основная Spark-задача: чтение CSV, трансформация, запись в Postgres."""
    from spark_manager import SparkManager

    with SparkManager(**SPARK_CONF) as spark:
        # ------ Extract ------
        df = spark.read.csv(
            "/opt/spark_data/temperature_dataset.csv",
            header=True,
            inferSchema=True,
        )
        print(f"Загружено строк: {df.count()}")
        df.printSchema()

        # ------ Transform ------
        from pyspark.sql import functions as F

        # Фильтр: только indoor
        df_indoor = df.filter(F.lower(F.col("`out/in`")) == "in")

        # Парсим дату
        df_dated = df_indoor.withColumn(
            "date", F.to_date(F.col("noted_date"), "dd-MM-yyyy HH:mm")
        )

        # Средняя температура по дням
        daily_avg = (
            df_dated.groupBy("date")
            .agg(
                F.avg("temp").alias("avg_temp"),
                F.min("temp").alias("min_temp"),
                F.max("temp").alias("max_temp"),
                F.count("*").alias("record_count"),
            )
            .orderBy("date")
        )

        daily_avg.show(10)

        # Топ-5 самых жарких и холодных дней
        hottest = daily_avg.orderBy(F.desc("avg_temp")).limit(5)
        coldest = daily_avg.orderBy(F.asc("avg_temp")).limit(5)

        print("=== Самые жаркие дни ===")
        hottest.show()
        print("=== Самые холодные дни ===")
        coldest.show()

        # ------ Load ------
        # Запись результатов в Postgres
        jdbc_url = "jdbc:postgresql://postgres:5432/airflow_db"
        jdbc_props = {
            "user": "airflow",
            "password": "airflow",
            "driver": "org.postgresql.Driver",
        }

        daily_avg.write.jdbc(
            url=jdbc_url,
            table="spark_daily_temperature",
            mode="overwrite",
            properties=jdbc_props,
        )
        print("Данные записаны в spark_daily_temperature")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="spark_etl_example",
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="ETL на Spark с динамическими worker-контейнерами",
    tags=["spark", "etl", "temperature"],
) as dag:

    start = EmptyOperator(task_id="start")

    download_task = PythonOperator(
        task_id="download_dataset",
        python_callable=download_dataset,
    )

    spark_task = PythonOperator(
        task_id="spark_temperature_etl",
        python_callable=run_spark_etl,
    )

    end = EmptyOperator(task_id="end")

    start >> download_task >> spark_task >> end
