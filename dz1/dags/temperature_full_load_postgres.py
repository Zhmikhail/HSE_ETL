from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import json
from pathlib import Path
import kagglehub
import numpy as np
from psycopg2.extras import execute_values

def download_and_prepare_dataset():
    """
    Загрузка датасета с Kaggle и подготовка данных для загрузки в PostgreSQL.
    Осуществляется скачивание исходного датасета, стандартизация колонок
    и преобразование данных в структурированный формат.
    
    Возвращает:
        Сериализованный JSON подготовленного DataFrame
    """
    print("=" * 60)
    print("Загрузка и подготовка датасета...")
    print("=" * 60)
    
    try:
        # Скачиваем датасет с Kaggle
        path = kagglehub.dataset_download("atulanandjha/temperature-readings-iot-devices")
        print(f"Датасет загружен по пути: {path}")
        
        # Ищем CSV файлы в скачанной директории
        csv_files = list(Path(path).rglob("*.csv"))
        
        if not csv_files:
            raise FileNotFoundError("CSV файлы не найдены в датасете")
        
        # Берем первый найденный CSV файл
        csv_path = str(csv_files[0])
        print(f"Найден файл: {csv_path}")
        
        # Читаем датасет
        df = pd.read_csv(csv_path)
        print(f"Исходный размер датасета: {df.shape}")
        print(f"Колонки: {df.columns.tolist()}")
        
        # Стандартизируем названия колонок
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
        df.columns = [col.replace('/', '_') for col in df.columns]
        print(f"Стандартизированные колонки: {df.columns.tolist()}")
        
        # Переименовываем колонки для унификации
        column_mapping = {}
        for col in df.columns:
            if 'date' in col:
                column_mapping[col] = 'noted_date'
            elif 'temp' in col:
                column_mapping[col] = 'temp'
            elif 'out_in' in col:
                column_mapping[col] = 'out_in'
        
        if column_mapping:
            df = df.rename(columns=column_mapping)
            print(f"После переименования: {df.columns.tolist()}")
        
        # Преобразуем дату в datetime формат
        if 'noted_date' in df.columns:
            df['noted_date'] = pd.to_datetime(df['noted_date'], errors='coerce')
            df = df.dropna(subset=['noted_date'])
            print(f"Диапазон дат: {df['noted_date'].min()} - {df['noted_date'].max()}")
        
        # Сериализуем DataFrame для передачи через XCom
        df_json = df.to_json(date_format='iso', orient='split')
        
        print("=" * 60)
        print("Подготовка данных завершена успешно")
        print("=" * 60)
        
        return df_json
        
    except Exception as e:
        print(f"Ошибка при загрузке датасета: {e}")
        raise

def get_connection():
    """
    Установка подключения к PostgreSQL базе данных.
    Использует параметры подключения из переменных окружения Airflow.
    
    Возвращает:
        Подключение к PostgreSQL или None при ошибке
    """
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        print("Подключение к PostgreSQL установлено")
        return conn
    except Exception as e:
        print(f"Ошибка подключения к PostgreSQL: {e}")
        return None

def create_tables():
    """
    Создание необходимых таблиц в базе данных PostgreSQL.
    Создаются таблицы для хранения температурных данных и истории загрузок.
    """
    print("=" * 60)
    print("Создание таблиц в базе данных...")
    print("=" * 60)
    
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Таблица для полной загрузки температурных данных
        create_full_table = """
        CREATE TABLE IF NOT EXISTS temperature_full (
            id SERIAL PRIMARY KEY,
            noted_date TIMESTAMP,
            temperature FLOAT,
            location VARCHAR(10),
            device_id VARCHAR(50),
            load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            batch_id VARCHAR(50)
        );
        """
        
        # Таблица для истории загрузок
        create_history_table = """
        CREATE TABLE IF NOT EXISTS load_history (
            id SERIAL PRIMARY KEY,
            load_type VARCHAR(20),
            batch_id VARCHAR(50),
            start_date TIMESTAMP,
            end_date TIMESTAMP,
            records_loaded INTEGER,
            load_status VARCHAR(20),
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cursor.execute(create_full_table)
        cursor.execute(create_history_table)
        conn.commit()
        
        print("Таблицы успешно созданы/проверены:")
        print("- temperature_full")
        print("- load_history")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Ошибка при создании таблиц: {e}")
        raise

def full_load_to_postgres(ti):
    """
    Выполняет полную загрузку всех данных в таблицу temperature_full.
    Очищает целевую таблицу и загружает все записи из DataFrame заново.
    Записывает метаданные о загрузке в таблицу load_history.
    
    Параметры:
        ti: TaskInstance для получения данных через XCom
    """
    print("=" * 60)
    print("ПОЛНАЯ ЗАГРУЗКА ДАННЫХ В POSTGRESQL")
    print("=" * 60)
    
    # Получаем подготовленные данные из предыдущей задачи
    df_json = ti.xcom_pull(task_ids='download_and_prepare_dataset')
    
    if not df_json:
        raise ValueError("Не удалось получить данные из предыдущей задачи")
    
    # Преобразуем JSON обратно в DataFrame
    df_dict = json.loads(df_json)
    df = pd.DataFrame(df_dict['data'], columns=df_dict['columns'])
    
    # Восстанавливаем типы данных
    if 'noted_date' in df.columns:
        df['noted_date'] = pd.to_datetime(df['noted_date'])
    
    print(f"Получен DataFrame для загрузки: {df.shape[0]} записей")
    
    conn = get_connection()
    if not conn:
        raise ConnectionError("Не удалось установить подключение к PostgreSQL")
    
    cursor = conn.cursor()
    
    batch_id = f"full_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        # Записываем начало загрузки в историю
        cursor.execute("""
        INSERT INTO load_history (load_type, batch_id, start_date, load_status)
        VALUES (%s, %s, %s, %s)
        """, ('full', batch_id, datetime.now(), 'started'))
        conn.commit()
    except Exception as e:
        print(f"Ошибка записи в историю: {e}")
    
    loaded_rows = 0
    
    try:
        # Очищаем таблицу перед загрузкой
        cursor.execute("TRUNCATE TABLE temperature_full")
        print("Таблица temperature_full очищена")
        
        records_to_insert = []
        
        for idx, row in df.iterrows():
            try:
                noted_date = row['noted_date']
                if pd.isna(noted_date):
                    continue
                
                temperature = row['temp']
                if pd.isna(temperature):
                    temperature = None
                
                location = str(row['out_in']).strip()[:10] if pd.notna(row['out_in']) else 'unknown'
                
                device_id = 'unknown'
                if pd.notna(row['id']):
                    id_str = str(row['id'])
                    if '_' in id_str:
                        device_id = f"device_{id_str.split('_')[-1][:10]}"
                    else:
                        device_id = f"device_{id_str[:10]}"
                elif 'room_id_id' in df.columns and pd.notna(row['room_id_id']):
                    device_id = f"room_{str(row['room_id_id'])[:10]}"
                else:
                    device_id = f"default_{idx}"
                
                records_to_insert.append((
                    noted_date,
                    temperature,
                    location,
                    device_id,
                    batch_id
                ))
                
            except Exception as e:
                continue
        
        print(f"Подготовлено {len(records_to_insert)} записей из {len(df)}")
        
        # Загружаем данные пачками
        batch_size = 1000
        total_rows = len(records_to_insert)
        
        for i in range(0, total_rows, batch_size):
            batch = records_to_insert[i:i+batch_size]
            
            if batch:
                insert_query = """
                INSERT INTO temperature_full 
                (noted_date, temperature, location, device_id, batch_id)
                VALUES %s
                """
                execute_values(cursor, insert_query, batch)
                loaded_rows += len(batch)
            
            if i % (batch_size * 10) == 0 or i + batch_size >= total_rows:
                print(f"Загружено {min(i+batch_size, total_rows)} из {total_rows} записей")
        
        conn.commit()
        print(f"Полная загрузка завершена: {loaded_rows} записей")
        
        # Обновляем историю загрузки
        cursor.execute("""
        UPDATE load_history 
        SET end_date = %s, records_loaded = %s, load_status = %s
        WHERE batch_id = %s
        """, (datetime.now(), loaded_rows, 'success', batch_id))
        conn.commit()
        
        # Выводим статистику загрузки
        cursor.execute("SELECT COUNT(*) FROM temperature_full")
        total_in_db = cursor.fetchone()[0]
        print(f"В таблице temperature_full теперь: {total_in_db} записей")
        
        cursor.execute("""
        SELECT 
            MIN(noted_date), 
            MAX(noted_date),
            COUNT(DISTINCT device_id),
            AVG(temperature)
        FROM temperature_full
        """)
        min_date, max_date, device_count, avg_temp = cursor.fetchone()
        print(f"Период данных: {min_date} - {max_date}")
        print(f"Уникальных устройств: {device_count}")
        if avg_temp is not None:
            print(f"Средняя температура: {avg_temp:.2f}°C")
        
    except Exception as e:
        print(f"Ошибка при полной загрузке: {e}")
        
        try:
            cursor.execute("""
            UPDATE load_history 
            SET end_date = %s, load_status = %s, error_message = %s
            WHERE batch_id = %s
            """, (datetime.now(), 'failed', str(e)[:500], batch_id))
            conn.commit()
        except:
            pass
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
    
    return loaded_rows

def print_final_summary(ti):
    """
    Вывод итоговой статистики по выполненной загрузке.
    Отображает общую информацию о загруженных данных и статус выполнения.
    
    Параметры:
        ti: TaskInstance для получения данных через XCom
    """
    print("=" * 60)
    print("ИТОГОВАЯ СТАТИСТИКА ЗАГРУЗКИ")
    print("=" * 60)
    
    try:
        # Получаем количество загруженных записей из предыдущей задачи
        loaded_rows = ti.xcom_pull(task_ids='full_load_to_postgres')
        
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Общая статистика из базы данных
        cursor.execute("SELECT COUNT(*) FROM temperature_full")
        total_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT DATE(noted_date)) FROM temperature_full")
        total_days = cursor.fetchone()[0]
        
        cursor.execute("SELECT MIN(noted_date), MAX(noted_date) FROM temperature_full")
        date_range = cursor.fetchone()
        
        cursor.execute("SELECT AVG(temperature), MIN(temperature), MAX(temperature) FROM temperature_full")
        temp_stats = cursor.fetchone()
        
        print(f"Всего загружено записей: {total_records}")
        print(f"Количество дней данных: {total_days}")
        print(f"Период данных: {date_range[0]} - {date_range[1]}")
        print(f"Средняя температура: {temp_stats[0]:.2f}°C")
        print(f"Минимальная температура: {temp_stats[1]:.2f}°C")
        print(f"Максимальная температура: {temp_stats[2]:.2f}°C")
        
        # История последней загрузки
        print(f"\nИстория последней загрузки:")
        cursor.execute("""
        SELECT batch_id, load_type, records_loaded, load_status, created_at
        FROM load_history
        ORDER BY created_at DESC
        LIMIT 1
        """)
        
        last_load = cursor.fetchone()
        if last_load:
            batch_id, load_type, records, status, created_at = last_load
            print(f"  Batch ID: {batch_id}")
            print(f"  Тип загрузки: {load_type}")
            print(f"  Записей загружено: {records}")
            print(f"  Статус: {status}")
            print(f"  Время загрузки: {created_at}")
        
        print("\n" + "=" * 60)
        print("ПОЛНАЯ ЗАГРУЗКА ДАННЫХ ЗАВЕРШЕНА УСПЕШНО!")
        print("=" * 60)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Ошибка при получении статистики: {e}")
        raise

# Настройки DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="temperature_full_load_postgres_v3",
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="Полная загрузка температурных данных IoT устройств в PostgreSQL",
    tags=["postgres", "full_load", "temperature", "data_migration"]
) as dag:

    # Стартовая задача
    start_task = EmptyOperator(
        task_id="start",
        doc="Начало выполнения DAG"
    )
    
    # Задача загрузки и подготовки данных
    download_task = PythonOperator(
        task_id="download_and_prepare_dataset",
        python_callable=download_and_prepare_dataset,
        doc="Загрузка датасета с Kaggle и подготовка данных"
    )
    
    # Задача создания таблиц в БД
    create_tables_task = PythonOperator(
        task_id="create_database_tables",
        python_callable=create_tables,
        doc="Создание необходимых таблиц в PostgreSQL"
    )
    
    # Задача полной загрузки данных в PostgreSQL
    full_load_task = PythonOperator(
        task_id="full_load_to_postgres",
        python_callable=full_load_to_postgres,
        doc="Полная загрузка подготовленных данных в PostgreSQL"
    )
    
    # Задача вывода итоговой статистики
    summary_task = PythonOperator(
        task_id="print_final_summary",
        python_callable=print_final_summary,
        doc="Вывод итоговой статистики по выполненной загрузке"
    )
    
    # Финальная задача
    end_task = EmptyOperator(
        task_id="end",
        doc="Завершение выполнения DAG"
    )
    
    # Определение порядка выполнения задач
    start_task >> download_task >> create_tables_task
    create_tables_task >> full_load_task >> summary_task >> end_task