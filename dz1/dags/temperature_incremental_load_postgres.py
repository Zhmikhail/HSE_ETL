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

def download_and_prepare_dataset(ti):
    """
    Загрузка датасета с Kaggle и подготовка данных для инкрементальной загрузки.
    Осуществляется скачивание исходного датасета, стандартизация колонок
    и преобразование данных в структурированный формат.
    
    Параметры:
        ti: TaskInstance для передачи данных через XCom
    
    Возвращает:
        Сериализованный JSON подготовленного DataFrame
    """
    print("=" * 60)
    print("ЗАГРУЗКА И ПОДГОТОВКА ДАТАСЕТА ДЛЯ ИНКРЕМЕНТАЛЬНОЙ ЗАГРУЗКИ")
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
        
        # Для инкрементальной загрузки берем данные за последние N дней
        # (в реальном сценарии здесь была бы проверка последней загрузки)
        max_date = df['noted_date'].max()
        last_n_days = 7  # Параметр можно передавать через DAG params
        cutoff_date = max_date - timedelta(days=last_n_days)
        
        df_incremental = df[df['noted_date'] >= cutoff_date].copy()
        print(f"Данных для инкрементальной загрузки (последние {last_n_days} дней): {df_incremental.shape[0]} записей")
        
        # Сериализуем DataFrame для передачи через XCom
        df_json = df_incremental.to_json(date_format='iso', orient='split')
        
        # Сохраняем в XCom
        ti.xcom_push(key='incremental_data', value=df_json)
        
        print("=" * 60)
        print("ПОДГОТОВКА ДАННЫХ ДЛЯ ИНКРЕМЕНТАЛЬНОЙ ЗАГРУЗКИ ЗАВЕРШЕНА")
        print("=" * 60)
        
        return df_json
        
    except Exception as e:
        print(f"Ошибка при загрузке датасета: {e}")
        raise

def create_incremental_tables():
    """
    Создание таблиц для инкрементальной загрузки в базе данных PostgreSQL.
    Создаются таблицы для хранения инкрементальных данных и истории загрузок.
    """
    print("=" * 60)
    print("СОЗДАНИЕ ТАБЛИЦ ДЛЯ ИНКРЕМЕНТАЛЬНОЙ ЗАГРУЗКИ")
    print("=" * 60)
    
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Таблица для инкрементальной загрузки температурных данных
        create_incremental_table = """
        CREATE TABLE IF NOT EXISTS temperature_incremental (
            id SERIAL PRIMARY KEY,
            noted_date TIMESTAMP,
            temperature FLOAT,
            location VARCHAR(10),
            device_id VARCHAR(50),
            load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            batch_id VARCHAR(50),
            UNIQUE(noted_date, device_id)
        );
        """
        
        # Таблица для истории загрузок (если еще не создана)
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
        
        cursor.execute(create_incremental_table)
        cursor.execute(create_history_table)
        conn.commit()
        
        print("Таблицы успешно созданы/проверены:")
        print("- temperature_incremental")
        print("- load_history")
        
        # Проверяем текущее состояние таблицы
        cursor.execute("SELECT COUNT(*) FROM temperature_incremental")
        count = cursor.fetchone()[0]
        print(f"Текущее количество записей в temperature_incremental: {count}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Ошибка при создании таблиц: {e}")
        raise

def incremental_load_to_postgres(ti, **kwargs):
    """
    Выполняет инкрементальную загрузку данных в таблицу temperature_incremental.
    Загружает данные за указанный период, используя UPSERT операцию.
    Обновляет существующие записи и добавляет новые.
    
    Параметры:
        ti: TaskInstance для получения данных через XCom
        **kwargs: Контекст DAG, включая параметры
    """
    print("=" * 60)
    print("ВЫПОЛНЕНИЕ ИНКРЕМЕНТАЛЬНОЙ ЗАГРУЗКИ В POSTGRESQL")
    print("=" * 60)
    
    # Получаем подготовленные данные из XCom
    df_json = ti.xcom_pull(task_ids='download_and_prepare_dataset', key='incremental_data')
    
    if not df_json:
        raise ValueError("Не удалось получить данные из предыдущей задачи")
    
    # Преобразуем JSON обратно в DataFrame
    df_dict = json.loads(df_json)
    df = pd.DataFrame(df_dict['data'], columns=df_dict['columns'])
    
    # Восстанавливаем типы данных
    if 'noted_date' in df.columns:
        df['noted_date'] = pd.to_datetime(df['noted_date'])
    
    print(f"Получен DataFrame для инкрементальной загрузки: {df.shape[0]} записей")
    
    # Получаем параметры из контекста DAG
    dag_run_conf = kwargs.get('dag_run').conf if kwargs.get('dag_run') else {}
    last_n_days = dag_run_conf.get('last_n_days', kwargs['params'].get('last_n_days', 7))
    batch_size = dag_run_conf.get('batch_size', kwargs['params'].get('batch_size', 500))
    
    print(f"Параметры загрузки: last_n_days={last_n_days}, batch_size={batch_size}")
    
    conn = get_connection()
    if not conn:
        raise ConnectionError("Не удалось установить подключение к PostgreSQL")
    
    cursor = conn.cursor()
    
    batch_id = f"inc_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        # Записываем начало загрузки в историю
        cursor.execute("""
        INSERT INTO load_history (load_type, batch_id, start_date, load_status)
        VALUES (%s, %s, %s, %s)
        """, ('incremental', batch_id, datetime.now(), 'started'))
        conn.commit()
    except Exception as e:
        print(f"Ошибка записи в историю: {e}")
    
    total_loaded = 0
    errors = 0
    
    try:
        if 'noted_date' in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df['noted_date']):
                df = df.copy()
                df['noted_date'] = pd.to_datetime(df['noted_date'], errors='coerce')
            
            max_date = df['noted_date'].max()
            
            if pd.isna(max_date):
                print("Не удалось определить максимальную дату")
                cursor.execute("""
                UPDATE load_history 
                SET end_date = %s, records_loaded = %s, load_status = %s
                WHERE batch_id = %s
                """, (datetime.now(), 0, 'failed_no_date', batch_id))
                conn.commit()
                return 0
            
            cutoff_date = max_date - timedelta(days=last_n_days)
            
            print(f"Максимальная дата в данных: {max_date}")
            print(f"Берем данные с: {cutoff_date}")
            print(f"Период загрузки: {last_n_days} дней ({cutoff_date.date()} - {max_date.date()})")
            
            mask = (df['noted_date'] >= cutoff_date) & (df['noted_date'] <= max_date)
            df_filtered = df[mask].copy()
            
            print(f"Данных за последние {last_n_days} дней: {len(df_filtered)} записей")
            
            if len(df_filtered) == 0:
                print("Нет данных для указанного периода")
                cursor.execute("""
                UPDATE load_history 
                SET end_date = %s, records_loaded = %s, load_status = %s
                WHERE batch_id = %s
                """, (datetime.now(), 0, 'no_data', batch_id))
                conn.commit()
                return 0
        else:
            print("Нет колонки с датой в данных")
            cursor.execute("""
            UPDATE load_history 
            SET end_date = %s, records_loaded = %s, load_status = %s
            WHERE batch_id = %s
            """, (datetime.now(), 0, 'failed_no_date_col', batch_id))
            conn.commit()
            return 0
        
        records_to_insert = []
        
        for idx, row in df_filtered.iterrows():
            try:
                noted_date = row['noted_date']
                if pd.isna(noted_date):
                    continue
                
                temperature = None
                if 'temp' in df.columns and pd.notna(row['temp']):
                    try:
                        temperature = float(row['temp'])
                    except:
                        temperature = None
                
                location = 'unknown'
                if 'out_in' in df.columns and pd.notna(row['out_in']):
                    location = str(row['out_in']).strip()[:10]
                
                device_id = 'unknown'
                if 'id' in df.columns and pd.notna(row['id']):
                    id_val = row['id']
                    if isinstance(id_val, str):
                        parts = id_val.split('_')
                        if len(parts) > 1:
                            device_id = f"dev_{parts[-1][:15]}"
                        else:
                            device_id = f"dev_{id_val[:15]}"
                    else:
                        device_id = f"dev_{str(id_val)[:15]}"
                else:
                    device_id = f"row_{idx}"
                
                records_to_insert.append((
                    noted_date,
                    temperature,
                    location,
                    device_id,
                    batch_id
                ))
                
            except Exception as e:
                errors += 1
                continue
        
        print(f"Подготовлено {len(records_to_insert)} записей")
        if errors > 0:
            print(f"Пропущено {errors} записей с ошибками")
        
        if not records_to_insert:
            print("Нет валидных записей для загрузки")
            cursor.execute("""
            UPDATE load_history 
            SET end_date = %s, records_loaded = %s, load_status = %s
            WHERE batch_id = %s
            """, (datetime.now(), 0, 'no_valid_data', batch_id))
            conn.commit()
            return 0
        
        dates = sorted(set(r[0].date() for r in records_to_insert))
        print(f"Будет загружено {len(dates)} дней: {dates[0]} - {dates[-1]}")
        
        unique_records = {}
        for record in records_to_insert:
            key = (record[0], record[3])
            unique_records[key] = record
        
        print(f"После удаления дубликатов: {len(unique_records)} уникальных записей")
        
        unique_records_list = list(unique_records.values())
        
        loaded_in_batch = 0
        batch_errors = 0
        
        for i in range(0, len(unique_records_list), batch_size):
            batch = unique_records_list[i:i+batch_size]
            
            try:
                for record in batch:
                    try:
                        check_query = """
                        SELECT COUNT(*) 
                        FROM temperature_incremental 
                        WHERE noted_date = %s AND device_id = %s
                        """
                        cursor.execute(check_query, (record[0], record[3]))
                        exists = cursor.fetchone()[0] > 0
                        
                        if exists:
                            update_query = """
                            UPDATE temperature_incremental 
                            SET temperature = %s, 
                                location = %s, 
                                load_timestamp = CURRENT_TIMESTAMP,
                                batch_id = %s
                            WHERE noted_date = %s AND device_id = %s
                            """
                            cursor.execute(update_query, 
                                         (record[1], record[2], record[4], record[0], record[3]))
                        else:
                            insert_query = """
                            INSERT INTO temperature_incremental 
                            (noted_date, temperature, location, device_id, batch_id)
                            VALUES (%s, %s, %s, %s, %s)
                            """
                            cursor.execute(insert_query, record)
                        
                        total_loaded += 1
                        
                    except Exception as record_error:
                        batch_errors += 1
                        try:
                            simple_insert = """
                            INSERT INTO temperature_incremental 
                            (noted_date, temperature, location, device_id, batch_id)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (noted_date, device_id) DO NOTHING
                            """
                            cursor.execute(simple_insert, record)
                            total_loaded += 1
                            batch_errors -= 1
                        except:
                            continue
                
                conn.commit()
                loaded_in_batch += len(batch)
                
                if (i // batch_size) % 5 == 0 or i + batch_size >= len(unique_records_list):
                    print(f"Загружено {min(i+batch_size, len(unique_records_list))} из {len(unique_records_list)} записей")
                    
            except Exception as batch_error:
                print(f"Ошибка пачки: {batch_error}")
                conn.rollback()
                for record in batch:
                    try:
                        simple_insert = """
                        INSERT INTO temperature_incremental 
                        (noted_date, temperature, location, device_id, batch_id)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (noted_date, device_id) DO NOTHING
                        """
                        cursor.execute(simple_insert, record)
                        total_loaded += 1
                    except:
                        batch_errors += 1
                        continue
                conn.commit()
        
        print(f"Инкрементальная загрузка завершена")
        print(f"Успешно загружено/обновлено: {total_loaded} записей")
        if batch_errors > 0:
            print(f"Ошибок при обработке: {batch_errors} записей")
        
        status = 'success' if batch_errors == 0 else 'partial'
        cursor.execute("""
        UPDATE load_history 
        SET end_date = %s, records_loaded = %s, load_status = %s
        WHERE batch_id = %s
        """, (datetime.now(), total_loaded, status, batch_id))
        conn.commit()
        
        cursor.execute("SELECT COUNT(*) FROM temperature_incremental")
        total_in_db = cursor.fetchone()[0]
        print(f"Всего записей в таблице temperature_incremental: {total_in_db}")
        
        cursor.execute("""
        SELECT 
            DATE(noted_date) as load_date, 
            COUNT(*) as records
        FROM temperature_incremental 
        WHERE batch_id = %s
        GROUP BY DATE(noted_date)
        ORDER BY load_date DESC
        LIMIT 5
        """, (batch_id,))
        
        print("Загруженные дни (последние 5):")
        results = cursor.fetchall()
        for date, count in results:
            print(f"  {date}: {count} записей")
        
        if results:
            cursor.execute("""
            SELECT 
                AVG(temperature) as avg_temp,
                MIN(temperature) as min_temp,
                MAX(temperature) as max_temp
            FROM temperature_incremental 
            WHERE batch_id = %s
            """, (batch_id,))
            
            avg_temp, min_temp, max_temp = cursor.fetchone()
            if avg_temp is not None:
                print(f"Статистика температуры загруженных данных:")
                print(f"  Средняя: {avg_temp:.1f}°C")
                print(f"  Минимальная: {min_temp:.1f}°C")
                print(f"  Максимальная: {max_temp:.1f}°C")
        
    except Exception as e:
        print(f"Критическая ошибка при инкрементальной загрузке: {e}")
        
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
    
    return total_loaded

def print_incremental_summary(ti):
    """
    Вывод итоговой статистики по выполненной инкрементальной загрузке.
    Отображает общую информацию о загруженных данных и статус выполнения.
    
    Параметры:
        ti: TaskInstance для получения данных через XCom
    """
    print("=" * 60)
    print("ИТОГОВАЯ СТАТИСТИКА ИНКРЕМЕНТАЛЬНОЙ ЗАГРУЗКИ")
    print("=" * 60)
    
    try:
        # Получаем количество загруженных записей из предыдущей задачи
        loaded_rows = ti.xcom_pull(task_ids='incremental_load_to_postgres')
        
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Общая статистика из базы данных
        cursor.execute("SELECT COUNT(*) FROM temperature_incremental")
        total_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT DATE(noted_date)) FROM temperature_incremental")
        total_days = cursor.fetchone()[0]
        
        cursor.execute("SELECT MIN(noted_date), MAX(noted_date) FROM temperature_incremental")
        date_range = cursor.fetchone()
        
        cursor.execute("SELECT AVG(temperature), MIN(temperature), MAX(temperature) FROM temperature_incremental")
        temp_stats = cursor.fetchone()
        
        print(f"Всего записей в таблице: {total_records}")
        print(f"Количество дней данных: {total_days}")
        print(f"Период данных: {date_range[0]} - {date_range[1]}")
        print(f"Средняя температура: {temp_stats[0]:.2f}°C")
        print(f"Минимальная температура: {temp_stats[1]:.2f}°C")
        print(f"Максимальная температура: {temp_stats[2]:.2f}°C")
        
        # История последней загрузки
        print(f"\nИнформация о последней загрузке:")
        cursor.execute("""
        SELECT batch_id, load_type, records_loaded, load_status, created_at
        FROM load_history
        WHERE load_type = 'incremental'
        ORDER BY created_at DESC
        LIMIT 1
        """)
        
        last_load = cursor.fetchone()
        if last_load:
            batch_id, load_type, records, status, created_at = last_load
            print(f"  Batch ID: {batch_id}")
            print(f"  Тип загрузки: {load_type}")
            print(f"  Загружено записей: {records}")
            print(f"  Статус: {status}")
            print(f"  Время завершения: {created_at}")
        
        print("\n" + "=" * 60)
        print("ИНКРЕМЕНТАЛЬНАЯ ЗАГРУЗКА ДАННЫХ ЗАВЕРШЕНА УСПЕШНО!")
        print("=" * 60)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Ошибка при получении статистики: {e}")
        raise

# Настройки DAG для инкрементальной загрузки
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="temperature_incremental_load_postgres_v5",
    schedule="0 2 * * *",  # Ежедневно в 2:00
    catchup=False,
    default_args=default_args,
    params={
        "last_n_days": 7,
        "batch_size": 500
    },
    description="Инкрементальная загрузка температурных данных IoT устройств в PostgreSQL",
    tags=["postgres", "incremental_load", "temperature", "daily_update"]
) as dag:

    # Стартовая задача
    start_task = EmptyOperator(
        task_id="start",
        doc="Начало выполнения DAG для инкрементальной загрузки"
    )
    
    # Задача создания таблиц в БД
    create_tables_task = PythonOperator(
        task_id="create_incremental_tables",
        python_callable=create_incremental_tables,
        doc="Создание таблиц для инкрементальной загрузки в PostgreSQL"
    )
    
    # Задача загрузки и подготовки данных
    download_task = PythonOperator(
        task_id="download_and_prepare_dataset",
        python_callable=download_and_prepare_dataset,
        doc="Загрузка датасета с Kaggle и подготовка данных для инкрементальной загрузки"
    )
    
    # Задача инкрементальной загрузки данных в PostgreSQL
    incremental_load_task = PythonOperator(
        task_id="incremental_load_to_postgres",
        python_callable=incremental_load_to_postgres,
        doc="Инкрементальная загрузка данных в PostgreSQL с использованием UPSERT"
    )
    
    # Задача вывода итоговой статистики
    summary_task = PythonOperator(
        task_id="print_incremental_summary",
        python_callable=print_incremental_summary,
        doc="Вывод итоговой статистики по выполненной инкрементальной загрузке"
    )
    
    # Финальная задача
    end_task = EmptyOperator(
        task_id="end",
        doc="Завершение выполнения DAG инкрементальной загрузки"
    )
    
    # Определение порядка выполнения задач
    start_task >> create_tables_task >> download_task
    download_task >> incremental_load_task >> summary_task >> end_task