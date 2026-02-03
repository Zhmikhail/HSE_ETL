from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
import os
from pathlib import Path
import kagglehub
import numpy as np

def download_dataset():
    """Загрузка датасета с Kaggle"""
    print("=" * 60)
    print("Начинаем загрузку датасета с Kaggle...")
    print("=" * 60)
    
    try:
        # Скачиваем датасет
        path = kagglehub.dataset_download("atulanandjha/temperature-readings-iot-devices")
        print(f"Датасет загружен по пути: {path}")
        
        # Ищем CSV файлы в скачанной директории
        csv_files = list(Path(path).rglob("*.csv"))
        
        if csv_files:
            # Берем первый найденный CSV файл
            csv_path = str(csv_files[0])
            print(f"Найден файл: {csv_path}")
            
            # Читаем датасет
            df = pd.read_csv(csv_path)
            print(f"Размер датасета: {df.shape}")
            print(f"Колонки: {df.columns.tolist()}")
            
            # Сохраняем во временный файл для использования в следующих задачах
            temp_path = "/tmp/temperature_dataset.csv"
            df.to_csv(temp_path, index=False)
            print(f"Данные сохранены в {temp_path}")
            
            return temp_path
        else:
            print("CSV файлы не найдены, ищем другие файлы...")
            all_files = list(Path(path).rglob("*"))
            print(f"Файлы в директории: {[str(f) for f in all_files[:10]]}")
            raise FileNotFoundError("CSV файлы не найдены в датасете")
            
    except Exception as e:
        print(f"Ошибка при загрузке датасета: {e}")
        raise

def process_temperature_data():
    """Обработка температурных данных"""
    print("=" * 60)
    print("Начинаем обработку температурных данных...")
    print("=" * 60)
    
    # Читаем данные из временного файла
    temp_path = "/tmp/temperature_dataset.csv"
    if not os.path.exists(temp_path):
        raise FileNotFoundError(f"Файл {temp_path} не найден")
    
    df = pd.read_csv(temp_path)
    print(f"Исходный размер датасета: {df.shape}")
    print(f"Колонки: {df.columns.tolist()}")
    
    # Приводим названия колонок к нижнему регистру для удобства
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    print(f"Стандартизированные колонки: {df.columns.tolist()}")
    
    # Проверяем наличие необходимых колонок
    required_columns = ['noted_date', 'temp', 'out/in']
    # Ищем альтернативные названия колонок
    available_columns = df.columns.tolist()
    
    column_mapping = {}
    for req_col in required_columns:
        req_col_clean = req_col.lower().replace('/', '_')
        for avail_col in available_columns:
            if req_col_clean in avail_col:
                column_mapping[req_col] = avail_col
                break
    
    print(f"Найденные соответствия колонок: {column_mapping}")
    
    # Переименовываем колонки если нашли соответствия
    if column_mapping:
        df = df.rename(columns={v: k for k, v in column_mapping.items()})
    
    # Проверяем наличие необходимых колонок после переименования
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"Отсутствуют необходимые колонки: {missing_columns}")
        print(f"Доступные колонки: {df.columns.tolist()}")
        # Пробуем продолжить с доступными колонками
        if 'noted_date' not in df.columns and 'date' in df.columns:
            df = df.rename(columns={'date': 'noted_date'})
        if 'temp' not in df.columns and 'temperature' in df.columns:
            df = df.rename(columns={'temperature': 'temp'})
    
    print(f"Финальные колонки: {df.columns.tolist()}")
    
    # 1. Фильтруем out/in = 'in'
    print("\nФильтрация out/in = 'in'...")
    if 'out/in' in df.columns:
        initial_count = len(df)
        print(f"Уникальные значения в out/in: {df['out/in'].astype(str).unique()[:10]}")
        
        # Приводим к строке и фильтруем
        df['out/in'] = df['out/in'].astype(str).str.strip().str.lower()
        df_filtered = df[df['out/in'] == 'in'].copy()
        
        filtered_count = len(df_filtered)
        print(f"До фильтрации: {initial_count}")
        print(f"После фильтрации out/in='in': {filtered_count}")
        print(f"Отфильтровано записей: {initial_count - filtered_count}")
    else:
        print("Колонка 'out/in' не найдена, пропускаем фильтрацию")
        df_filtered = df.copy()
    
    # 2. Преобразуем noted_date в формат 'yyyy-MM-dd' с типом данных date
    print("\nПреобразование даты...")
    if 'noted_date' in df_filtered.columns:
        # Пробуем разные форматы даты
        try:
            df_filtered['noted_date'] = pd.to_datetime(df_filtered['noted_date'], errors='coerce')
            df_filtered = df_filtered.dropna(subset=['noted_date'])
            df_filtered['noted_date'] = df_filtered['noted_date'].dt.strftime('%Y-%m-%d')
            df_filtered['noted_date'] = pd.to_datetime(df_filtered['noted_date'])
            print(f"Дата преобразована в формат YYYY-MM-DD")
            print(f"Диапазон дат: {df_filtered['noted_date'].min()} - {df_filtered['noted_date'].max()}")
        except Exception as e:
            print(f"Ошибка при преобразовании даты: {e}")
    else:
        print("Колонка 'noted_date' не найдена")
    
    # 3. Очищаем температуру по 5-му и 95-му процентилю
    print("\nОчистка температуры по процентилям...")
    if 'temp' in df_filtered.columns and df_filtered['temp'].notna().any():
        # Преобразуем температуру в числовой формат
        df_filtered['temp'] = pd.to_numeric(df_filtered['temp'], errors='coerce')
        df_filtered = df_filtered.dropna(subset=['temp'])
        
        # Вычисляем процентили
        lower_percentile = df_filtered['temp'].quantile(0.05)
        upper_percentile = df_filtered['temp'].quantile(0.95)
        
        print(f"5-й процентиль: {lower_percentile:.2f}°C")
        print(f"95-й процентиль: {upper_percentile:.2f}°C")
        print(f"Минимальная температура до очистки: {df_filtered['temp'].min():.2f}°C")
        print(f"Максимальная температура до очистки: {df_filtered['temp'].max():.2f}°C")
        
        # Фильтруем данные по процентилям
        initial_count = len(df_filtered)
        df_cleaned = df_filtered[
            (df_filtered['temp'] >= lower_percentile) & 
            (df_filtered['temp'] <= upper_percentile)
        ].copy()
        
        print(f"После очистки по процентилям: {len(df_cleaned)} (удалено {initial_count - len(df_cleaned)} записей)")
        print(f"Минимальная температура после очистки: {df_cleaned['temp'].min():.2f}°C")
        print(f"Максимальная температура после очистки: {df_cleaned['temp'].max():.2f}°C")
        
        # 4. Вычисляем 5 самых жарких и самых холодных дней за год
        print("\nВычисление самых жарких и холодных дней...")
        
        if 'noted_date' in df_cleaned.columns:
            # Группируем по дате и вычисляем среднюю температуру за день
            daily_temp = df_cleaned.groupby('noted_date')['temp'].mean().reset_index()
            
            # Самые жаркие дни
            hottest_days = daily_temp.nlargest(5, 'temp')
            print("\n5 самых жарких дней:")
            for idx, row in hottest_days.iterrows():
                print(f"   {row['noted_date'].strftime('%Y-%m-%d')}: {row['temp']:.2f}°C")
            
            # Самые холодные дни
            coldest_days = daily_temp.nsmallest(5, 'temp')
            print("\n5 самых холодных дней:")
            for idx, row in coldest_days.iterrows():
                print(f"   {row['noted_date'].strftime('%Y-%m-%d')}: {row['temp']:.2f}°C")
            
            # Сохраняем результаты в файл
            results_path = "/tmp/temperature_results.csv"
            df_cleaned.to_csv(results_path, index=False)
            
            # Сохраняем информацию о жарких/холодных днях
            summary_path = "/tmp/temperature_summary.txt"
            with open(summary_path, 'w') as f:
                f.write("=== РЕЗУЛЬТАТЫ ОБРАБОТКИ ТЕМПЕРАТУРНЫХ ДАННЫХ ===\n\n")
                f.write(f"Всего записей после очистки: {len(df_cleaned)}\n")
                f.write(f"5-й процентиль: {lower_percentile:.2f}°C\n")
                f.write(f"95-й процентиль: {upper_percentile:.2f}°C\n\n")
                
                f.write("5 САМЫХ ЖАРКИХ ДНЕЙ:\n")
                for idx, row in hottest_days.iterrows():
                    f.write(f"{row['noted_date'].strftime('%Y-%m-%d')}: {row['temp']:.2f}°C\n")
                
                f.write("\n5 САМЫХ ХОЛОДНЫХ ДНЕЙ:\n")
                for idx, row in coldest_days.iterrows():
                    f.write(f"{row['noted_date'].strftime('%Y-%m-%d')}: {row['temp']:.2f}°C\n")
            
            print(f"Результаты сохранены в {results_path}")
            print(f"Сводка сохранена в {summary_path}")
            
            return results_path
        else:
            print("Колонка 'noted_date' не найдена, невозможно вычислить дни")
            return None
    else:
        print("Колонка 'temp' не найдена или содержит только NaN значения")
        return None

def create_temperature_tables():
    """Создание таблиц в базе данных"""
    print("=" * 60)
    print("Создание таблиц в базе данных...")
    print("=" * 60)
    
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Создаем таблицу для температурных данных
        create_table_query = """
        CREATE TABLE IF NOT EXISTS temperature_data (
            id SERIAL PRIMARY KEY,
            noted_date DATE,
            temp FLOAT,
            out_in VARCHAR(10),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cursor.execute(create_table_query)
        print("Таблица temperature_data создана/проверена")
        
        # Создаем таблицу для результатов
        create_summary_table_query = """
        CREATE TABLE IF NOT EXISTS temperature_summary (
            id SERIAL PRIMARY KEY,
            date DATE UNIQUE,
            avg_temp FLOAT,
            min_temp FLOAT,
            max_temp FLOAT,
            record_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cursor.execute(create_summary_table_query)
        print("Таблица temperature_summary создана/проверена")
        
        # Создаем таблицу для экстремальных значений
        create_extremes_table_query = """
        CREATE TABLE IF NOT EXISTS temperature_extremes (
            id SERIAL PRIMARY KEY,
            date DATE,
            temperature FLOAT,
            type VARCHAR(10), -- 'hot' или 'cold'
            rank INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cursor.execute(create_extremes_table_query)
        print("Таблица temperature_extremes создана/проверена")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("Все таблицы успешно созданы")
        
    except Exception as e:
        print(f"Ошибка при создании таблиц: {e}")
        raise

def save_to_database():
    """Сохранение обработанных данных в базу данных"""
    print("=" * 60)
    print("Сохранение данных в базу данных...")
    print("=" * 60)
    
    results_path = "/tmp/temperature_results.csv"
    if not os.path.exists(results_path):
        print(f"Файл {results_path} не найден")
        return
    
    df_cleaned = pd.read_csv(results_path)
    print(f"Загружено {len(df_cleaned)} записей для сохранения")
    
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Очищаем таблицу перед вставкой новых данных
        cursor.execute("TRUNCATE TABLE temperature_data")
        print("Таблица temperature_data очищена")
        
        # Вставляем данные пачками для эффективности
        batch_size = 1000
        total_rows = len(df_cleaned)
        
        for i in range(0, total_rows, batch_size):
            batch = df_cleaned.iloc[i:i+batch_size]
            
            # Подготавливаем данные для вставки
            records = []
            for _, row in batch.iterrows():
                noted_date = row.get('noted_date', None)
                temp = row.get('temp', None)
                out_in = row.get('out/in', None)
                
                if pd.notna(noted_date) and pd.notna(temp):
                    # Преобразуем дату в строку для SQL
                    if hasattr(noted_date, 'strftime'):
                        noted_date_str = noted_date.strftime('%Y-%m-%d')
                    else:
                        noted_date_str = str(noted_date)
                    
                    records.append((noted_date_str, float(temp), str(out_in)))
            
            if records:
                # Используем executemany для пакетной вставки
                insert_query = """
                INSERT INTO temperature_data (noted_date, temp, out_in)
                VALUES (%s, %s, %s)
                """
                
                cursor.executemany(insert_query, records)
            
            print(f"Вставлено {min(i + batch_size, total_rows)} из {total_rows} записей")
        
        conn.commit()
        print(f"Данные успешно сохранены в базу данных")
        
        print("\nВычисление статистики...")
        
        cursor.execute("""
        INSERT INTO temperature_summary (date, avg_temp, min_temp, max_temp, record_count)
        SELECT 
            noted_date as date,
            AVG(temp) as avg_temp,
            MIN(temp) as min_temp,
            MAX(temp) as max_temp,
            COUNT(*) as record_count
        FROM temperature_data
        GROUP BY noted_date
        ON CONFLICT (date) DO UPDATE SET
            avg_temp = EXCLUDED.avg_temp,
            min_temp = EXCLUDED.min_temp,
            max_temp = EXCLUDED.max_temp,
            record_count = EXCLUDED.record_count,
            created_at = CURRENT_TIMESTAMP;
        """)
        
        # Самые жаркие дни
        cursor.execute("TRUNCATE TABLE temperature_extremes")
        
        cursor.execute("""
        INSERT INTO temperature_extremes (date, temperature, type, rank)
        SELECT date, avg_temp, 'hot', 
               ROW_NUMBER() OVER (ORDER BY avg_temp DESC) as rank
        FROM temperature_summary
        ORDER BY avg_temp DESC
        LIMIT 5;
        """)
        
        # Самые холодные дни
        cursor.execute("""
        INSERT INTO temperature_extremes (date, temperature, type, rank)
        SELECT date, avg_temp, 'cold', 
               ROW_NUMBER() OVER (ORDER BY avg_temp ASC) as rank
        FROM temperature_summary
        ORDER BY avg_temp ASC
        LIMIT 5;
        """)
        
        conn.commit()
        
        # Выводим результаты
        print("\nСамые жаркие дни из БД:")
        cursor.execute("""
        SELECT date, temperature 
        FROM temperature_extremes 
        WHERE type = 'hot' 
        ORDER BY rank
        """)
        for row in cursor.fetchall():
            print(f"    {row[0]}: {row[1]:.2f}°C")
        
        print("\nСамые холодные дни из БД:")
        cursor.execute("""
        SELECT date, temperature 
        FROM temperature_extremes 
        WHERE type = 'cold' 
        ORDER BY rank
        """)
        for row in cursor.fetchall():
            print(f"    {row[0]}: {row[1]:.2f}°C")
        
        cursor.close()
        conn.close()
        
        print("\nВсе данные успешно сохранены и обработаны")
        
    except Exception as e:
        print(f"Ошибка при сохранении в базу данных: {e}")
        raise

def print_final_summary():
    """Вывод итоговой статистики"""
    print("=" * 60)
    print("ИТОГОВАЯ СТАТИСТИКА")
    print("=" * 60)
    
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Общая статистика
        cursor.execute("SELECT COUNT(*) FROM temperature_data")
        total_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT noted_date) FROM temperature_data")
        total_days = cursor.fetchone()[0]
        
        cursor.execute("SELECT MIN(noted_date), MAX(noted_date) FROM temperature_data")
        date_range = cursor.fetchone()
        
        cursor.execute("SELECT AVG(temp), MIN(temp), MAX(temp) FROM temperature_data")
        temp_stats = cursor.fetchone()
        
        print(f"Всего записей: {total_records}")
        print(f"Количество дней: {total_days}")
        print(f"Период данных: {date_range[0]} - {date_range[1]}")
        print(f"Средняя температура: {temp_stats[0]:.2f}°C")
        print(f"Минимальная температура: {temp_stats[1]:.2f}°C")
        print(f"Максимальная температура: {temp_stats[2]:.2f}°C")
        
        print("\n" + "=" * 60)
        print("ОБРАБОТКА ЗАВЕРШЕНА УСПЕШНО!")
        print("=" * 60)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Ошибка при получении статистики: {e}")

# Настройки DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="temperature_data_processing_v3",
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="Обработка температурных данных IoT устройств",
    tags=["etl", "temperature", "iot", "data_processing"]
) as dag:

    start_task = EmptyOperator(task_id="start")
    
    download_task = PythonOperator(
        task_id="download_temperature_dataset",
        python_callable=download_dataset
    )
    
    create_tables_task = PythonOperator(
        task_id="create_database_tables",
        python_callable=create_temperature_tables
    )
    
    process_task = PythonOperator(
        task_id="process_temperature_data",
        python_callable=process_temperature_data
    )
    
    save_db_task = PythonOperator(
        task_id="save_to_database",
        python_callable=save_to_database
    )
    
    summary_task = PythonOperator(
        task_id="print_final_summary",
        python_callable=print_final_summary
    )
    
    end_task = EmptyOperator(task_id="end")
    
    # Определяем порядок выполнения задач
    start_task >> download_task >> create_tables_task
    create_tables_task >> process_task >> save_db_task >> summary_task >> end_task