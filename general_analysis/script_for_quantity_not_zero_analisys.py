import pandas as pd
from sqlalchemy import create_engine
from db_config import DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE
import logging

# Налаштування логування
logging.basicConfig(filename='script.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def connect_to_database():
    try:
        db_connection_str = f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_DATABASE}'
        engine = create_engine(db_connection_str)
        return engine
    except Exception as error:
        logging.error(f"Помилка при підключенні до бази даних: {error}")
        return None

def create_q_not_zero_processed_tables_table(engine):
    try:
        with engine.connect() as connection:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS q_not_zero_processed_tables (
                id INT AUTO_INCREMENT PRIMARY KEY,
                table_name VARCHAR(255) NOT NULL,
                UNIQUE KEY (table_name)
            );
            """
            connection.execute(create_table_query)
            logging.info("Таблицю q_not_zero_processed_tables створено або вже існує.")
    except Exception as error:
        logging.error(f"Помилка при створенні таблиці q_not_zero_processed_tables: {error}")

def create_results_table(engine):
    try:
        with engine.connect() as connection:
            create_results_table_query = """
            CREATE TABLE IF NOT EXISTS quantity_analysis_results (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(255) NOT NULL,
                total_items INT,
                total_quantity INT
            );
            """
            connection.execute(create_results_table_query)
            logging.info("Таблицю quantity_analysis_results створено або вже існує.")
    except Exception as error:
        logging.error(f"Помилка при створенні таблиці quantity_analysis_results: {error}")

def get_tables_with_pattern(engine, pattern):
    try:
        with engine.connect() as connection:
            tables = connection.execute(f"SHOW TABLES LIKE '{pattern}';")
            return [table[0] for table in tables]
    except Exception as error:
        logging.error(f"Помилка при отриманні списку таблиць: {error}")
        return []

def calculate_total_quantity(engine, tables):
    try:
        with engine.connect() as connection:
            for table in tables:
                if not is_table_processed(connection, table):
                    query_total_items = f"SELECT COUNT(*) as total_items FROM {table};"
                    total_items_result = connection.execute(query_total_items)
                    total_items = total_items_result.fetchone()[0]

                    query_total_quantity = f"SELECT SUM(quantity_in_stock) as total_quantity FROM {table};"
                    total_quantity_result = connection.execute(query_total_quantity)
                    total_quantity = total_quantity_result.fetchone()[0]

                    logging.info(f"Таблиця: {table}")
                    logging.info(f"Загальна кількість позицій в наявності: {total_items}")
                    logging.info(f"Загальна кількість товарів в наявності: {total_quantity}")

                    save_results_to_table(engine, table, total_items, total_quantity)
                    mark_table_as_processed(connection, table)
    except Exception as error:
        logging.error(f"Помилка при обчисленні кількості товарів: {error}")

def save_results_to_table(engine, table_name, total_items, total_quantity):
    try:
        with engine.connect() as connection:
            insert_query = """
            INSERT INTO quantity_analysis_results (table_name, total_items, total_quantity)
            VALUES (%s, %s, %s);
            """
            connection.execute(insert_query, (table_name, total_items, total_quantity))
            logging.info(f"Результати аналізу для таблиці {table_name} збережено в таблицю analysis_results.")
    except Exception as error:
        logging.error(f"Помилка при збереженні результатів аналізу: {error}")

def is_table_processed(connection, table_name):
    try:
        query = f"SELECT COUNT(*) FROM q_not_zero_processed_tables WHERE table_name = '{table_name}';"
        result = connection.execute(query)
        count = result.fetchone()[0]
        return count > 0
    except Exception as error:
        logging.error(f"Помилка при перевірці обробки таблиці {table_name}: {error}")
        return False

def mark_table_as_processed(connection, table_name):
    try:
        insert_query = f"INSERT IGNORE INTO q_not_zero_processed_tables (table_name) VALUES ('{table_name}');"
        connection.execute(insert_query)
    except Exception as error:
        logging.error(f"Помилка при позначенні таблиці {table_name} як обробленої: {error}")

def main():
    engine = connect_to_database()
    if engine:
        create_q_not_zero_processed_tables_table(engine)  # Створення таблиці для відстеження оброблених таблиц
        create_results_table(engine)  # Створення таблиці для результатів аналізу
        pattern = "quantity_not_zero_products_%"
        tables = get_tables_with_pattern(engine, pattern)
        if tables:
            calculate_total_quantity(engine, tables)

if __name__ == "__main__":
    main()
