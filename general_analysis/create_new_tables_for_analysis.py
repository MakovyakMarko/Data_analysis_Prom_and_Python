import mysql.connector
from datetime import datetime
from db_config import DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE  # Імпортуємо змінні з db_config.py

# Функція для створення таблиці контролю оброблених даних
def create_processed_tables_table(db_config):
    try:
        # Підключення до бази даних
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Створення таблиці для відстеження оброблених таблиц
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_tables (
                table_name VARCHAR(255) PRIMARY KEY,
                processing_date DATETIME
            )
        """)

        print("Таблицю processed_tables створено або вже існує.")

    except mysql.connector.Error as err:
        print("Помилка MySQL:", err)

    finally:
        # Закриття підключення до бази даних
        if connection.is_connected():
            cursor.close()
            connection.close()

# Функція для внесення інформації про оброблену таблицю
def mark_table_as_processed(db_config, table_name):
    try:
        # Підключення до бази даних
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Внесення запису про оброблену таблицю
        processing_date = datetime.now()
        cursor.execute("INSERT INTO processed_tables (table_name, processing_date) VALUES (%s, %s)",
                       (table_name, processing_date))

        # Збереження змін до бази даних
        connection.commit()
        print(f"Таблицю {table_name} відзначено як оброблену.")

    except mysql.connector.Error as err:
        print("Помилка MySQL:", err)

    finally:
        # Закриття підключення до бази даних
        if connection.is_connected():
            cursor.close()
            connection.close()

# Функція для витягнення даних і створення нової таблиці
def is_table_processed(cursor, table_name):
    cursor.execute("SELECT COUNT(*) FROM processed_tables WHERE table_name = %s", (table_name,))
    result = cursor.fetchone()
    return result[0] > 0

def process_data(db_config):
    try:
        # Підключення до бази даних
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Отримання списку таблиць
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        for table in tables:
            table_name = table[0]

            # Перевірка, чи ім'я таблиці відповідає формату "products_(date)"
            if table_name.startswith("products_"):
                # Перевірка, чи дана таблиця вже була оброблена
                if not is_table_processed(cursor, table_name):
                    # Вибрані колонки та умова
                    columns = ["id", "name_ua", "price", "url", "vendor_code", "category_id", "currency_id", "quantity_in_stock"]
                    condition = "quantity_in_stock > 0"

                    # Створення запиту для витягнення даних
                    query = f"SELECT {', '.join(columns)} FROM {table_name} WHERE {condition}"

                    # Створення ім'я нової таблиці для аналізу
                    new_table_name = f"quantity_not_zero_{table_name}"

                    # Створення нової таблиці для аналізу
                    cursor.execute(f"CREATE TABLE IF NOT EXISTS {new_table_name} AS {query}")

                    # Внесення запису про оброблену таблицю
                    mark_table_as_processed(db_config, table_name)

        # Збереження змін до бази даних
        connection.commit()
        print("Дані для аналізу створено.")

    except mysql.connector.Error as err:
        print("Помилка MySQL:", err)

    finally:
        # Закриття підключення до бази даних
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    # Параметри підключення до бази даних з модуля2
    db_config = {
        "host": DB_HOST,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "database": DB_DATABASE
    }

    # Створення таблиці для оброблених таблиц
    create_processed_tables_table(db_config)

    # Обробка даних і ведення журналу оброблених таблиц
    process_data(db_config)