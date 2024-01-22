import mysql.connector
from db_config import DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE

def addded_goods_purchase_price():
    # З'єднання з базою даних MySQL
    conn = mysql.connector.connect(
        host=DB_HOST, 
        user=DB_USER, 
        password=DB_PASSWORD, 
        database=DB_DATABASE
    )

    # Перевірка наявності з'єднання
    if conn.is_connected():
        cursor = conn.cursor()

        try:
            # Створення таблиці для ведення журналу оброблених таблиц, якщо її ще немає
            create_log_table_query = """
            CREATE TABLE IF NOT EXISTS added_goods_processed_tables (
                id INT AUTO_INCREMENT PRIMARY KEY,
                table_name VARCHAR(255) UNIQUE
            );
            """
            cursor.execute(create_log_table_query)

            # Отримання списку таблиць, які ще не були оброблені
            get_tables_to_process_query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name LIKE 'added_products_%_products_%'
                AND table_name NOT IN (SELECT table_name FROM added_goods_processed_tables);
            """
            cursor.execute(get_tables_to_process_query)
            tables = cursor.fetchall()

            for table in tables:
                table_name = table[0]

                # SQL-запит для перевірки наявності колонки "purchase_price" у таблиці
                check_column_query = f"""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{table_name}' AND COLUMN_NAME = 'purchase_price';
                """
                cursor.execute(check_column_query)
                column = cursor.fetchone()

                # Перевірка чи існує колонка "purchase_price"
                if not column:
                    # SQL-запит для додавання нового стовпця "purchase_price" до таблиці
                    alter_table_query = f"""
                    ALTER TABLE {table_name}
                    ADD COLUMN purchase_price DECIMAL(10, 2);
                    """

                    # Виконання SQL-запиту для додавання стовпця
                    cursor.execute(alter_table_query)

                    # SQL-запит для оновлення значень у новому стовпці
                    update_query = f"""
                    UPDATE {table_name}
                    SET purchase_price = price * quantity_in_stock;
                    """

                    # Виконання SQL-запиту для оновлення даних
                    cursor.execute(update_query)

                    # Додавання обробленої таблиці до журналу
                    insert_log_query = "INSERT INTO added_goods_processed_tables (table_name) VALUES (%s);"
                    cursor.execute(insert_log_query, (table_name,))

                    print(f"Таблиця {table_name} була оновлена.")
                else:
                    print(f"Колонка 'purchase_price' вже існує у таблиці {table_name}.")

            # Підтвердження змін у базі даних
            conn.commit()

        except mysql.connector.Error as err:
            print(f"Помилка бази даних: {err}")
        finally:
            # Закриття курсора та з'єднання
            cursor.close()
            conn.close()

    else:
        print("Помилка підключення до бази даних.")

if __name__ == "__main__":
    addded_goods_purchase_price()