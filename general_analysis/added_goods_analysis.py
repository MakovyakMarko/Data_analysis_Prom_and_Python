# -*- coding: utf-8 -*-
"""
Created on Tue Sep 19 18:33:12 2023

@author: Marko
"""

import mysql.connector
from db_config import DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE  # Імпортуємо змінні з config.py

def analyze_and_create_tables_with_price():
    # Параметри підключення до бази даних MySQL
    db_config = {
        "host": DB_HOST,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "database": DB_DATABASE,
    }
    
    try:
        # Підключення до бази даних
        conn = mysql.connector.connect(**db_config)
    
        # Створення курсора для виконання SQL запитів
        cursor = conn.cursor()
    
        # Отримання списку таблиць з бази даних
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        selected_tables = [table[0] for table in tables if table[0].startswith("products_")]
    
        # Створення таблиці для зберігання інформації про проведений аналіз
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS analyzed_pairs_of_added_goods (
            table1 VARCHAR(255),
            table2 VARCHAR(255),
            analyzed BOOLEAN DEFAULT FALSE,
            PRIMARY KEY (table1, table2)
        )
        """)
    
        # Отримання списку вже аналізованих пар таблиць
        cursor.execute("SELECT table1, table2 FROM analyzed_pairs_of_added_goods WHERE analyzed = TRUE")
        analyzed_pairs_of_added_goods = set(cursor.fetchall())
    
        # Перебір таблиць та порівняння їх попарно
        for i in range(len(tables) - 1):
            table1 = tables[i][0]
            table2 = tables[i + 1][0]
    
            # Перевірка, чи починаються імена таблиць з "products_"
            if table1.startswith("products_") and table2.startswith("products_"):
    
                # Перевірка, чи пара таблиць вже була аналізована
                if (table1, table2) in analyzed_pairs_of_added_goods or (table2, table1) in analyzed_pairs_of_added_goods:
                    print(f"Пара таблиць {table1} і {table2} вже була аналізована, пропускаємо.")
                    continue
                # SQL запит для порівняння кількості записів в обох таблицях
                count_query = f"""
                SELECT COUNT(*) FROM {table1}
                UNION ALL
                SELECT COUNT(*) FROM {table2}
                """
                
                # Виконання SQL запиту для отримання кількості записів у кожній таблиці
                cursor.execute(count_query)
                counts = cursor.fetchall()
    
                # Перевірка, чи були зміни в парі таблиць
                if counts[0] != counts[1]:
                    # Генерація імені для нової таблиці результатів
                    new_table_name = f"added_{table1}_{table2}"
        
                    # SQL запит для створення нової таблиці
                    create_table_query = f"""
                    CREATE TABLE {new_table_name} AS
                    SELECT IFNULL({table1}.id, NULL) AS id,
                           IFNULL({table1}.name, NULL) AS name,
                           IFNULL({table1}.name_ua, NULL) AS name_ua,
                           IFNULL({table1}.price, NULL) AS price,
                           IFNULL({table1}.quantity_in_stock, NULL) AS quantity_in_stock  -- Додали стовпець quantity_in_stock
                           -- Продовжте для інших полів
                    FROM {table1}
                    LEFT JOIN {table2} ON {table1}.id = {table2}.id
                    WHERE {table2}.id IS NULL
                    """
        
                    # Виконання SQL запиту для створення таблиці
                    cursor.execute(create_table_query)
        
                    # Виведення повідомлення про створення таблиці
                    print(f"Таблицю {new_table_name} створено успішно!")
        
                    # SQL запит для вставки результатів
                    insert_data_query = f"""
                    INSERT INTO {new_table_name} (id, name, name_ua, price, quantity_in_stock)  -- Додали стовпець quantity_in_stock
                    SELECT IFNULL({table1}.id, NULL) AS id,
                           IFNULL({table1}.name, NULL) AS name,
                           IFNULL({table1}.name_ua, NULL) AS name_ua,
                           IFNULL({table1}.price, NULL) AS price,
                           IFNULL({table1}.quantity_in_stock, NULL) AS quantity_in_stock  -- Додали стовпець quantity_in_stock
                           -- Продовжте для інших полів
                    FROM {table1}
                    LEFT JOIN {table2} ON {table1}.id = {table2}.id
                    WHERE {table2}.id IS NULL
                    """
        
                    # Виконання SQL запиту для вставки даних
                    cursor.execute(insert_data_query)
        
                    # Виведення повідомлення про вставку даних
                    print(f"Дані вставлено до таблиці {new_table_name} успішно!")
        
                # Позначення пари таблиць як проаналізованої
                cursor.execute(f"INSERT INTO analyzed_pairs_of_added_goods (table1, table2, analyzed) VALUES ('{table1}', '{table2}', TRUE)")
        
                # Збереження змін до бази даних
                conn.commit()
        
    except mysql.connector.Error as e:
        print(f"Помилка: {e}")
    
    finally:
        # Закриття курсора та з'єднання з базою даних
        if cursor:
            cursor.close()
        if conn:
            conn.close()
                
                
if __name__ == "__main__":
    analyze_and_create_tables_with_price()