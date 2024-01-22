# -*- coding: utf-8 -*-
"""
Created on Tue Sep 19 18:33:12 2023

@author: Marko
"""

import mysql.connector
from db_config import DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE  # Імпортуємо змінні з config.py

def analyze_quantity_in_stock_pairs_with_price(db_config):
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
    
        # Створення таблиці для зберігання інформації про проведений аналіз
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS analyzed_quantity_in_stock_pairs (
            table1 VARCHAR(255),
            table2 VARCHAR(255),
            analyzed BOOLEAN DEFAULT FALSE,
            PRIMARY KEY (table1, table2)
        )
        """)
    
        # Отримання списку вже аналізованих пар таблиць
        cursor.execute("SELECT table1, table2 FROM analyzed_quantity_in_stock_pairs WHERE analyzed = TRUE")
        analyzed_pairs = set(cursor.fetchall())
    
        # Отримання списку таблиць products_(date)
        product_tables = [table[0] for table in tables if table[0].startswith("products_")]
    
        # Перебір таблиць та аналіз кількості товарів в парах
        for i in range(len(product_tables) - 1):
            table1 = product_tables[i]
            table2 = product_tables[i + 1]
    
            # Перевірка, чи пара таблиць вже була аналізована
            if (table1, table2) in analyzed_pairs or (table2, table1) in analyzed_pairs:
                print(f"Пара таблиць {table1} і {table2} вже була аналізована, пропускаємо.")
                continue
            
            # SQL запит для аналізу кількості товарів та ціни в парі таблиць
            analyze_query = f"""
            SELECT 
                'Decreased to 0' AS action, 
                p1.id, 
                p1.name, 
                p1.quantity_in_stock AS previous_quantity,
                p2.quantity_in_stock AS current_quantity,
                p1.price AS previous_price,
                p2.price AS current_price
            FROM 
                {table1} p1
            JOIN 
                {table2} p2 ON p1.id = p2.id
            WHERE 
                p1.quantity_in_stock > 0 AND p2.quantity_in_stock = 0;
            """
            
            # Виконання SQL запиту для аналізу кількості товарів та ціни
            cursor.execute(analyze_query)
            analysis_results = cursor.fetchall()
    
            if analysis_results:
                # Генерація імені для нової таблиці результатів
                new_table_name = f"quantity_{table1}_{table2}"
    
                # SQL запит для створення нової таблиці результатів
                create_table_query = f"""
                CREATE TABLE {new_table_name} AS
                SELECT 
                    'Decreased to 0' AS action, 
                    p1.id, 
                    p1.name, 
                    p1.quantity_in_stock AS previous_quantity,
                    p2.quantity_in_stock AS current_quantity,
                    p1.price AS previous_price,
                    p2.price AS current_price
                FROM 
                    {table1} p1
                JOIN 
                    {table2} p2 ON p1.id = p2.id
                WHERE 
                    p1.quantity_in_stock > 0 AND p2.quantity_in_stock = 0;
                """
    
                # Виконання SQL запиту для створення таблиці результатів
                cursor.execute(create_table_query)
    
                # Виведення повідомлення про створення таблиці
                print(f"Таблицю {new_table_name} створено успішно!")
    
                # Позначення пари таблиць як проаналізованої
                cursor.execute(f"INSERT INTO analyzed_quantity_in_stock_pairs (table1, table2, analyzed) VALUES ('{table1}', '{table2}', TRUE)")
        
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
    analyze_quantity_in_stock_pairs_with_price(db_config)
