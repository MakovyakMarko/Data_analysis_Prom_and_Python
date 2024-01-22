import os
import re
import xml.etree.ElementTree as ET
import mysql.connector
from datetime import datetime
from db_config import DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE  # Імпортуємо змінні з config.py

def process_xml_to_database():

    xml_directory = 'M:/Dropshipping/'
    
    # Отримати повний шлях до файлу "randomize_prom_64744.xml"
    xml_file_path = os.path.join(xml_directory, "randomize_prom_64744.xml")
    
    params = {}
    
    # Перевірка наявності файлу
    if os.path.exists(xml_file_path):
        # Отримати поточний час
        current_time = datetime.now().strftime("%Y%m%d%H%M%S")
    
        # Оновити ім'я файлу, додаючи до нього поточний час
        new_file_name = f"randomize_prom_64744_{current_time}.xml"
        new_xml_file_path = os.path.join(xml_directory, new_file_name)
    
        # Перейменувати файл
        os.rename(xml_file_path, new_xml_file_path)
    
        # Завантажити XML файл
        tree = ET.parse(new_xml_file_path)
        root = tree.getroot()
    else:
        print(f"Файл 'randomize_prom_64744.xml' не знайдено.")
        
    # Підключення до бази даних
    db = mysql.connector.connect(
        host= DB_HOST, 
        user= DB_USER, 
        password= DB_PASSWORD, 
        database= DB_DATABASE  
    )
    cursor = db.cursor()
    
    # Отримання поточного часу (перенесено сюди для використання в таблицях)
    current_time = datetime.now().strftime("%Y%m%d%H%M%S")
    table_name = f"products_{current_time}"
    
    for offer_id, param_data in params.items():
        # Перевірка чи існує відповідний offer_id в таблиці товарів
        check_product_sql = f"SELECT id FROM {table_name} WHERE id = %s"
        cursor.execute(check_product_sql, (offer_id,))
        product_exists = cursor.fetchone()
    
        if product_exists:
            for param_name, param_value in param_data.items():
                # SQL-запит для вставки даних в таблицю параметрів (param)
                insert_param_sql = f"""
                INSERT INTO param_{table_name} (name, value, offer_id)
                VALUES (%s, %s, %s)
                """
                param_values = (param_name, param_value, offer_id)
                cursor.execute(insert_param_sql, param_values)
        else:
            print(f"Товар з offer_id={offer_id} не існує в таблиці {table_name}. Пропускаємо.")
    
    
    # SQL-запит для створення таблиці products
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(1024),
        name_ua VARCHAR(1024),
        price DECIMAL(10, 2),
        description TEXT,
        url VARCHAR(1024),
        vendor_code VARCHAR(1024),
        category_id INT,
        country_of_origin VARCHAR(1024),
        store VARCHAR(1024),
        pickup VARCHAR(1024),
        delivery VARCHAR(1024),
        currency_id VARCHAR(1024),
        quantity_in_stock INT,
        keywords TEXT
    )
    """
    cursor.execute(create_table_sql)
    db.commit()
    # SQL-запит для створення таблиці параметрів (param)
    create_param_table_sql = f"""
    CREATE TABLE IF NOT EXISTS param_{table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(1024),
        value TEXT,
        offer_id INT,  
        FOREIGN KEY (offer_id) REFERENCES {table_name}(id)
    )
    """
    cursor.execute(create_param_table_sql)
    db.commit()
    # SQL-запит для створення таблиці, яка зв'яже товари з їх параметрами
    create_product_param_link_table_sql = f"""
    CREATE TABLE IF NOT EXISTS product_param_link_{table_name} (
        product_id INT,
        param_id INT,
        FOREIGN KEY (product_id) REFERENCES {table_name}(id),
        FOREIGN KEY (param_id) REFERENCES param_{table_name}(id)
    )
    """
    cursor.execute(create_product_param_link_table_sql)
    db.commit()
    
    # Виконання SQL-запитів для створення таблиць
    db.commit()
    
    # Проходження через елементи <offer> у XML файлі і вставка даних в таблиці products та param
    for offer in root.findall('.//offer'):
        offer_id = offer.get('id')
        name = offer.find('name').text
        name_ua = offer.find('name_ua').text
        price = offer.find('price').text
        description = offer.find('.//description').text
        url = offer.find('url').text
        vendor_code = offer.find('vendorCode').text
        category_id = offer.find('categoryId').text
        
        store = offer.find('store').text
        pickup = offer.find('pickup').text
        delivery = offer.find('delivery').text
        currency_id = offer.find('currencyId').text
        quantity_in_stock = offer.find('quantity_in_stock').text
        
        country_of_origin = offer.find('country_of_origin')
        country_of_origin = country_of_origin.text if country_of_origin is not None else ""
    
        # Перевірка наявності елементу <keywords>
        keywords_element = offer.find('keywords')
        keywords = keywords_element.text if keywords_element is not None else ""
    
        # Опис товару (враховуючи CDATA)
        description_element = offer.find('.//description')
        description = description_element.text if description_element is not None else ""
    
        # SQL-запит для вставки даних в таблицю products
        sql = f"INSERT INTO {table_name} (id, name, name_ua, price, description, url, vendor_code, category_id, country_of_origin, store, pickup, delivery, currency_id, quantity_in_stock, keywords) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        values = (offer_id, name, name_ua, price, description, url, vendor_code, category_id, country_of_origin, store, pickup, delivery, currency_id, quantity_in_stock, keywords)
        cursor.execute(sql, values)
        # Отримання ідентифікатора створеного запису
        product_id = cursor.lastrowid
        # Знаходимо всі елементи 'param' в поточному 'offer'
        params_elements = offer.findall('.//param')
    
        # Проходимо через кожен 'param' і додаємо його до таблиці param
        for param_element in params_elements:
            param_name = param_element.get('name')
            param_value = param_element.text
    
            # Перевірка на None перед отриманням тексту, оскільки деякі елементи можуть бути відсутніми
            if param_value is not None:
                # SQL-запит для вставки даних в таблицю параметрів (param)
                insert_param_sql = f"""
                INSERT INTO param_{table_name} (name, value, offer_id)
                VALUES (%s, %s, %s)
                """
                param_values = (param_name, param_value, offer_id)
                cursor.execute(insert_param_sql, param_values)
                # Отримання ідентифікатора створеного запису
                param_id = cursor.lastrowid
        insert_link_sql = f"""
        INSERT INTO product_param_link_{table_name} (product_id, param_id)
        VALUES (%s, %s)
        """
        link_values = (product_id, param_id)
        cursor.execute(insert_link_sql, link_values)
    # Збереження змін до бази даних
    db.commit()
    
    # Закриття підключення
    cursor.close()
    db.close()
    
if __name__ == "__main__":
    process_xml_to_database()