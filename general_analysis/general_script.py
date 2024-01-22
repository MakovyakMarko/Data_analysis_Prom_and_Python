# -*- coding: utf-8 -*-
"""
Created on Fri Sep 15 10:45:21 2023

@author: Marko
"""
import time
import building_table_with_data # Замість "first_module" вкажіть ім'я вашого першого модуля
import create_new_tables_for_analysis  # Імпортуємо модуль
import script_for_quantity_not_zero_analisys
import added_goods_analysis
import decrease_to_zero_analysis
import decrease_analysis
import quantity_increase
import total_cost_added_goods
import profit_analysis
import cost_of_increased_goods
import decrease_to_zero_price_analysis
from db_config import DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE  # Імпортуємо змінні з db_config.py

if __name__ == "__main__":
    db_config = {
        "host": DB_HOST,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "database": DB_DATABASE
    }
    # Виклик першого модуля
    building_table_with_data.process_xml_to_database()
    # Викликаємо функцію з імпортованого модуля, передаючи параметри підключення до бази даних
    create_new_tables_for_analysis.create_processed_tables_table(db_config)
    create_new_tables_for_analysis.process_data(db_config)
    script_for_quantity_not_zero_analisys.main()
    added_goods_analysis.analyze_and_create_tables_with_price()
    decrease_to_zero_analysis.analyze_quantity_in_stock_pairs_with_price(db_config)
    decrease_analysis.analyze_quantity_decreased_for_all_tables_with_price(db_config)
    quantity_increase.analyze_quantity_increased_for_all_tables(db_config)
    time.sleep(10)
    total_cost_added_goods.addded_goods_purchase_price()
    profit_analysis.profit_analysis()
    cost_of_increased_goods.cost_of_increased_goods()
    decrease_to_zero_price_analysis.decrease_to_zero_price_analysis()