# -*- coding: utf-8 -*-
"""
Created on Mon Oct 16 10:54:15 2023

@author: Marko
"""

from pytrends.request import TrendReq

# Створення з'єднання з Google Trends API
pytrend = TrendReq()

# Встановлення параметрів пошуку
kw_list = ["товар1", "товар2", "товар3"]  # Список товарів, які ви хочете перевірити

# Отримання даних з Google Trends
pytrend.build_payload(kw_list, timeframe='today 5-y', geo='US')  # Встановлення параметрів за вашим вибором
data = pytrend.interest_over_time()

# Аналіз та обробка отриманих даних
# Робіть необхідний аналіз для визначення найпопулярніших товарів