import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
import subprocess

# URL сторінки, яку ми моніторимо
url = "https://www.websklad.biz.ua/?page_id=61486"

# Шлях до теки для збереження XML-файлів
download_path = r"M:\Dropshipping"

# Останнє оновлення дати
last_update = None

# Головна функція для моніторингу та завантаження
def monitor_and_download():
    global last_update
    
    try:
        # Отримуємо вміст сторінки
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Отримуємо значення селекторів
        status_selector = "#main > div.user-custom-export-container > div:nth-child(3) > div.user-export-value.completed.status"
        date_selector = "#main > div.user-custom-export-container > div:nth-child(3) > div.user-export-value.date"
        link_selector = "#main > div.user-custom-export-container > div:nth-child(3) > div.user-export-value.link > a"
        
        status = soup.select_one(status_selector).text.strip()
        date_str = soup.select_one(date_selector).text.strip()
        link = soup.select_one(link_selector)["href"]
        
        # Перевіряємо, чи статус "Вигрузка створена"
        if status == "Вигрузка створена":
            # Перетворюємо рядок з датою у об'єкт datetime
            date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            
            # Якщо це перше оновлення або дата оновлення більша за попередню
            if last_update is None or date > last_update:
                last_update = date  # Оновлюємо останнє оновлення дати
                
                # Завантажуємо XML-файл
                response = requests.get(link)
                
                # Отримуємо ім'я файлу з URL
                file_name = os.path.basename(link)
                
                # Зберігаємо файл у теку
                with open(os.path.join(download_path, file_name), "wb") as file:
                    file.write(response.content)
                    print(f"Завантажено файл: {file_name}")
                    
                # Викликаємо ваш модуль обробки файлу general_module
                subprocess.run(["python", "./general_analysis/general_module.py", os.path.join(download_path, file_name)])
    
    except Exception as e:
        print(f"Помилка: {str(e)}")

# Створюємо планувальник
scheduler = BackgroundScheduler()
scheduler.add_job(monitor_and_download, 'cron', hour='*', minute='14')

if __name__ == "__main__":
    scheduler.start()
    
    try:
        while True:
            pass
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()