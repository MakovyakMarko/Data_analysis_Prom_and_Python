# -*- coding: utf-8 -*-
"""
Created on Mon Sep 25 19:33:19 2023

@author: Marko
"""

import tkinter as tk
from tkinter import ttk
import threading
import subprocess

# Функція для запуску модуля у фоновому потоці
def run_module():
    subprocess.Popen(["python", "file_download_module.py"])

# Створюємо головне вікно
root = tk.Tk()
root.title("Графічний інтерфейс для запуску модуля")

# Створюємо мітку
label = ttk.Label(root, text="Натисніть кнопку, щоб запустити модуль у фоновому режимі:")
label.pack(pady=10)

# Створюємо кнопку для запуску модуля
run_button = ttk.Button(root, text="Запустити модуль", command=run_module)
run_button.pack(pady=10)

# Функція для завершення роботи графічного інтерфейсу
def on_closing():
    root.destroy()

root.protocol("WM_DELETE_WINDOW", on_closing)

# Запускаємо головний цикл графічного інтерфейсу
root.mainloop()
