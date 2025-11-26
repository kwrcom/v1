# Используем официальный образ Apache Airflow 2.1.5 на базе Python 3.10
# Этот образ содержит все необходимые компоненты Airflow
FROM apache/airflow:2.1.5-python3.10

# Переключаемся на root для установки системных зависимостей
USER root

# Устанавливаем системные зависимости, необходимые для компиляции Python пакетов
# build-essential - компиляторы C/C++ для сборки нативных расширений
# libpq-dev - библиотеки PostgreSQL для psycopg2
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Возвращаемся к пользователю airflow для безопасности
USER airflow

# Копируем файл с зависимостями Python
COPY requirements.txt /requirements.txt

# Обновляем pip до последней версии для корректной установки пакетов
RUN pip install --no-cache-dir --upgrade pip

# Устанавливаем Python зависимости из requirements.txt
# --no-cache-dir - не кэшируем загруженные пакеты для уменьшения размера образа
# -r - читаем зависимости из файла
RUN pip install --no-cache-dir -r /requirements.txt

# Копируем модуль app_models для ML обучения
COPY app_models /opt/airflow/app_models

# Устанавливаем рабочую директорию
WORKDIR /opt/airflow

# При запуске контейнера будет использована команда по умолчанию из базового образа
# (обычно это airflow webserver или scheduler)
