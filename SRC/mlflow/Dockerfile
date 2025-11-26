# Используем официальный образ Python 3.10 slim (облегченная версия)
# Slim версия уменьшает размер образа, содержит только необходимые компоненты
FROM python:3.10-slim

# Устанавливаем метаданные образа
LABEL maintainer="MLflow Server"
LABEL description="Custom MLflow tracking server with PostgreSQL and MinIO support"

# Переменные окружения для Python
# PYTHONUNBUFFERED=1 - выводить логи без буферизации для реального времени
# PYTHONDONTWRITEBYTECODE=1 - не создавать .pyc файлы
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Устанавливаем системные зависимости
# libpq-dev - библиотеки PostgreSQL
# gcc - компилятор для сборки Python пакетов с C расширениями
# g++ - компилятор C++ (для некоторых зависимостей)
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Создаем директорию для MLflow artifacts
# Эта директория будет использоваться для временного хранения артефактов
RUN mkdir -p /mlflow/artifacts

# Устанавливаем рабочую директорию
WORKDIR /mlflow

# Копируем файл с зависимостями Python
COPY requirements.txt /mlflow/requirements.txt

# Обновляем pip и устанавливаем wheel для ускорения установки пакетов
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Устанавливаем Python зависимости из requirements.txt
# --no-cache-dir - экономим место, не сохраняя кэш pip
RUN pip install --no-cache-dir -r /mlflow/requirements.txt

# Открываем порт 5000 для MLflow UI и API
EXPOSE 5000

# Команда запуска MLflow сервера
# --host 0.0.0.0 - слушаем на всех интерфейсах
# --port 5000 - порт веб-интерфейса
# backend-store-uri и default-artifact-root будут переданы через docker-compose
CMD ["mlflow", "server", "--host", "0.0.0.0", "--port", "5000"]
