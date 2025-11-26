"""
Kafka Producer для генерации синтетических транзакций

Генерирует синтетические транзакции и отправляет их в Kafka топик 'transactions'.
Использует UTC для временных меток и реалистичную маркировку мошенничества (1-2%).
"""

import os
import json
import time
import random
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any

import numpy as np
from faker import Faker
from scipy.stats import lognorm, truncnorm
from confluent_kafka import Producer
from jsonschema import validate, ValidationError

# Конфигурация
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
BATCH_DELAY_SECONDS = float(os.getenv("BATCH_DELAY_SECONDS", "1.0"))
SEED = int(os.getenv("SEED", "42"))

# Инициализация генератора
random.seed(SEED)
np.random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

# JSON Schema для валидации транзакций
TRANSACTION_SCHEMA = {
    "type": "object",
    "required": ["transaction_id", "user_id", "merchant_id", "timestamp", "amount", "currency", "is_fraud"],
    "properties": {
        "transaction_id": {"type": "string"},
        "user_id": {"type": "string"},
        "merchant_id": {"type": "string"},
        "timestamp": {"type": "string"},
        "amount": {"type": "number", "minimum": 0},
        "currency": {"type": "string"},
        "is_fraud": {"type": "integer", "minimum": 0, "maximum": 1}
    }
}


class TransactionProducer:
    """
    Класс для непрерывной генерации и отправки синтетических транзакций в Kafka
    """
    
    def __init__(self):
        # Инициализация Kafka Producer
        self.producer = Producer({
            'bootstrap.servers': KAFKA_BROKER,
            'client.id': f'transaction-producer-{os.getpid()}'
        })
        
        # Счетчики для статистики
        self.total_sent = 0
        self.total_fraud = 0
        self.transaction_counter = 0
        
        # Предопределенные данные для генерации
        self.countries = ["US", "GB", "DE", "FR", "RU", "KZ", "IN", "CN", "BR", "NG"]
        self.currencies = {
            "US": "USD", "GB": "GBP", "DE": "EUR", "FR": "EUR",
            "RU": "RUB", "KZ": "KZT", "IN": "USD", "CN": "USD",
            "BR": "USD", "NG": "USD"
        }
        self.merchant_categories = [
            "electronics", "groceries", "travel", "entertainment",
            "fashion", "services", "gas", "food_delivery"
        ]
        self.device_types = ["mobile", "desktop", "tablet"]
        
        print(f"✅ Producer initialized. Kafka Broker: {KAFKA_BROKER}, Topic: {KAFKA_TOPIC}")
    
    def _hash_ip(self, ip: str) -> str:
        """Хэширование IP адреса для приватности"""
        return hashlib.sha1(ip.encode()).hexdigest()[:16]
    
    def _truncated_normal(self, mean: float, sd: float, low: float, high: float) -> float:
        """Генерация случайного числа из обрезанного нормального распределения"""
        a, b = (low - mean) / sd, (high - mean) / sd
        return truncnorm.rvs(a, b, loc=mean, scale=sd)
    
    def _determine_fraud_label(self, txn: Dict[str, Any]) -> int:
        """
        Определение метки мошенничества на основе паттернов
        
        Паттерны мошенничества:
        - Account takeover: большая сумма + ночное время + необычное устройство
        - Card testing: множество мелких транзакций подряд
        - Geographic anomaly: транзакция из необычной страны
        
        Итоговый процент мошенничества: 1-2%
        """
        fraud_score = 0.0
        
        # Базовая вероятность мошенничества
        base_fraud_prob = 0.002
        
        # Паттерн 1: Account Takeover
        # Большая сумма (>$1000) + ночное время (0-5 часов) + высокий риск мерчанта
        if txn["amount"] > 1000 and txn["transaction_hour"] in [0, 1, 2, 3, 4, 5]:
            fraud_score += 0.3
            if txn["merchant_risk_score"] > 0.5:
                fraud_score += 0.2
        
        # Паттерн 2: Card Testing
        # Множество транзакций в короткий промежуток времени
        if txn.get("prev_transaction_count_1h", 0) > 5:
            fraud_score += 0.25
        
        # Паттерн 3: Geographic Anomaly
        # Транзакция из страны, отличной от домашней
        if txn["country"] != txn.get("home_country", txn["country"]):
            fraud_score += 0.15
        
        # Паттерн 4: Аномальная сумма
        # Сумма значительно превышает типичные траты пользователя
        typical_spend = txn.get("typical_spend", 100)
        if txn["amount"] > typical_spend * 10:
            fraud_score += 0.25
        
        # Паттерн 5: Высокий риск мерчанта
        if txn["merchant_risk_score"] > 0.7:
            fraud_score += 0.2
        
        # Паттерн 6: Скорость транзакций (velocity)
        if txn.get("velocity_1h", 0) > 3.0:
            fraud_score += 0.15
        
        # Добавляем случайный шум
        fraud_score += np.random.beta(1, 50) * 0.1
        
        # Преобразуем в вероятность через sigmoid
        fraud_prob = 1 / (1 + np.exp(-(fraud_score * 2.0)))
        final_prob = base_fraud_prob + (fraud_prob * 0.05)
        
        # Принудительная маркировка для достижения 1-2% мошенничества
        # Используем случайное значение для принятия решения
        is_fraud = 1 if random.random() < final_prob else 0
        
        return is_fraud
    
    def _generate_transaction(self) -> Dict[str, Any]:
        """
        Генерация одной синтетической транзакции
        
        Использует UTC для временных меток (datetime.now(tz=timezone.utc))
        """
        self.transaction_counter += 1
        
        # Базовые идентификаторы
        txn_id = f"txn_{self.transaction_counter:09d}"
        user_id = f"user_{random.randint(1, 5000):06d}"
        merchant_id = f"m_{random.randint(1, 1000):05d}"
        
        # Временная метка в UTC (важно для избежания проблем с часовыми поясами)
        timestamp = datetime.now(tz=timezone.utc)
        transaction_hour = timestamp.hour
        
        # Генерация суммы транзакции
        # Используем log-normal распределение для реалистичности
        typical_spend = random.uniform(10, 500)
        amount = lognorm(s=1.0, scale=typical_spend).rvs()
        
        # Редкие случаи очень больших сумм (могут быть мошенническими)
        if random.random() < 0.002:
            amount *= random.uniform(10, 50)
        
        amount = round(float(amount), 2)
        
        # География и валюта
        country = random.choices(
            self.countries,
            weights=[30, 8, 6, 6, 10, 4, 8, 10, 10, 8],
            k=1
        )[0]
        home_country = random.choices(self.countries, k=1)[0]
        currency = self.currencies.get(country, "USD")
        
        # Мерчант
        merchant_category = random.choices(
            self.merchant_categories,
            weights=[10, 20, 8, 10, 12, 15, 8, 17],
            k=1
        )[0]
        
        # Риск мерчанта: базовый риск + случайный компонент
        base_risk = {
            "electronics": 0.15, "groceries": 0.03, "travel": 0.12,
            "entertainment": 0.06, "fashion": 0.07, "services": 0.05,
            "gas": 0.04, "food_delivery": 0.08
        }.get(merchant_category, 0.05)
        
        merchant_risk_score = min(0.95, max(0.01, np.random.beta(2, 20) + base_risk))
        
        # IP и устройство
        ip_address = self._hash_ip(fake.ipv4())
        device_type = random.choices(self.device_types, weights=[0.6, 0.3, 0.1], k=1)[0]
        os_name = random.choice(["Windows", "macOS", "Linux", "Android", "iOS"])
        browser = random.choice(["Chrome", "Firefox", "Safari", "Edge"])
        
        # Поведенческие признаки (простая симуляция)
        prev_transaction_count_1h = random.randint(0, 10)
        prev_transaction_count_24h = random.randint(0, 50)
        velocity_1h = random.uniform(0, 5)
        
        # Формируем транзакцию
        transaction = {
            "transaction_id": txn_id,
            "user_id": user_id,
            "merchant_id": merchant_id,
            "timestamp": timestamp.isoformat(),
            "transaction_date": timestamp.date().isoformat(),
            "transaction_hour": transaction_hour,
            "amount": amount,
            "currency": currency,
            "country": country,
            "home_country": home_country,
            "city": fake.city(),
            "ip_address": ip_address,
            "device_type": device_type,
            "os": os_name,
            "browser": browser,
            "merchant_category": merchant_category,
            "merchant_risk_score": round(merchant_risk_score, 3),
            "mcc_code": random.randint(3000, 5999),
            "prev_transaction_count_1h": prev_transaction_count_1h,
            "prev_transaction_count_24h": prev_transaction_count_24h,
            "velocity_1h": round(velocity_1h, 3),
            "typical_spend": round(typical_spend, 2),
        }
        
        # Определяем метку мошенничества на основе паттернов
        transaction["is_fraud"] = self._determine_fraud_label(transaction)
        
        return transaction
    
    def _validate_transaction(self, txn: Dict[str, Any]) -> bool:
        """
        Валидация транзакции через JSON Schema
        
        Возвращает True если валидация успешна, False в противном случае
        """
        try:
            validate(instance=txn, schema=TRANSACTION_SCHEMA)
            return True
        except ValidationError as e:
            print(f"❌ Validation error: {e.message}")
            return False
    
    def _delivery_callback(self, err, msg):
        """Callback для подтверждения доставки сообщения в Kafka"""
        if err:
            print(f"❌ Message delivery failed: {err}")
        else:
            # Успешная доставка (логируем только каждую 1000-ю)
            if self.total_sent % 1000 == 0:
                print(f"✅ Delivered {self.total_sent} messages to {msg.topic()} [{msg.partition()}]")
    
    def run_continuous_production(self):
        """
        Основной метод: непрерывная генерация и отправка транзакций в Kafka
        
        Генерирует батчи транзакций с заданной задержкой
        """
        print(f"🚀 Starting continuous transaction generation...")
        print(f"   Batch size: {BATCH_SIZE}, Delay: {BATCH_DELAY_SECONDS}s")
        print(f"   Target fraud rate: 1-2%")
        
        try:
            while True:
                batch_fraud_count = 0
                
                # Генерируем и отправляем батч транзакций
                for _ in range(BATCH_SIZE):
                    # Генерируем транзакцию
                    transaction = self._generate_transaction()
                    
                    # Валидация через JSON Schema
                    if not self._validate_transaction(transaction):
                        print(f"⚠️  Skipping invalid transaction: {transaction['transaction_id']}")
                        continue
                    
                    # Подсчет мошенничества
                    if transaction["is_fraud"] == 1:
                        batch_fraud_count += 1
                        self.total_fraud += 1
                    
                    # Сериализация в JSON
                    message = json.dumps(transaction)
                    
                    # Отправка в Kafka
                    self.producer.produce(
                        KAFKA_TOPIC,
                        key=transaction["user_id"].encode('utf-8'),
                        value=message.encode('utf-8'),
                        callback=self._delivery_callback
                    )
                    
                    self.total_sent += 1
                
                # Flush для гарантии отправки
                self.producer.flush()
                
                # Статистика
                fraud_rate = (self.total_fraud / self.total_sent * 100) if self.total_sent > 0 else 0
                print(f"📊 Batch sent: {BATCH_SIZE} txns | "
                      f"Fraud in batch: {batch_fraud_count} | "
                      f"Total: {self.total_sent} txns | "
                      f"Fraud rate: {fraud_rate:.2f}%")
                
                # Задержка перед следующим батчом
                time.sleep(BATCH_DELAY_SECONDS)
                
        except KeyboardInterrupt:
            print("\n⏸️  Stopping producer...")
        except Exception as e:
            print(f"❌ Error in production loop: {e}")
        finally:
            self.producer.flush()
            print(f"\n✅ Producer stopped. Total sent: {self.total_sent}, Fraud: {self.total_fraud}")


if __name__ == "__main__":
    producer = TransactionProducer()
    producer.run_continuous_production()
