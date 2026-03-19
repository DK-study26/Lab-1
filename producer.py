"""
Kafka Producer — Агентство по продаже авиабилетов
Генерирует сообщения о бронировании авиабилетов и отправляет их в Kafka.
"""

import json
import time
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from message_generator import generate_kafka_message

# --- Настройка логирования ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# --- Конфигурация ---
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "airline-bookings"
SEND_INTERVAL_SECONDS = 3   # интервал между сообщениями
TOTAL_MESSAGES = 10         # количество сообщений (0 = бесконечно)


def create_producer(broker: str) -> KafkaProducer:
    """Создаёт и возвращает экземпляр KafkaProducer."""
    return KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",                  # подтверждение от всех реплик
        retries=3,
        max_block_ms=5000,
    )


def on_send_success(record_metadata):
    logger.info(
        "✅ Сообщение доставлено | topic=%s | partition=%d | offset=%d",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
    )


def on_send_error(exc):
    logger.error("❌ Ошибка отправки: %s", exc)


def send_message(producer: KafkaProducer, topic: str, message: dict):
    """Отправляет одно сообщение в Kafka и выводит его на консоль."""
    booking_ref = message["data"]["booking"]["booking_reference"]
    flight_num  = message["data"]["flight"]["flight_number"]
    pax_name    = (
        message["data"]["passenger"]["last_name"]
        + " "
        + message["data"]["passenger"]["first_name"]
    )
    dep = message["data"]["departure_airport"]["iata_code"]
    arr = message["data"]["arrival_airport"]["iata_code"]

    logger.info(
        "📤 Отправка | ref=%s | рейс=%s | %s→%s | пассажир=%s",
        booking_ref, flight_num, dep, arr, pax_name,
    )
    print("\n" + "=" * 70)
    print("СГЕНЕРИРОВАННОЕ СООБЩЕНИЕ:")
    print(json.dumps(message, ensure_ascii=False, indent=2))
    print("=" * 70 + "\n")

    future = producer.send(topic, value=message)
    future.add_callback(on_send_success).add_errback(on_send_error)


def main():
    logger.info("🚀 Запуск Producer | broker=%s | topic=%s", KAFKA_BROKER, KAFKA_TOPIC)

    try:
        producer = create_producer(KAFKA_BROKER)
    except KafkaError as e:
        logger.error("Не удалось подключиться к Kafka: %s", e)
        return

    count = 0
    try:
        while True:
            message = generate_kafka_message()   # генерация ВЫНЕСЕНА в отдельный модуль
            send_message(producer, KAFKA_TOPIC, message)

            count += 1
            if TOTAL_MESSAGES and count >= TOTAL_MESSAGES:
                logger.info("Достигнут лимит сообщений (%d). Завершение.", TOTAL_MESSAGES)
                break

            time.sleep(SEND_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("⛔ Остановлено пользователем.")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer закрыт. Всего отправлено: %d сообщений.", count)


if __name__ == "__main__":
    main()
