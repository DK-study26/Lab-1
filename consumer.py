"""
Kafka Consumer — Агентство по продаже авиабилетов
Получает сообщения из Kafka, валидирует их и выводит на консоль.
"""

import json
import logging
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError

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
GROUP_ID = "airline-agency-consumer-group"

# Допустимые значения статусов
VALID_BOOKING_STATUSES = {"confirmed", "pending", "cancelled"}
VALID_TICKET_STATUSES  = {"issued", "refunded", "used"}
VALID_SEAT_CLASSES     = {"economy", "business", "first"}
VALID_PAYMENT_STATUSES = {"paid", "pending", "refunded"}
VALID_EVENT_TYPES      = {"booking_created", "booking_updated", "booking_cancelled"}

# Допустимые IATA-коды аэропортов (расширяемый список)
KNOWN_AIRPORTS = {
    "SVO", "DME", "VKO", "LED", "SVX", "AER",
    "IST", "FRA", "DXB", "NRT", "JFK", "LHR", "CDG",
}


# ---------------------------------------------------------------------------
# Валидация
# ---------------------------------------------------------------------------

class ValidationError(Exception):
    """Исключение для ошибок валидации сообщения."""
    pass


def _require(condition: bool, message: str):
    if not condition:
        raise ValidationError(message)


def validate_airline(airline: dict):
    _require(isinstance(airline, dict), "airline должен быть объектом")
    _require(bool(airline.get("iata_code")), "airline.iata_code обязателен")
    _require(len(airline["iata_code"]) == 2, "airline.iata_code должен быть 2 символа")
    _require(bool(airline.get("name")), "airline.name обязателен")


def validate_airport(airport: dict, field_name: str):
    _require(isinstance(airport, dict), f"{field_name} должен быть объектом")
    iata = airport.get("iata_code", "")
    _require(len(iata) == 3, f"{field_name}.iata_code должен быть 3 символа, получено: '{iata}'")


def validate_flight(flight: dict):
    _require(isinstance(flight, dict), "flight должен быть объектом")
    _require(bool(flight.get("flight_id")),     "flight.flight_id обязателен")
    _require(bool(flight.get("flight_number")), "flight.flight_number обязателен")

    # Проверяем формат даты вылета
    dep = flight.get("scheduled_departure", "")
    try:
        datetime.strptime(dep, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        raise ValidationError(f"Некорректный формат scheduled_departure: '{dep}'")

    # Кол-во мест ≥ 0
    total = flight.get("total_seats", -1)
    _require(isinstance(total, int) and total >= 0,
             f"flight.total_seats должен быть неотрицательным целым числом, получено: {total}")


def validate_passenger(passenger: dict):
    _require(isinstance(passenger, dict), "passenger должен быть объектом")
    _require(bool(passenger.get("passenger_id")), "passenger.passenger_id обязателен")
    _require(bool(passenger.get("last_name")),    "passenger.last_name обязателен")
    _require(bool(passenger.get("first_name")),   "passenger.first_name обязателен")

    # Проверка формата паспорта (РФ: "XX XXXXXX")
    passport = passenger.get("passport_number", "")
    parts = passport.split()
    _require(
        len(parts) == 2 and len(parts[0]) == 2 and len(parts[1]) == 6,
        f"Некорректный формат паспорта: '{passport}' (ожидается 'XX XXXXXX')",
    )

    # E-mail должен содержать @
    email = passenger.get("email", "")
    _require("@" in email, f"Некорректный email пассажира: '{email}'")


def validate_ticket(ticket: dict):
    _require(isinstance(ticket, dict), "ticket должен быть объектом")
    _require(bool(ticket.get("ticket_id")),     "ticket.ticket_id обязателен")
    _require(bool(ticket.get("ticket_number")), "ticket.ticket_number обязателен")

    seat_class = ticket.get("seat_class", "")
    _require(seat_class in VALID_SEAT_CLASSES,
             f"Недопустимый seat_class: '{seat_class}' (допустимо: {VALID_SEAT_CLASSES})")

    price = ticket.get("price", -1)
    _require(isinstance(price, (int, float)) and price > 0,
             f"Цена билета должна быть положительной, получено: {price}")

    status = ticket.get("status", "")
    _require(status in VALID_TICKET_STATUSES,
             f"Недопустимый статус билета: '{status}' (допустимо: {VALID_TICKET_STATUSES})")


def validate_booking(booking: dict):
    _require(isinstance(booking, dict), "booking должен быть объектом")
    _require(bool(booking.get("booking_id")),        "booking.booking_id обязателен")
    _require(bool(booking.get("booking_reference")), "booking.booking_reference обязателен")

    status = booking.get("status", "")
    _require(status in VALID_BOOKING_STATUSES,
             f"Недопустимый статус бронирования: '{status}' (допустимо: {VALID_BOOKING_STATUSES})")

    pay_status = booking.get("payment_status", "")
    _require(pay_status in VALID_PAYMENT_STATUSES,
             f"Недопустимый статус оплаты: '{pay_status}' (допустимо: {VALID_PAYMENT_STATUSES})")

    total = booking.get("total_price", -1)
    _require(isinstance(total, (int, float)) and total > 0,
             f"total_price должен быть положительным числом, получено: {total}")


def validate_message(message: dict) -> list[str]:
    """
    Валидирует всё сообщение целиком.
    Возвращает список ошибок (пустой список = сообщение валидно).
    """
    errors = []

    # Проверка верхнего уровня
    for field in ("event_type", "event_id", "timestamp", "data"):
        if not message.get(field):
            errors.append(f"Отсутствует обязательное поле верхнего уровня: '{field}'")

    event_type = message.get("event_type", "")
    if event_type not in VALID_EVENT_TYPES:
        errors.append(f"Неизвестный event_type: '{event_type}'")

    data = message.get("data")
    if not isinstance(data, dict):
        errors.append("Поле 'data' должно быть объектом")
        return errors   # дальнейшая валидация невозможна

    validators = [
        ("airline",           lambda: validate_airline(data.get("airline", {}))),
        ("departure_airport", lambda: validate_airport(data.get("departure_airport", {}), "departure_airport")),
        ("arrival_airport",   lambda: validate_airport(data.get("arrival_airport", {}), "arrival_airport")),
        ("flight",            lambda: validate_flight(data.get("flight", {}))),
        ("passenger",         lambda: validate_passenger(data.get("passenger", {}))),
        ("booking",           lambda: validate_booking(data.get("booking", {}))),
        ("ticket",            lambda: validate_ticket(data.get("booking", {}).get("ticket", {}))),
    ]

    for name, fn in validators:
        try:
            fn()
        except ValidationError as e:
            errors.append(f"[{name}] {e}")

    # Аэропорты отправления и прибытия не должны совпадать
    dep_code = data.get("departure_airport", {}).get("iata_code", "")
    arr_code = data.get("arrival_airport",   {}).get("iata_code", "")
    if dep_code and arr_code and dep_code == arr_code:
        errors.append(f"Аэропорт вылета и прилёта совпадают: '{dep_code}'")

    return errors


# ---------------------------------------------------------------------------
# Consumer
# ---------------------------------------------------------------------------

def create_consumer(broker: str, topic: str, group_id: str) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )


def process_message(raw_message):
    msg = raw_message.value
    partition = raw_message.partition
    offset    = raw_message.offset

    logger.info("📥 Получено сообщение | partition=%d | offset=%d", partition, offset)
    print("\n" + "=" * 70)
    print("ПОЛУЧЕННОЕ СООБЩЕНИЕ:")
    print(json.dumps(msg, ensure_ascii=False, indent=2))
    print("-" * 70)

    errors = validate_message(msg)

    if errors:
        print("❌ NOT VALID")
        print("Ошибки валидации:")
        for err in errors:
            print(f"  • {err}")
    else:
        booking_ref = msg["data"]["booking"]["booking_reference"]
        flight_num  = msg["data"]["flight"]["flight_number"]
        dep = msg["data"]["departure_airport"]["iata_code"]
        arr = msg["data"]["arrival_airport"]["iata_code"]
        pax = (msg["data"]["passenger"]["last_name"] + " "
               + msg["data"]["passenger"]["first_name"])
        print(f"✅ VALID")
        print(f"   Бронирование : {booking_ref}")
        print(f"   Рейс         : {flight_num}  {dep} → {arr}")
        print(f"   Пассажир     : {pax}")
        print(f"   Сумма        : {msg['data']['booking']['total_price']} {msg['data']['booking']['currency']}")

    print("=" * 70 + "\n")


def main():
    logger.info("🚀 Запуск Consumer | broker=%s | topic=%s | group=%s",
                KAFKA_BROKER, KAFKA_TOPIC, GROUP_ID)

    try:
        consumer = create_consumer(KAFKA_BROKER, KAFKA_TOPIC, GROUP_ID)
    except KafkaError as e:
        logger.error("Не удалось подключиться к Kafka: %s", e)
        return

    logger.info("⏳ Ожидание сообщений... (Ctrl+C для остановки)")
    try:
        for raw_message in consumer:
            process_message(raw_message)
    except KeyboardInterrupt:
        logger.info("⛔ Остановлено пользователем.")
    finally:
        consumer.close()
        logger.info("Consumer закрыт.")


if __name__ == "__main__":
    main()
