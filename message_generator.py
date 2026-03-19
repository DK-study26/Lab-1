import random
import uuid
import json
from datetime import datetime, timedelta


# --- Справочные данные ---

AIRLINES = [
    {"iata_code": "SU", "name": "Аэрофлот", "country": "Россия", "hub": "SVO"},
    {"iata_code": "S7", "name": "S7 Airlines", "country": "Россия", "hub": "OB"},
    {"iata_code": "U6", "name": "Уральские авиалинии", "country": "Россия", "hub": "SVX"},
    {"iata_code": "DP", "name": "Победа", "country": "Россия", "hub": "VKO"},
    {"iata_code": "TK", "name": "Turkish Airlines", "country": "Турция", "hub": "IST"},
    {"iata_code": "LH", "name": "Lufthansa", "country": "Германия", "hub": "FRA"},
    {"iata_code": "EK", "name": "Emirates", "country": "ОАЭ", "hub": "DXB"},
]

AIRPORTS = [
    {"iata_code": "SVO", "name": "Шереметьево", "city": "Москва", "country": "Россия", "timezone": "Europe/Moscow"},
    {"iata_code": "DME", "name": "Домодедово", "city": "Москва", "country": "Россия", "timezone": "Europe/Moscow"},
    {"iata_code": "VKO", "name": "Внуково", "city": "Москва", "country": "Россия", "timezone": "Europe/Moscow"},
    {"iata_code": "LED", "name": "Пулково", "city": "Санкт-Петербург", "country": "Россия", "timezone": "Europe/Moscow"},
    {"iata_code": "SVX", "name": "Кольцово", "city": "Екатеринбург", "country": "Россия", "timezone": "Asia/Yekaterinburg"},
    {"iata_code": "AER", "name": "Сочи", "city": "Сочи", "country": "Россия", "timezone": "Europe/Moscow"},
    {"iata_code": "IST", "name": "Аэропорт Стамбул", "city": "Стамбул", "country": "Турция", "timezone": "Europe/Istanbul"},
    {"iata_code": "FRA", "name": "Франкфурт", "city": "Франкфурт", "country": "Германия", "timezone": "Europe/Berlin"},
    {"iata_code": "DXB", "name": "Дубай Интернэшнл", "city": "Дубай", "country": "ОАЭ", "timezone": "Asia/Dubai"},
    {"iata_code": "NRT", "name": "Нарита", "city": "Токио", "country": "Япония", "timezone": "Asia/Tokyo"},
]

FIRST_NAMES = ["Александр", "Мария", "Иван", "Анна", "Дмитрий",
               "Елена", "Сергей", "Ольга", "Андрей", "Наталья",
               "Михаил", "Татьяна", "Алексей", "Юлия", "Владимир"]

LAST_NAMES = ["Иванов", "Петров", "Сидоров", "Козлов", "Смирнов",
              "Попов", "Новиков", "Морозов", "Волков", "Соколов"]

SEAT_CLASSES = ["economy", "business", "first"]
BOOKING_STATUSES = ["confirmed", "pending", "cancelled"]
TICKET_STATUSES = ["issued", "refunded", "used"]
PAYMENT_METHODS = ["card", "cash", "online_wallet"]


def _random_date(days_ahead_min=1, days_ahead_max=180):
    delta = random.randint(days_ahead_min, days_ahead_max)
    return (datetime.now() + timedelta(days=delta)).strftime("%Y-%m-%dT%H:%M:%S")


def _random_flight_number(airline_iata):
    return f"{airline_iata}{random.randint(100, 9999)}"


def _random_price(seat_class):
    base = {"economy": (3000, 25000), "business": (20000, 120000), "first": (80000, 350000)}
    lo, hi = base[seat_class]
    return round(random.uniform(lo, hi), 2)


def _random_seat(seat_class):
    row = {"economy": range(10, 45), "business": range(1, 10), "first": range(1, 4)}[seat_class]
    col = random.choice(["A", "B", "C", "D", "E", "F"])
    return f"{random.choice(list(row))}{col}"


def _random_passport():
    return f"{random.randint(10, 99)} {random.randint(100000, 999999)}"


# --- Генераторы сущностей ---

def generate_airline():
    return random.choice(AIRLINES)


def generate_airport():
    return random.choice(AIRPORTS)


def generate_passenger():
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    gender = random.choice(["male", "female"])
    birth_year = random.randint(1955, 2005)
    return {
        "passenger_id": str(uuid.uuid4()),
        "first_name": first_name,
        "last_name": last_name,
        "gender": gender,
        "date_of_birth": f"{birth_year}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
        "passport_number": _random_passport(),
        "nationality": random.choice(["RU", "US", "DE", "CN", "TR", "AE"]),
        "email": f"{last_name.lower()}{random.randint(1, 999)}@mail.ru",
        "phone": f"+7{random.randint(9000000000, 9999999999)}",
        "frequent_flyer_number": f"FF{random.randint(1000000, 9999999)}" if random.random() > 0.4 else None,
    }


def generate_flight(airline=None, departure_airport=None, arrival_airport=None):
    if airline is None:
        airline = random.choice(AIRLINES)
    if departure_airport is None:
        departure_airport = random.choice(AIRPORTS)
    # Ensure arrival != departure
    if arrival_airport is None:
        arrival_choices = [a for a in AIRPORTS if a["iata_code"] != departure_airport["iata_code"]]
        arrival_airport = random.choice(arrival_choices)

    departure_dt = datetime.now() + timedelta(
        days=random.randint(1, 90),
        hours=random.randint(0, 23),
        minutes=random.choice([0, 10, 20, 30, 40, 50])
    )
    flight_duration_min = random.randint(60, 720)
    arrival_dt = departure_dt + timedelta(minutes=flight_duration_min)

    return {
        "flight_id": str(uuid.uuid4()),
        "flight_number": _random_flight_number(airline["iata_code"]),
        "airline": airline,
        "departure_airport": departure_airport,
        "arrival_airport": arrival_airport,
        "scheduled_departure": departure_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "scheduled_arrival": arrival_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "flight_duration_minutes": flight_duration_min,
        "aircraft_type": random.choice(["Boeing 737", "Airbus A320", "Boeing 777", "Airbus A380", "Sukhoi Superjet 100"]),
        "total_seats": random.choice([120, 150, 180, 220, 350]),
        "available_seats": random.randint(0, 150),
        "status": random.choice(["scheduled", "boarding", "departed", "arrived", "delayed", "cancelled"]),
        "terminal": random.choice(["A", "B", "C", "D", None]),
        "gate": f"{random.choice(['A','B','C','D'])}{random.randint(1,30)}" if random.random() > 0.2 else None,
    }


def generate_ticket(flight=None, passenger=None):
    if flight is None:
        flight = generate_flight()
    if passenger is None:
        passenger = generate_passenger()

    seat_class = random.choice(SEAT_CLASSES)
    price = _random_price(seat_class)

    return {
        "ticket_id": str(uuid.uuid4()),
        "ticket_number": f"TKT-{random.randint(100000000, 999999999)}",
        "flight_id": flight["flight_id"],
        "passenger_id": passenger["passenger_id"],
        "seat_number": _random_seat(seat_class),
        "seat_class": seat_class,
        "price": price,
        "currency": "RUB",
        "baggage_allowance_kg": {"economy": 20, "business": 30, "first": 40}[seat_class],
        "hand_luggage_kg": 10,
        "meal_preference": random.choice(["standard", "vegetarian", "halal", "kosher", None]),
        "status": random.choice(TICKET_STATUSES),
        "issued_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }


def generate_booking(flight=None, passenger=None):
    if flight is None:
        flight = generate_flight()
    if passenger is None:
        passenger = generate_passenger()

    ticket = generate_ticket(flight=flight, passenger=passenger)
    total = ticket["price"]

    return {
        "booking_id": str(uuid.uuid4()),
        "booking_reference": f"BK{random.randint(100000, 999999)}",
        "created_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "status": random.choice(BOOKING_STATUSES),
        "passenger": passenger,
        "flight": flight,
        "ticket": ticket,
        "total_price": total,
        "currency": "RUB",
        "payment_method": random.choice(PAYMENT_METHODS),
        "payment_status": random.choice(["paid", "pending", "refunded"]),
        "agency_code": f"AGT{random.randint(1000, 9999)}",
        "notes": random.choice([None, "Запрос на место у окна", "Инвалидное кресло", "Требуется детское меню"]),
    }


def generate_kafka_message():
    """
    Основная функция генерации сообщения для Kafka.
    Вынесена за пределы Producer в соответствии с принципом
    единственной ответственности (SRP из SOLID).
    """
    # Связываем сущности: один рейс, один пассажир, одно бронирование
    airline = generate_airline()
    dep_airport = generate_airport()
    arr_choices = [a for a in AIRPORTS if a["iata_code"] != dep_airport["iata_code"]]
    arr_airport = random.choice(arr_choices)

    flight = generate_flight(airline=airline,
                             departure_airport=dep_airport,
                             arrival_airport=arr_airport)
    passenger = generate_passenger()
    booking = generate_booking(flight=flight, passenger=passenger)

    message = {
        "event_type": "booking_created",
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "source": "airline_agency_system",
        "data": {
            "airline": airline,
            "departure_airport": dep_airport,
            "arrival_airport": arr_airport,
            "flight": flight,
            "passenger": passenger,
            "booking": booking,
        }
    }
    return message
