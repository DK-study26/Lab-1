# Kafka Lab — Агентство по продаже авиабилетов

Лабораторная работа по потоковой обработке данных в реальном времени с использованием Apache Kafka.

## Описание

Реализован полный цикл работы с сообщениями в Kafka по теме агентство по продаже авиабилетов. Producer генерирует бронирования (авиакомпания, рейс, аэропорты, пассажир, билет, статус, стоимость) и отправляет их в топик. Consumer читает сообщения из топика, валидирует их и выводит результат в консоль.

Первое сообщение намеренно генерируется с невалидными данными для демонстрации работы валидации.

## Стек технологий

**Apache Kafka 4.0.0** — брокер сообщений
**Python 3** — язык разработки скриптов
**kafka-python** — клиентская библиотека для работы с Kafka из Python

## Структура проекта

```
kafka-airline/
├── message_generator.py  # Генерация случайных сообщений (вне Producer — принцип SOLID)
├── producer.py           # Отправка сообщений в Kafka-топик
├── consumer.py           # Чтение и валидация сообщений из Kafka-топика
└── requirements.txt      # Зависимости Python
```

## Формат сообщения

```json
{
  "event_type": "booking_created",
  "event_id": "f3a2c1d0-8b4e-4f1a-9c7e-2b5d3e6f8a0b",
  "timestamp": "2026-03-19T10:30:10",
  "source": "airline_agency_system",
  "data": {
    "airline": {
      "iata_code": "SU",
      "name": "Аэрофлот",
      "country": "Россия",
      "hub": "SVO"
    },
    "departure_airport": {
      "iata_code": "SVO",
      "name": "Шереметьево",
      "city": "Москва",
      "country": "Россия"
    },
    "arrival_airport": {
      "iata_code": "LED",
      "name": "Пулково",
      "city": "Санкт-Петербург",
      "country": "Россия"
    },
    "flight": {
      "flight_number": "SU1234",
      "scheduled_departure": "2026-06-20T09:00:00",
      "scheduled_arrival": "2026-06-20T10:20:00",
      "aircraft_type": "Airbus A320",
      "total_seats": 150,
      "status": "scheduled"
    },
    "passenger": {
      "first_name": "Александр",
      "last_name": "Иванов",
      "passport_number": "45 123456",
      "email": "ivanov123@mail.ru",
      "phone": "+79161234567"
    },
    "booking": {
      "booking_reference": "BK482910",
      "status": "confirmed",
      "ticket": {
        "ticket_number": "TKT-847392810",
        "seat_number": "14A",
        "seat_class": "economy",
        "price": 5800.00
      },
      "total_price": 5800.00,
      "currency": "RUB",
      "payment_method": "card",
      "payment_status": "paid"
    }
  }
}
```

## Правила валидации (Consumer)

| Поле | Условие невалидности |
|-|-|
| `event_type` | не входит в список: `booking_created`, `booking_updated`, `booking_cancelled` |
| `airline.iata_code` | длина не равна 2 символам |
| `airport.iata_code` | длина не равна 3 символам |
| `flight.scheduled_departure` | не соответствует формату `YYYY-MM-DDTHH:MM:SS` |
| `flight.total_seats` | отрицательное число или не целое |
| `passenger.passport_number` | не соответствует формату `XX XXXXXX` |
| `passenger.email` | не содержит символ `@` |
| `ticket.seat_class` | не входит в список: `economy`, `business`, `first` |
| `ticket.price` | отрицательное число или ноль |
| `booking.status` | не входит в список: `confirmed`, `pending`, `cancelled` |
| `booking.payment_status` | не входит в список: `paid`, `pending`, `refunded` |
| аэропорты | аэропорт вылета совпадает с аэропортом прилёта |
| обязательные поля | отсутствует любое из полей верхнего уровня |

## Запуск

### Требования

* Java 17+
* Python 3.10+
* Apache Kafka 4.2.0

### 1. Установить зависимости Python

```
pip install -r requirements.txt
```

### 2. Инициализировать Kafka (только один раз)

```
cd C:\path\to\kafka

bin\windows\kafka-storage.bat random-uuid
bin\windows\kafka-storage.bat format --standalone -t <UUID> -c config\server.properties
```

### 3. Запустить Kafka (окно 1)

```
bin\windows\kafka-server-start.bat config\server.properties
```

### 4. Создать топик (один раз, после запуска брокера)

```
bin\windows\kafka-topics.bat --create --topic airline-bookings --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 5. Запустить Consumer (окно 2)

```
python consumer.py
```

### 6. Запустить Producer (окно 3)

```
python producer.py
```

## Пример вывода

**Producer:**

```
[PRODUCER] Отправка НЕВАЛИДНОГО сообщения:
{"event_type": "unknown", "data": {"airline": {"iata_code": "X"}, "passenger": {"email": "not-email"}, ...}}
[PRODUCER] Сообщение отправлено | Партиция: 0 | Смещение: 0

[PRODUCER] Сгенерировано сообщение:
{"event_type": "booking_created", "data": {"airline": {"iata_code": "SU", "name": "Аэрофлот"}, ...}}
[PRODUCER] Сообщение отправлено | Партиция: 0 | Смещение: 1
```

**Consumer:**

```
[CONSUMER] Получено сообщение:
{"event_type": "unknown", "data": {"airline": {"iata_code": "X"}, "passenger": {"email": "not-email"}, ...}}
[CONSUMER] NOT VALID
           - Неизвестный event_type: 'unknown'
           - [airline] iata_code должен быть 2 символа, получено: 'X'
           - [passenger] Некорректный email: 'not-email'

[CONSUMER] Получено сообщение:
{"event_type": "booking_created", "data": {"airline": {"iata_code": "SU"}, ...}}
[CONSUMER] VALID  |  Бронирование: BK482910 | Рейс: SU1234 SVO→LED | Пассажир: Иванов Александр
```

## Скриншот

[Демонстрация работы Producer и Consumer](screen1.jpg)
