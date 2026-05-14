# MQTT API

## Описание

MQTTGATE предназначен для взаимодействия с устройствами и БД IntraSCADA через MQTT брокер.

Все топики формируются относительно `clientId`.

---

# Структура топиков

# Общая структура

```text
clientId/
├── status/
│   ├── connection
│   ├── command
│   └── db
├── errors
├── device_command
├── db_request
├── db_response
├── locations/
├── devices/
└── tags/
```

## Топики состояний

### Состояние соединения

```text
clientId + "/status/connection"
```

### Состояние разрешения на обработку команд устройств

```text
clientId + "/status/command"
```

### Состояние разрешения на обработку команд БД

```text
clientId + "/status/db"
```

### Сообщения об ошибках

```text
clientId + "/errors"
```

---

# Команды устройствам

## Топик отправки команд

```text
clientId + "/device_command"
```

## Примеры команд

### Выполнение действия

```json
{
  "device": "FAN_001",
  "prop": "toggle"
}
```

### Запись значения свойства

```json
{
  "device": "FAN_001",
  "prop": "state",
  "value": 1
}
```

### Использование DID устройства

Допускается указывать `did` устройства — определение устройства происходит автоматически.

```json
{
  "device": "d0107",
  "prop": "state",
  "value": 0
}
```

---

# Работа с БД

## Топик отправки запросов

```text
clientId + "/db_request"
```

## Пример запроса

```json
{
  "device": "SpS_097",
  "prop": "speed",
  "start": 1767225600000,
  "end": 1777885228000
}
```

### Параметры

| Поле | Описание |
|---|---|
| `device` | Имя устройства |
| `prop` | Имя свойства |
| `start` | Начало диапазона времени (timestamp ms) |
| `end` | Конец диапазона времени (timestamp ms) |

---

## Топик ответов БД

```text
clientId + "/db_response"
```

## Пример ответа

```json
[
  {
    "id": 11761,
    "ts": 1777368565985,
    "dn": "SpS_097",
    "prop": "speed",
    "val": 2.2
  },
  {
    "id": 11762,
    "ts": 1777368566512,
    "dn": "SpS_097",
    "prop": "speed",
    "val": 1.7
  },
  {
    "id": 11763,
    "ts": 1777368566929,
    "dn": "SpS_097",
    "prop": "speed",
    "val": 1.2
  }
]
```

### Поля ответа

| Поле | Описание |
|---|---|
| `id` | Идентификатор записи |
| `ts` | Timestamp записи |
| `dn` | Имя устройства |
| `prop` | Имя свойства |
| `val` | Значение |

---

# Данные

## Топик локаций

```text
clientId + "/locations/" + RootTopic
```

> `RootTopic` задается в расширениях.

---

## Топик устройств

```text
clientId + "/devices/"
```

---

## Топик устройств по тегам

```text
clientId + "/tags/"
```

---


---

