# MQTTGATE (master)

## Описание
MQTTGATE публикует устройства, теги, локации и историю БД IntraSCADA в MQTT-брокер и принимает обратно команды и запросы к БД. Все топики формируются относительно `clientId`.

Статические топики (`status/*`, `meta/*`) публикуются с `retain: true`, поэтому подписчик получает актуальное состояние сразу при подписке, не дожидаясь очередного heartbeat. Значения устройств публикуются без retain.

---

# Структура топиков

```text
clientId/
├── status/
│   ├── connection       {connection, heartbeat}        retained  (+ LWT)
│   ├── paramsInfo       параметры шлюза                 retained
│   ├── extraInfo        конфигурация фильтров           retained
│   └── foldersInfo      карта папок (id → folder)       retained
├── errors               ошибки обработки
├── device_command       ← подписка (входящие команды)
├── db_request           ← подписка (входящие запросы к БД)
├── db_response          ответы БД
├── locations/…/{prop}   {value, ts, quality}
├── devices/…/{prop}     {value, ts, quality}
├── tags/…/{prop}        {value, ts, quality}
└── meta/
    └── {dn | did}       мета-информация об устройстве   retained
```

## Направление потоков
| Топик | Кто публикует | Кто читает |
|---|---|---|
| `status/*`, `errors`, `meta/*`, `locations/*`, `devices/*`, `tags/*`, `db_response` | мастер | клиенты / внешние потребители |
| `device_command`, `db_request` | внешний клиент | мастер (подписан) |
| `$SYS/+/new/clients` | брокер | мастер (подписан) |

Мастер дополнительно подписывается на `$SYS/+/new/clients`: при подключении нового клиента (если включён `republishOnNewclient`) выполняется переотправка последних значений.

---

# Топики состояний

### Состояние соединения
```text
clientId + "/status/connection"
```
Публикуется периодически (каждые `heartbeatInt` секунд) с `retain: true`. При аварийном обрыве брокер публикует Last Will с `connection: false`.
```json
{ "connection": true, "heartbeat": 1777368565985 }
```

### Параметры шлюза
```text
clientId + "/status/paramsInfo"
```
Санитайзнутый набор параметров (секреты подключения — логин/пароль/адрес брокера/сертификаты — **не публикуются**). Сюда же вынесены флаги доступа `deviceControl` и `dbAccess` (ранее — отдельные топики `status/command` и `status/db`).
```json
{
  "heartbeatInt": 10,
  "republishInt": 60,
  "clientId": "ISmqttGate",
  "version": "1.0.0",
  "clearRetainOnStart": 0,
  "republishOnNewclient": 0,
  "topicsByName": false,
  "deviceControl": true,
  "dbAccess": true
}
```
### Поля
| Поле | Описание |
|---|---|
| `heartbeatInt` | Период публикации статусов, с |
| `republishInt` | Период переотправки значений, с |
| `clientId` | Идентификатор шлюза (корень всех топиков) |
| `version` | Версия шлюза (для проверки совместимости клиент/мастер) |
| `clearRetainOnStart` | Очищать retained при старте (`0/1`) |
| `republishOnNewclient` | Переотправлять значения при подключении нового клиента (`0/1`) |
| `topicsByName` | Адресация устройств: `dn` при `true`, `did` при `false` |
| `deviceControl` | Разрешена обработка команд устройствам |
| `dbAccess` | Разрешена обработка запросов к БД |

> Поле `topicsByName` определяет, чем адресуются устройства в топиках и meta. Клиент обязан читать этот флаг перед разбором остальных топиков.
> `republishInt` здесь — в секундах (как в параметрах); внутри мастера значение переводится в миллисекунды.

### Конфигурация фильтров
```text
clientId + "/status/extraInfo"
```
Массив активных фильтров выгрузки (по локациям / тегам / устройствам).
```json
[
  { "filter": "location", "locationStr": "/place/dg004/", "rootTopic": "shop1" },
  { "filter": "tag", "tagStr": "pumps" },
  { "filter": "device", "did": "d0107" }
]
```

### Карта папок
```text
clientId + "/status/foldersInfo"
```
Объект вида `{ folderId: folder }`, где `folder` содержит как минимум `id` и `title`. Используется для разворачивания идентификаторов папок в человекочитаемые имена при `topicsByName`.

### Сообщения об ошибках
```text
clientId + "/errors"
```
Публикуется при ошибке обработки входящей команды или запроса.
```json
{ "command": { "device": "FAN_001", "prop": "state" }, "error": "Device control disabled" }
```
Возможные значения `error`: `Device control disabled`, `DB access disabled`, `Invalid command format`, `Handle message error`.

---

# Команды устройствам

## Топик отправки команд
```text
clientId + "/device_command"
```
Обработка возможна только если в `paramsInfo` установлено `deviceControl: true`.

## Примеры команд
### Выполнение действия (без значения)
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
Допускается указывать `did` устройства (формат `d` + цифры) — определение устройства происходит автоматически; иначе `device` трактуется как `dn`.
```json
{
  "device": "d0107",
  "prop": "state",
  "value": 0
}
```
> Наличие поля `value` переключает команду в режим записи (`setval`); без него выполняется действие (`device`).

---

# Работа с БД

## Топик отправки запросов
```text
clientId + "/db_request"
```
Обработка возможна только если в `paramsInfo` установлено `dbAccess: true`.

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
| `device` | Имя устройства (`dn`) |
| `prop` | Имя свойства |
| `start` | Начало диапазона времени (timestamp ms) |
| `end` | Конец диапазона времени (timestamp ms) |

## Топик ответов БД
```text
clientId + "/db_response"
```
## Пример ответа
```json
[
  { "id": 11761, "ts": 1777368565985, "dn": "SpS_097", "prop": "speed", "val": 2.2 },
  { "id": 11762, "ts": 1777368566512, "dn": "SpS_097", "prop": "speed", "val": 1.7 },
  { "id": 11763, "ts": 1777368566929, "dn": "SpS_097", "prop": "speed", "val": 1.2 }
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

# Данные (значения свойств)

Значения публикуются на листовые топики `.../{prop}` под соответствующим корнем. Идентификатор устройства в пути — `dn` или `did` в зависимости от `topicsByName`. Публикуются без retain.

## Полезная нагрузка листового топика
Единый формат для всех трёх веток (`locations` / `devices` / `tags`):
```json
{ "value": 2.2, "ts": 1777368565985, "quality": 1 }
```
| Поле | Описание |
|---|---|
| `value` | Текущее значение свойства |
| `ts` | Timestamp значения (ms) |
| `quality` | Достоверность (`0` — недостоверно) |

## Локации
```text
clientId + "/locations/" + RootTopic + <путь> + "/" + {dn|did} + "/" + prop
```
> `RootTopic` задаётся в расширении фильтра (`rootTopic`) и соответствует первому сегменту после `locations/`.

## Устройства
```text
clientId + "/devices/" + {dn|did} + "/" + prop
```

## Устройства по тегам
```text
clientId + "/tags/" + <tag> + "/" + {dn|did} + "/" + prop
```

---

# Мета-информация об устройствах

## Топик
```text
clientId + "/meta/" + {dn | did}
```
Публикуется один раз при старте с `retain: true`. В качестве суффикса используется `dn` или `did` в зависимости от `topicsByName`. Из `props` удаляются рантайм-поля (`value`, `ts`, `chstatus`) — meta описывает только структуру устройства.

### Структура payload
```json
{
  "_id": "d0117",
  "name": "Датчик скорости 3",
  "dn": "SpS_111",
  "type": "t040",
  "parent": "dg055",
  "tags": "##",
  "location": "/place/dg004/dg055/",
  "locations": ["shop1/dg004/dg055/SpS_111"],
  "props": {
    "<propName>": {
      "name": "Наименование свойства",
      "op": "rw | par | cmd",
      "vtype": "N | S | B",
      "min": null,
      "max": null,
      "dig": 0,
      "mu": ""
    }
  }
}
```

### Поля устройства
| Поле | Описание |
|---|---|
| `_id` | Уникальный идентификатор устройства (`did`) |
| `name` | Человекочитаемое имя устройства |
| `dn` | Device Name — символьное имя устройства |
| `type` | Тип устройства |
| `parent` | Идентификатор родительской группы |
| `tags` | Теги устройства (разделены `#`) |
| `location` | Исходный путь расположения устройства |
| `locations` | Список опубликованных топик-путей устройства в ветке `locations/` (может быть пустым для устройств, выгруженных только по тегу/did) |
| `props` | Объект свойств устройства |

### Поля свойства (`props`)
| Поле | Описание |
|---|---|
| `name` | Человекочитаемое название свойства |
| `op` | Операция: `rw` — чтение/запись, `par` — параметр, `cmd` — команда |
| `vtype` | Тип значения: `N` — число, `S` — строка, `B` — булево |
| `min` | Минимальное значение (`null` — не ограничено) |
| `max` | Максимальное значение (`null` — не ограничено) |
| `dig` | Количество знаков после запятой |
| `mu` | Единица измерения |

> Свойства с `"op": "cmd"` содержат только поля `name` и `op` — это команды без значения; в дерево каналов клиента они не включаются.

### Пример payload
```json
{
  "_id": "d0117",
  "name": "Датчик скорости 3",
  "dn": "SpS_111",
  "type": "t040",
  "parent": "dg055",
  "tags": "##",
  "location": "/place/dg004/dg055/",
  "locations": ["shop1/dg004/dg055/SpS_111"],
  "props": {
    "speed":       { "name": "Скорость",    "op": "rw",  "vtype": "N", "min": 0,    "max": 5.5,        "dig": 5, "mu": "м/с" },
    "pulse":       { "name": "Импульсы",    "op": "rw",  "vtype": "N", "min": null, "max": 4294967295, "dig": 0, "mu": "" },
    "record":      { "name": "Запись",      "op": "rw",  "vtype": "B", "min": null, "max": null,       "dig": 0, "mu": "" },
    "startRecord": { "name": "Старт записи","op": "cmd" }
  }
}
```

---

# QoS и retain
| Группа топиков | QoS | Retain |
|---|---|---|
| `status/connection` (+ LWT) | 1 | да |
| `status/paramsInfo`, `status/extraInfo`, `status/foldersInfo` | 1 | да |
| `meta/*` | 1 | да |
| `locations/*`, `devices/*`, `tags/*` (значения) | 0 | нет |
| `errors`, `db_response` | 0 | нет |

> QoS 1 на статике страхует единичную доставку. Retained-значение брокер отдаёт новому подписчику с `min(publishQoS, subscribeQoS)`, поэтому для сквозной гарантии подписку клиента на `meta/*` также имеет смысл поднимать до QoS 1.

---

## Примечание о статусах доступа
Флаги `deviceControl` и `dbAccess` включены в `paramsInfo` и отдельными топиками (`status/command`, `status/db`) больше не публикуются. Если прошлые версии шлюза их публиковали с retain, последние значения останутся висеть на брокере — их следует один раз очистить публикацией пустого сообщения с `retain: true` (либо положиться на `clearRetainOnStart`, который проходит по `clientId/#`).
