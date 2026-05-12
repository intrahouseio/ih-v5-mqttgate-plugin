---Топики состояний---
Состояние соединения: clientId + "/status/connection"
Состояние разрешения на обработку команд устройств: clientId + "/status/command"
Состояние разрешения на обработку команд БД: clientId + "/status/db"
Сообщения об ошибках: clientId + "/errors"


---Команды---
топик для отправки команд устройствам: clientId + "/device_command"
пример команды: {"device": "FAN_001", "prop": "toggle"}
пример записи свойства: {"device": "FAN_001", "prop": "state", "value": 1}

допустимо указывать did устройства, определение происходит автоматически:
{"device": "d0107", "prop": "state", "value": 0}


топик для отправки запросов БД: clientId + "/db_request"
пример команды:  {"device": "SpS_097", "prop": "speed", "start": 1767225600000, "end": 1777885228000}

топик для ответов от БД: clientId + "/db_response"
пример ответа: [{"id":11761,"ts":1777368565985,"dn":"SpS_097","prop":"speed","val":2.2},{"id":11762,"ts":1777368566512,"dn":"SpS_097","prop":"speed","val":1.7},{"id":11763,"ts":1777368566929,"dn":"SpS_097","prop":"speed","val":1.2},{"id":11764,"ts":1777368567363,"dn":"SpS_097","prop":"speed","val":0.8},{"id":11765,"ts":1777368567905,"dn":"SpS_097","prop":"speed","val":0.3}]


---Данные---
Топик локаций: clientId + "/locations" + "/RootTopic" (задается в расширениях))
Топик устройств clientId + "/devices/"
Топик устройств по тегам: clientId + "/tags/"
