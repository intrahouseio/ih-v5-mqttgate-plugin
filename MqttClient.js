const mqtt = require("mqtt");
const EventEmitter = require("events");

class MqttClient extends EventEmitter {
    constructor(brokerUrl, options = {}) {
        super();
        this.brokerUrl = brokerUrl;
        this.options = options;
        this.client = null;
    }

    async connect() {
        this.emit("tryconnect");
        this.client = mqtt.connect(this.brokerUrl, this.options);

        this.client.on("connect", () => {
            this.emit("connect");
        });

        this.client.on("message", (topic, message, packet) => {
            this.emit("data", {
                topic: topic.toString(),
                message: message.toString(),
                packet: packet
            });
        });

        this.client.on("close", () => {
            this.emit("disconnect");
        });

        this.client.on("error", (err) => {
            this.emit("error", err);
        });
    }

    send(topic, message, qos = 0, retain = false) {
        const normMsg = this.checkData(message);
        if (this.client && this.client.connected && normMsg !== false) {
            this.client.publish(topic, normMsg, { qos, retain });
        } else {
            this.emit("error", "Filed send message!");
        }
    }

    subscribe(topic) {
        if (this.client && this.client.connected) {
            this.client.subscribe(topic, (err) => {
                if (err) this.emit("error", err);
            });
        } else {
            this.emit("error", "Filed subscribe!");
        }
    }

    unsubscribe(topic) {
        if (this.client && this.client.connected) {
            this.client.unsubscribe(topic, (err) => {
                if (err) this.emit("error", err);
            });
        }
    }

    disconnect() {
        if (this.client) {
            this.client.end();
            this.client = null;
            this.emit("disconnect");
        }
    }

    debug(message) {
        this.emit("debug", message);
    }

    checkData(data) {
        try {
            if (typeof (data) === 'object') {
                return JSON.stringify(data);
            }
            else {
                return data.toString();
            }
        } catch (error) {
            return false;
        }
    }
}

module.exports = MqttClient;
