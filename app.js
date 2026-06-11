const util = require('util');
const fs = require('fs').promises;
const path = require('path')
const MqttClient = require('./MqttClient')


module.exports = async function (plugin) {
    const extra = plugin.extra;

    const brokerIP = plugin.params.brokerIP || '127.0.0.1';
    const brokerPort = plugin.params.brokerPort || '1883';
    const protocol = plugin.params.protocol || 'mqtt';
    const clientId = plugin.params.clientId || "ISmqttGate";
    const useselfsigned = plugin.params.useselfsigned || false;

    const auth = plugin.params.auth || false;
    const topicsByName = plugin.params.topicsByName || false;
    const username = plugin.params.username || "admin";
    const password = plugin.params.password || "password";

    const heartbeatInt = plugin.params.heartbeatInt || 10;
    const republishInt = (plugin.params.republishInt || 60) * 1000;
    const republishOnNewclient = plugin.params.republishOnNewclient || 0;
    const deviceControl = plugin.params.deviceControl ? true : false;
    const dbAccess = plugin.params.dbAccess ? true : false;

    const debug = plugin.params.debug || false;

    function log(msg, level = 0, isDebugMsg = false) {
        (!isDebugMsg || debug) && plugin.log(msg, level);
    }

    (async () => {
        try {
            log("App MQTTGATE is started!", 0, false);
            process.send({ type: 'procinfo', data: { connection: 1 } });

            let client;

            const brokerUrl = `${protocol}://${brokerIP}:${brokerPort}`;
            const willpayload = { connection: false, heartbeat: Date.now() };
            const options = { clientId: clientId, rejectUnauthorized: false, };
            const connectionStateTopic = clientId + "/status/connection";
            const commandStateTopic = clientId + "/status/command";
            const dbStateTopic = clientId + "/status/db";
            const commandTopic = clientId + "/device_command";
            const dbTopic = clientId + "/db_request";
            const dbResponseTopic = clientId + "/db_response";
            const locationTopic = clientId + "/locations";
            const deviceTopic = clientId + "/devices/";
            const tagTopic = clientId + "/tags/";
            const errorTopic = clientId + "/errors";
            const metaTopic = clientId + "/meta";
            const newCliTopic = '$SYS/+/new/clients';

            const plugindir = __dirname;
            const certdir = "cert";
            const KEYFILE = "private-key.pem";
            const CERTFILE = "public-cert.pem";
            const CAFILE = "csr.pem";

            const locations = [];
            const tags = [];
            const devices = [];

            const devlinks = {};
            const didmap = {};
            const dnmap = {};
            const foldersmap = {};
            const locationsroot = {};
            const valuemap = {};
            const metamap = {};
            const updatemap = {};


            function startClient() {
                client = new MqttClient(brokerUrl, options);

                client.on("tryconnect", () => log("Try connect to: " + brokerUrl, 0, false));
                client.on("connect", () => onConnect());
                client.on("disconnect", () => onDisconnect());
                client.on("error", (err) => onError(err));
                client.on("data", (topic, message) => onMessage(topic, message));
                client.on("debug", (message) => log("MQTT client debug >> " + message, 0, true));

                client.connect();
            }

            function onConnect() {
                process.send({ type: 'procinfo', data: { connection: 2 } });
                log("✅ Connected", 0, false);
                heartbeat();
                sentMeta();
                send(errorTopic, {});
                subOnDevices();
                subscribe(commandTopic);
                subscribe(dbTopic);
                subscribe(newCliTopic);
                updatecheck();
            }

            function onDisconnect() {
                log("🔌 Disconnected", 0, false);
                process.send({ type: 'procinfo', data: { connection: 0 } });
                plugin.exit(2);
            }

            function onError(error) {
                log("❌ Error: " + error, 0, false);
                client.removeAllListeners();
                plugin.exit(1);
            }

            function onMessage(data) {
                const topic = data.topic;
                const message = data.message;
                log("MSG >> Topic: " + topic + " || Message:" + message, 2, true)
                messageHandler(topic, message);
            }

            function send(topic, message, qos = 0, retain = false) {
                try {
                    log(`SEND >> Topic: ${topic} || Message: ${util.inspect(message)} || QOS: ${qos} || Retain: ${retain}`, 2, true)
                    client.send(topic, message, qos, retain);
                } catch (error) {
                    log("Send error: " + error, 0, false);
                }
            }

            function subscribe(topic) {
                log("SUB >> Topic: " + topic, 2, true);
                client.subscribe(topic);
            }

            function unsubscribe(topic) {
                log("UNSUB >> Topic: " + topic, 2, true)
                client.unsubscribe(topic);
            }

            function heartbeat() {
                const startInd = { connection: true, heartbeat: Date.now() };
                const commandInd = { access: deviceControl };
                const dbInd = { access: dbAccess };
                send(connectionStateTopic, startInd, 1, true);
                send(commandStateTopic, commandInd);
                send(dbStateTopic, dbInd);
                setTimeout(heartbeat, Number(heartbeatInt) * 1000);
            }

            function repubCheck(isnewcli) {
                const repubArr = [];
                const ticknow = performance.now();
                Object.keys(updatemap).forEach(did => {

                    Object.keys(updatemap[did]).forEach(prop => {
                        const didprop = `${did}_${prop}`;
                        const lastupdate = updatemap[did][prop]["lastupdate"];
                        if (isnewcli) {
                            if (ticknow > lastupdate) repubArr.push(didprop);
                        }
                        else {
                            const delta = ticknow - lastupdate;
                            if (delta > republishInt) repubArr.push(didprop);
                        }
                    });

                });
                republish(repubArr)
            }

            function updatecheck() {
                setInterval(() => {
                    if (republishInt) {
                        repubCheck(false);
                    }
                }, republishInt);
            }

            function republish(didarr) {
                didarr.forEach(devprop => {
                    const [dev, prop] = devprop.split("_");
                    const value = valuemap[dev][prop];
                    if (!dev || !prop || !value) return;
                    devPropPublish(dev, prop, value);
                })
            }

            async function getOptions() {
                options["will"] = { topic: connectionStateTopic, payload: JSON.stringify(willpayload), qos: 1, retain: true };
                if (auth) {
                    options["username"] = username;
                    options["password"] = password;
                }
                if (useselfsigned) {
                    log("Read cetificates...", 0, false);
                    options["key"] = await readCertFlie(KEYFILE);
                    options["cert"] = await readCertFlie(CERTFILE);
                    options["ca"] = await readCertFlie(CAFILE);
                    options["rejectUnauthorized"] = true;
                }
            }

            async function getFolders() {
                const places = await plugin.places.get();
                for (let folder of places) {
                    foldersmap[folder.id] = folder;
                }
            }

            async function readCertFlie(file) {
                try {
                    const filePath = path.join(plugindir, certdir, file)
                    return await fs.readFile(filePath, 'utf8');
                } catch (error) {
                    log("Error read file: " + file + " - " + error, 0, false)
                    return "";
                }
            }

            async function checkExtra() {
                for (let ext of extra) {
                    if (ext.filter == "location") {
                        locations.push(ext.locationStr);
                        locationsroot[ext.locationStr] = ext.rootTopic;
                    }
                    if (ext.filter == "tag") tags.push(ext.tagStr);
                    if (ext.filter == "device") devices.push(ext.did);
                }
            }

            async function topicByName(topic) {
                let namepatharr = [];
                const patharr = topic.split("/");
                for (let p of patharr) {
                    namepatharr.push(foldersmap[p] ? foldersmap[p]["title"] : p);
                }
                return namepatharr.join("/");
            }

            async function devlinksInsert(fulldev, pref, link) {
                const id = fulldev._id;
                const dn = fulldev.dn;

                let topic = pref + link;
                let devtopic = id;

                if (topicsByName) {
                    topic = await topicByName(topic);
                    devtopic = dn;
                }

                if (!devlinks.hasOwnProperty(id)) { devlinks[id] = [] }
                devlinks[id].push(topic + devtopic);
                didmap[id] = fulldev;
                dnmap[dn] = fulldev;

                valuemap[id] = {};
                updatemap[id] = {};

                for (let prop of Object.keys(fulldev.props)) {
                    const initVlaue = fulldev.props[prop].value;
                    const initTs = fulldev.props[prop].ts;
                    const initQuality = fulldev.props[prop].quality;
                    valuemap[id][prop] = { value: initVlaue, ts: initTs, quality: initQuality };

                    const propType = fulldev.props[prop].op;
                    if (propType != "cmd") {
                        updatemap[id][prop] = { lastupdate: null };
                    }
                }

            }

            async function buildByLocations() {
                for (let location of locations) {
                    const devarr = await plugin.devices.get({ location: location })
                    for (let dev of devarr) {
                        const roottopic = locationsroot[location] || "Location_" + Math.floor(Math.random() * 10000);
                        const topicpath = dev.location.replace(location, '')
                        await devlinksInsert(dev, locationTopic, `/${roottopic}${topicpath}`);
                    }
                }
            }

            async function buildByTag() {
                for (let tag of tags) {
                    const devarr = await plugin.devices.get({ tag: tag })
                    for (let dev of devarr) {
                        await devlinksInsert(dev, tagTopic, tag + "/")
                    }
                }
            }

            async function buildByDevice() {
                const devdids = [];
                for (let device of devices) {
                    devdids.push(device);
                }
                const devarr = await plugin.devices.get({ did: devdids })
                for (let dev of devarr) {
                    await devlinksInsert(dev, deviceTopic, "")
                }
            }

            function filterDevMeta(devmeta) {
                if (!devmeta || !devmeta.props) return devmeta;

                Object.keys(devmeta.props).forEach(propkey => {
                    const propMeta = devmeta.props[propkey];
                    delete propMeta.value;
                    delete propMeta.ts;
                    delete propMeta.chstatus;
                });

                return devmeta;
            }

            function sentMeta() {
                Object.assign(metamap, topicsByName ? dnmap : didmap);
                Object.keys(metamap).forEach(devkey => {
                    const devmeta = filterDevMeta(metamap[devkey]);
                    send(`${metaTopic}/${devkey}`, devmeta, 1, true);
                });
            }

            function checkJsonValue(value) {
                try {
                    return JSON.parse(value);
                } catch (error) {
                    return value;
                }
            }

            function updateValue(dev) {
                const { did, prop, value, ts, chstatus } = dev;
                if (!valuemap[did] || !valuemap[did][prop]) return;

                const holdValue = valuemap[did][prop];
                if (value !== undefined) { holdValue.value = checkJsonValue(value) }
                if (ts !== undefined) { holdValue.ts = ts; }
                if (chstatus !== undefined) { holdValue.quality = chstatus; }

                devPropPublish(did, prop, holdValue);
            }

            function devPropPublish(did, prop, value) {
                if (!devlinks[did]) return;

                devlinks[did].forEach(topic => {
                    send(topic + "/" + prop, value);
                })

                updatemap[did][prop]["lastupdate"] = performance.now();
            }

            function subOnDevices() {
                plugin.onSub('devices', { extra: 1 }, data => {
                    //log("onSub data : " + util.inspect(data, null, 4))
                    data.forEach(item => {
                        updateValue(item)
                    });
                })
            }

            function command(message) {
                const { device, prop, value } = message;
                if (!device || !prop) return false;
                let did = device;

                const regex = /^d\d+$/;
                const isdid = regex.test(device);
                const hasValue = value === undefined ? false : true;

                if (!isdid) {
                    if (dnmap.hasOwnProperty(device)) {
                        did = dnmap[device]["_id"];
                    }
                    else {
                        return false;
                    }
                }

                const baseCommand = {
                    type: 'command',
                    command: hasValue ? 'setval' : 'device',
                    did: did,
                    prop: prop
                };
                if (hasValue) {
                    baseCommand["value"] = value;
                }
                plugin.send(baseCommand);
                return true;
            }

            async function getDbData(message) {
                const { device, prop, start, end } = message;
                if (!device || !prop || !start || !end) return false;
                const dbresponse = await plugin.get('hist', { dn_prop: `${device}.${prop}`, start: start, end: end });
                client.send(dbResponseTopic, dbresponse);
                return true;
            }

            async function messageHandler(topic, message) {
                let res = true;
                let msgdata;
                let errorstring = "";
                try {
                    msgdata = checkJsonValue(message);
                    if (topic === commandTopic) {
                        if (!deviceControl) { errorstring = "Device control disabled" }
                        else { res = command(msgdata) }
                    }
                    if (topic === dbTopic) {
                        if (!dbAccess) { errorstring = "DB access disabled" }
                        else { res = await getDbData(msgdata) }
                    }
                    if (topic.startsWith('$SYS/') && topic.endsWith('/new/clients')) {
                        const newclientId = msgdata.toString();
                        log("New client: " + newclientId, 0, false);
                        if (!republishOnNewclient) return;
                        if (newclientId !== clientId) {
                            repubCheck(true);
                        }
                    }

                    if (!res) errorstring = "Invalid command format";
                } catch (error) {
                    errorstring = "Handle message error";
                    log(`Command proccessing error: ${errorstring}: ${message} || ${error}`, 0, false)
                }
                finally {
                    if (errorstring) {
                        client.send(errorTopic, { command: msgdata, error: errorstring })
                    }
                }
            }


            function terminate() {
                plugin.log('TERMINATE PLUGIN', 2);
                process.send({ type: 'procinfo', data: { connection: 0 } });
                plugin.exit(100);
            }

            plugin.onChange('extra', async recs => { //TODO
                log("Change extra subs >>> restart...", 0, false);
                plugin.exit(2);
            })

            process.on('SIGTERM', () => {
                terminate();
            });

            if (plugin.onStop) {
                plugin.onStop(async () => {
                    terminate();
                });
            }


            await getOptions();
            await getFolders();
            await checkExtra();
            await buildByLocations();
            await buildByTag();
            await buildByDevice();
            startClient();

        } catch (error) {
            log("Main proccess error: " + error, 0, false)
        }
    })();
}
