const util = require('util');
const fs = require('fs').promises;
const path = require('path');
const MqttClient = require('./MqttClient');


module.exports = async function (plugin) {
    const params = plugin.params;
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
    const clearRetainOnStart = plugin.params.clearRetainOnStart || 0;
    const republishInt = (plugin.params.republishInt || 60) * 1000;
    const republishOnNewclient = plugin.params.republishOnNewclient || 0;
    const deviceControl = plugin.params.deviceControl ? true : false;
    const dbAccess = plugin.params.dbAccess ? true : false;

    const debug = plugin.params.debug || false;

    function log(msg, level = 0, isDebugMsg = false) {
        (!isDebugMsg || debug) && plugin.log(msg, level);
    }

    function delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    (async () => {
        try {
            log("App MQTTGATE is started!", 0, false);
            process.send({ type: 'procinfo', data: { connection: 1 } });

            let client;
            let clearRetainFlag;
            let clearRetainTimer;
            let clearRetainWd;

            const brokerUrl = `${protocol}://${brokerIP}:${brokerPort}`;
            const willpayload = { connection: false, heartbeat: Date.now() };
            const options = { clientId: clientId, rejectUnauthorized: false };
            const paramsInfoTopic = clientId + "/status/paramsInfo";
            const extraInfoTopic = clientId + "/status/extraInfo";
            const foldersInfo = clientId + "/status/foldersInfo";
            const connectionStateTopic = clientId + "/status/connection";
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
            const didList = [];// решает проблему многократного срабатывания при sub

            const devlinks = {};
            const didmap = {};
            const dnmap = {};
            const foldersmap = {};
            const locationsroot = {};
            const valuemap = {};
            const metamap = {};
            const updatemap = {};
            const clearRetainTopics = [];
            const clearRetainTimerValue = 2000;
            const maxClearRetainTime = 20000;


            function startClient() {
                client = new MqttClient(brokerUrl, options);

                client.on("tryconnect", () => log("Try connect to: " + brokerUrl, 0, false));
                client.on("connect", () => onConnect());
                client.on("disconnect", () => onDisconnect());
                client.on("error", (err) => onError(err));
                client.on("data", (data) => onMessage(data));
                client.on("debug", (message) => log("MQTT client debug >> " + message, 0, true));

                client.connect();
            }

            function onConnect() {
                process.send({ type: 'procinfo', data: { connection: 2 } });
                log("✅ Connected", 0, false);
                if (clearRetainOnStart) { clearRetain() }
                else { mainStart() }
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
                const qos = data.packet.qos;
                const retain = data.packet.retain;
                log(`MSG >> Topic: ${topic} || Message: ${message} | qos: ${qos} | retain: ${retain}`, 2, true)
                messageHandler(topic, message, qos, retain);
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

            function mainStart() {
                heartbeat();
                sentMeta();
                send(errorTopic, {});
                subOnDevices();
                subscribe(commandTopic);
                subscribe(dbTopic);
                subscribe(newCliTopic);
                updatecheck();
            }

            async function finishClearRetain() {
                if (!clearRetainFlag) return;
                clearRetainFlag = false;
                clearTimeout(clearRetainTimer);
                clearTimeout(clearRetainWd);

                unsubscribe(clientId + "/#");
                for (let topic of clearRetainTopics) {
                    send(topic, '', 0, true);
                    await delay(50);
                }
                await delay(1000);
                mainStart();
            }

            function restartClearRetainTimer() {
                clearTimeout(clearRetainTimer);
                clearRetainTimer = setTimeout(finishClearRetain, clearRetainTimerValue);
            }

            async function clearRetain() {
                log("Clear retain data... Start after clean...", 0, false);
                clearRetainFlag = true;
                subscribe(clientId + "/#");
                clearRetainWd = setTimeout(finishClearRetain, maxClearRetainTime);
            }

            function getParams() {
                const sendParams = {};
                if (params) {
                    sendParams["heartbeatInt"] = params?.heartbeatInt;
                    sendParams["republishInt"] = params?.republishInt;
                    sendParams["clientId"] = params?.clientId;
                    sendParams["version"] = params?.version;
                    sendParams["clearRetainOnStart"] = params?.clearRetainOnStart;
                    sendParams["republishOnNewclient"] = params?.republishOnNewclient;
                    sendParams["topicsByName"] = params?.topicsByName;
                    sendParams["deviceControl"] = params?.deviceControl;
                    sendParams["dbAccess"] = params?.dbAccess;
                }
                return sendParams;
            }

            function getExtra() {
                const sendExtra = [];
                if (extra) {
                    extra.forEach(extraFilter => {
                        sendExtra.push(
                            {
                                filter: extraFilter?.filter,
                                locationStr: extraFilter?.locationStr,
                                tagStr: extraFilter?.tagStr,
                                did: extraFilter?.did,
                                rootTopic: extraFilter?.rootTopic
                            }
                        )
                    });
                }
                return sendExtra;
            }

            function heartbeat() {
                const startInd = { connection: true, heartbeat: Date.now() };
                send(connectionStateTopic, startInd, 1, true);
                send(paramsInfoTopic, getParams(), 1, true);
                send(extraInfoTopic, getExtra(), 1, true);
                send(foldersInfo, foldersmap, 1, true);
                setTimeout(heartbeat, Number(heartbeatInt) * 1000);
            }

            function repubCheck(isnewcli) {
                const repubArr = [];
                const ticknow = performance.now();
                Object.keys(updatemap).forEach(did => {

                    Object.keys(updatemap[did]).forEach(prop => {
                        const didprop = { dev: did, prop: prop }
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
                republish(repubArr);
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
                    const dev = devprop.dev;
                    const prop = devprop.prop;
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
                    log("Read certificates...", 0, false);
                    options["key"] = await readCertFile(KEYFILE);
                    options["cert"] = await readCertFile(CERTFILE);
                    options["ca"] = await readCertFile(CAFILE);
                    options["rejectUnauthorized"] = true;
                }
            }

            async function getFolders() {
                const places = await plugin.places.get();
                for (let folder of places) {
                    foldersmap[folder.id] = folder;
                }
            }

            async function readCertFile(file) {
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
                    const initValue = fulldev.props[prop].value;
                    const initTs = fulldev.props[prop].ts;
                    const initQuality = fulldev.props[prop].quality;
                    valuemap[id][prop] = { value: initValue, ts: initTs, quality: initQuality };

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
                        if (!didList.includes(dev._id)) { didList.push(dev._id) }// решает проблему многократного срабатывания при sub
                    }
                }
            }

            async function buildByTag() {
                for (let tag of tags) {
                    const devarr = await plugin.devices.get({ tag: tag })
                    for (let dev of devarr) {
                        await devlinksInsert(dev, tagTopic, tag + "/");
                        if (!didList.includes(dev._id)) { didList.push(dev._id) }// решает проблему многократного срабатывания при sub
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
                    await devlinksInsert(dev, deviceTopic, "");
                    if (!didList.includes(dev._id)) { didList.push(dev._id) }// решает проблему многократного срабатывания при sub
                }
            }

            function addLinksToMeta() {
                Object.values(devlinks).forEach(links => {
                    links.forEach(link => {
                        const linkpath = link.split("/");
                        const cid = linkpath[0];
                        const pubType = linkpath[1];
                        const dev = linkpath[linkpath.length - 1];
                        const path = linkpath.slice(2).join("/");

                        if (cid !== clientId) return;

                        if (!metamap[dev].hasOwnProperty("locations")) { metamap[dev]["locations"] = [] }
                        if (pubType === "locations") {
                            metamap[dev]["locations"].push(path)
                        }
                    });
                });
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
                addLinksToMeta();
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
                plugin.onSub('devices', { did_prop: didList }, data => { // решает проблему многократного срабатывания при sub
                    //plugin.onSub('devices', { extra: 1 }, data => {
                    //log("onSub data : " + util.inspect(data, null, 4)) didList
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
                    if (dnmap.hasOwnProperty(device)) { did = dnmap[device]["_id"] }
                    else { return false }
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

            async function messageHandler(topic, message, qos, retain) {
                let res = true;
                let msgdata;
                let errorstring = "";
                try {
                    msgdata = checkJsonValue(message);

                    if (clearRetainFlag && retain) {
                        log("Clear retain topic: " + topic, 0, true);
                        clearRetainTopics.push(topic);
                        restartClearRetainTimer();
                        return;
                    }
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
                        if (newclientId !== clientId) repubCheck(true);
                    }

                    if (!res) errorstring = "Invalid command format";
                } catch (error) {
                    errorstring = "Handle message error";
                    log(`Command proccessing error: ${errorstring}: ${topic} | ${message} || ${error}`, 0, false);
                }
                finally {
                    if (errorstring) {
                        client.send(errorTopic, { command: msgdata, error: errorstring });
                    }
                }
            }


            function terminate() {
                log('TERMINATE PLUGIN', 0, false);
                process.send({ type: 'procinfo', data: { connection: 0 } });
                plugin.exit(100);
            }

            plugin.onChange('extra', data => { //TODO
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
            log("Main process error: " + error, 0, false);
        }
    })();
}
