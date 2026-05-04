const fs = require('fs').promises;
const path = require('path')


module.exports = {

    async saveData(command, data, unit, holder) {
        const plugindir = __dirname;
        const certdir = "cert";

        const KEYFILE = "private-key.pem";
        const CERTFILE = "public-cert.pem";
        const CAFILE = "csr.pem";

        let targetFile;

        if (command == "upload_key") { targetFile = KEYFILE }
        if (command == "upload_cert") { targetFile = CERTFILE }
        if (command == "upload_ca") { targetFile = CAFILE }

        if (targetFile) {
            await fs.writeFile(path.join(plugindir, certdir, targetFile), data);
            holder.emit('debug', 'plugin_' + unit, "Upload: " + targetFile)
        }

    },

    async upload_key(unit, indata, holder) {
        await this.saveData("upload_key", indata, unit, holder)
        return { response: 1 };
    },

    async upload_cert(unit, indata, holder) {
        await this.saveData("upload_cert", indata, unit, holder)
        return { response: 1 };
    },

    async upload_ca(unit, indata, holder) {
        await this.saveData("upload_ca", indata, unit, holder)
        return { response: 1 };
    }
};	