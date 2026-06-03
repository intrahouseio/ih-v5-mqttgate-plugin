const util = require('util');
const app = require('./app');

(async () => {
    let plugin;
    try {
        const opt = getOptFromArgs();
        const pluginapi = opt && opt.pluginapi ? opt.pluginapi : 'ih-plugin-api';
        plugin = require(pluginapi + '/index.js')();

        plugin.log('Plugin is start.', 0);

        plugin.params = await plugin.params.get();
        plugin.log('Recieve plugin parametrs: ' + util.inspect(plugin.params), 1);

        plugin.extra = await plugin.extra.get();

        plugin.logger.setParams({ logsize: plugin.params.logsize, logrotate: plugin.params.logrotate });

        app(plugin);


    } catch (err) {
        plugin.exit(8, `Error: ${util.inspect(err)}`);
    }
})();

function getOptFromArgs() {
    let opt;
    try {
        opt = JSON.parse(process.argv[2]); //
    } catch (e) {
        opt = {};
    }
    return opt;
}
