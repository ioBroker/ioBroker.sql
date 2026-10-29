// Creates the SQL adapter class with a stubbed @iobroker/adapter-core, so single methods of
// build/main can be tested without js-controller. adapter-core itself is never loaded (it exits the
// process when js-controller cannot be resolved).
const Module = require('node:module');

class FakeAdapter {
    constructor() {
        this.namespace = 'sql.0';
        this.config = {};
        this.logs = [];
        const push = level => message => this.logs.push(`${level}: ${message}`);
        this.log = { debug: () => {}, info: push('info'), warn: push('warn'), error: push('error') };
    }
}

function createAdapter() {
    const origLoad = Module._load;
    Module._load = function (request, ...rest) {
        if (request === '@iobroker/adapter-core') {
            return { Adapter: FakeAdapter, getAbsoluteDefaultDataDir: () => '/tmp' };
        }
        return origLoad.call(this, request, ...rest);
    };
    try {
        return require('../../build/main')({});
    } finally {
        Module._load = origLoad;
    }
}

module.exports = { createAdapter };
