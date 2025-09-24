/* jshint -W097 */ // jshint strict:false
/*jslint node: true */
/*jshint expr: true*/
const expect = require('chai').expect;
const setup = require('./lib/setup');
const tests = require('./lib/testcases');

let objects = null;
let states = null;
let onStateChanged = null;
let onObjectChanged = null;
let sendToID = 1;

const adapterShortName = setup.adapterName.substring(setup.adapterName.indexOf('.') + 1);

let now = new Date().getTime();

function checkConnectionOfAdapter(cb, counter) {
    counter = counter || 0;
    if (counter > 20) {
        cb && cb('Cannot check connection');
        return;
    }

    states.getState(`system.adapter.${adapterShortName}.0.alive`, function (err, state) {
        if (err) console.error(`PostgreSQL: ${err}`);
        if (state && state.val) {
            cb && cb();
        } else {
            setTimeout(function () {
                checkConnectionOfAdapter(cb, counter + 1);
            }, 1000);
        }
    });
}

function checkValueOfState(id, value, cb, counter) {
    counter = counter || 0;
    if (counter > 20) {
        cb && cb(`Cannot check value Of State ${id}`);
        return;
    }

    states.getState(id, function (err, state) {
        if (err) console.error(`PostgreSQL: ${err}`);
        if (value === null && !state) {
            cb && cb();
        } else if (state && (value === undefined || state.val === value)) {
            cb && cb();
        } else {
            setTimeout(function () {
                checkValueOfState(id, value, cb, counter + 1);
            }, 500);
        }
    });
}

function sendTo(target, command, message, callback) {
    onStateChanged = function (id, state) {
        if (id === 'messagebox.system.adapter.test.0') {
            callback(state.message);
        }
    };

    states.pushMessage(`system.adapter.${target}`, {
        command: command,
        message: message,
        from: 'system.adapter.test.0',
        callback: {
            message: message,
            id: sendToID++,
            ack: false,
            time: new Date().getTime(),
        },
    });
}

describe(`Test ${__filename}`, function () {
    before(`Test ${__filename} Start js-controller`, function (_done) {
        this.timeout(600000); // because of first install from npm
        setup.adapterStarted = false;

        setup.setupController(async function () {
            var config = await setup.getAdapterConfig();
            // enable adapter
            config.common.enabled = true;
            config.common.loglevel = 'debug';

            config.native.enableDebugLogs = true;
            config.native.host = '127.0.0.1';
            config.native.dbtype = 'postgresql';
            config.native.user = 'postgres';
            config.native.password = process.env.SQL_PASS || '';

            await setup.setAdapterConfig(config.common, config.native);

            setup.startController(
                true,
                function (id, obj) {},
                function (id, state) {
                    if (onStateChanged) onStateChanged(id, state);
                },
                async (_objects, _states) => {
                    objects = _objects;
                    states = _states;

                    await tests.preInit(objects, states, sendTo, adapterShortName);

                    _done();
                },
            );
        });
    });

    it(`Test ${__filename}: Check if adapter started`, function (done) {
        this.timeout(60000);
        checkConnectionOfAdapter(function () {
            now = new Date().getTime();
            sendTo(
                'sql.0',
                'enableHistory',
                {
                    id: 'system.adapter.sql.0.memHeapTotal',
                    options: {
                        changesOnly: false,
                        debounce: 0,
                        retention: 31536000,
                        storageType: 'String',
                    },
                },
                function (result) {
                    expect(result.error).to.be.undefined;
                    expect(result.success).to.be.true;
                    sendTo(
                        'sql.0',
                        'enableHistory',
                        {
                            id: 'system.adapter.sql.0.uptime',
                            options: {
                                changesOnly: false,
                                debounce: 0,
                                retention: 31536000,
                                storageType: 'Boolean',
                            },
                        },
                        function (result) {
                            expect(result.error).to.be.undefined;
                            expect(result.success).to.be.true;
                            // wait till adapter receives the new settings
                            setTimeout(function () {
                                done();
                            }, 10000);
                        },
                    );
                },
            );
        });
    });

    tests.register(it, expect, sendTo, adapterShortName, true, 0, 2);

    it(`Test ${__filename}: Check Datapoint Types`, function (done) {
        this.timeout(5000);

        sendTo('sql.0', 'query', 'SELECT name, type FROM datapoints', function (result) {
            console.log(`PostgreSQL: ${JSON.stringify(result.result, null, 2)}`);
            expect(result.result.length).to.least(3);
            for (var i = 0; i < result.result.length; i++) {
                if (result.result[i].name === 'sql.0.testValue') {
                    expect(result.result[i].type).to.be.equal(0);
                } else if (result.result[i].name === 'sql.0.testValueDebounce') {
                    expect(result.result[i].type).to.be.equal(0);
                } else if (result.result[i].name === 'system.adapter.sql.0.memHeapTotal') {
                    expect(result.result[i].type).to.be.equal(1);
                } else if (result.result[i].name === 'system.adapter.sql.0.uptime') {
                    expect(result.result[i].type).to.be.equal(2);
                }
            }

            setTimeout(function () {
                done();
            }, 3000);
        });
    });

    after(`Test ${__filename} Stop js-controller`, function (done) {
        this.timeout(16000);

        setup.stopController(function (normalTerminated) {
            console.log(`PostgreSQL: Adapter normal terminated: ${normalTerminated}`);
            setTimeout(done, 2000);
        });
    });
});
