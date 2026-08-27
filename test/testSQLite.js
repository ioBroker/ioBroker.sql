/* jshint -W097 */ // jshint strict:false
/*jslint node: true */
/*jshint expr: true*/
const assert = require('node:assert');
const setup = require('./lib/setup');
const tests = require('./lib/testcases');

let objects = null;
let states = null;
let onStateChanged = null;
let sendToID = 1;

const adapterShortName = setup.adapterName.substring(setup.adapterName.indexOf('.') + 1);

let now = new Date().getTime();

// `system.adapter.<name>.0.alive` is written with `expire: 1`, so it exists for one second out of the
// fifteen between two adapter heartbeats. Sampling it once per second is a lottery that a loaded machine
// loses - every sample can land in the fourteen seconds where the state is gone - so sample much faster
// than the state lives, and give the adapter a full minute to come up on a cold CI runner.
function checkConnectionOfAdapter(cb, counter) {
    counter ||= 0;
    if (counter > 240) {
        cb?.('Cannot check connection');
        return;
    }

    states.getState(`system.adapter.${adapterShortName}.0.alive`, (err, state) => {
        if (err) {
            console.error(`${adapterShortName}:${err}`);
        }
        if (state?.val) {
            cb?.();
        } else {
            setTimeout(() => checkConnectionOfAdapter(cb, counter + 1), 250);
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
            const config = await setup.getAdapterConfig();
            // enable adapter
            config.common.enabled = true;
            config.common.loglevel = 'debug';

            config.native.enableDebugLogs = true;
            config.native.dbtype = 'sqlite';

            await setup.setAdapterConfig(config.common, config.native);

            setup.startController(
                true,
                function (id, obj) {},
                function (id, state) {
                    if (state) onStateChanged(id, state);
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

    it(`Test ${__filename} Check if adapter started`, function (done) {
        this.timeout(120000);
        checkConnectionOfAdapter(function (error) {
            // Without this the suite would keep running against an adapter that never came up, and
            // every following test would fail with a bare "Timeout of Nms exceeded" instead.
            if (error) {
                done(new Error(error));
                return;
            }
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
                    assert.strictEqual(result.error, undefined);
                    assert.strictEqual(result.success, true);
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
                            assert.strictEqual(result.error, undefined);
                            assert.strictEqual(result.success, true);
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

    tests.register(it, sendTo, adapterShortName, false, 0, 2);

    it(`Test ${__filename}: Check Datapoint Types`, function (done) {
        this.timeout(10000);

        sendTo('sql.0', 'query', 'SELECT name, type FROM datapoints', function (result) {
            console.log(`SQLite: ${JSON.stringify(result.result, null, 2)}`);
            assert.ok(result.result.length >= 3, `${result.result.length} >= 3`);
            for (let i = 0; i < result.result.length; i++) {
                if (result.result[i].name === 'sql.0.testValue') {
                    assert.strictEqual(result.result[i].type, 0);
                } else if (result.result[i].name === 'sql.0.testValueDebounce') {
                    assert.strictEqual(result.result[i].type, 0);
                } else if (result.result[i].name === 'system.adapter.sql.0.memHeapTotal') {
                    assert.strictEqual(result.result[i].type, 1);
                } else if (result.result[i].name === 'system.adapter.sql.0.uptime') {
                    assert.strictEqual(result.result[i].type, 2);
                }
            }

            setTimeout(() => done(), 5000);
        });
    });

    it(`Test ${__filename}: Read the list of all datapoints`, function (done) {
        this.timeout(10000);

        sendTo('sql.0', 'getDatapoints', {}, function (result) {
            assert.ok(!result.error, `${result.error}`);
            assert.ok(Array.isArray(result.result), 'array expected');
            assert.ok(result.result.length >= 3, `${result.result.length} >= 3`);

            const testValue = result.result.find(point => point.id === 'sql.0.testValue');
            assert.ok(testValue, 'sql.0.testValue not found');
            assert.strictEqual(testValue.type, 'Number');
            assert.ok(testValue.index > 0, `${testValue.index} > 0`);

            // sorted by ID
            const ids = result.result.map(point => point.id);
            assert.deepStrictEqual(ids, [...ids].sort(), 'not sorted by id');
            done();
        });
    });

    it(`Test ${__filename}: Read raw entries page by page`, function (done) {
        this.timeout(20000);

        sendTo('sql.0', 'getRawEntries', { id: 'sql.0.testValue', limit: 5 }, function (result) {
            assert.ok(!result.error, `${result.error}`);
            assert.strictEqual(result.type, 'Number');
            assert.strictEqual(result.table, 'ts_number');
            assert.strictEqual(result.sort, 'desc');
            console.log(`SQLite: ${result.total} raw entries for sql.0.testValue`);
            assert.ok(result.total >= 10, `${result.total} >= 10`);
            assert.strictEqual(result.result.length, 5);
            // newest first
            assert.ok(result.result[0].ts >= result.result[4].ts);

            const total = result.total;
            const newest = result.result[0].ts;

            sendTo('sql.0', 'getRawEntries', { id: 'sql.0.testValue', limit: 5, offset: 5 }, function (result) {
                assert.ok(!result.error, `${result.error}`);
                assert.strictEqual(result.total, total);
                assert.strictEqual(result.result.length, 5);
                // the second page is older than the first one
                assert.ok(result.result[0].ts < newest, `${result.result[0].ts} < ${newest}`);

                sendTo('sql.0', 'getRawEntries', { id: 'sql.0.testValue', limit: 1, sort: 'asc' }, function (result) {
                    assert.ok(!result.error, `${result.error}`);
                    assert.strictEqual(result.sort, 'asc');
                    assert.strictEqual(result.result.length, 1);
                    assert.ok(result.result[0].ts <= newest);

                    sendTo('sql.0', 'getRawEntries', { id: 'sql.0.doesNotExist' }, function (result) {
                        assert.ok(result.error, `Error expected, but got ${JSON.stringify(result)}`);
                        done();
                    });
                });
            });
        });
    });

    it(`Test ${__filename}: Update a value of a datapoint with disabled logging`, function (done) {
        this.timeout(60000);

        sendTo('sql.0', 'disableHistory', { id: 'sql.0.testValue' }, function (result) {
            assert.strictEqual(result.error, undefined);
            assert.strictEqual(result.success, true);

            // wait till the adapter processed the object change and flushed its RAM buffer
            setTimeout(function () {
                sendTo('sql.0', 'getRawEntries', { id: 'sql.0.testValue', limit: 1 }, function (result) {
                    assert.ok(!result.error, `${result.error}`);
                    assert.strictEqual(result.result.length, 1);

                    const entry = result.result[0];
                    const newValue = typeof entry.val === 'number' ? entry.val + 42 : 42;

                    sendTo(
                        'sql.0',
                        'update',
                        { id: 'sql.0.testValue', state: { ts: entry.ts, val: newValue, ack: true, q: 0 } },
                        function (result) {
                            assert.strictEqual(result.error, undefined);
                            assert.strictEqual(result.success, true);

                            setTimeout(function () {
                                sendTo(
                                    'sql.0',
                                    'getRawEntries',
                                    { id: 'sql.0.testValue', start: entry.ts, end: entry.ts },
                                    function (result) {
                                        assert.ok(!result.error, `${result.error}`);
                                        assert.strictEqual(result.result.length, 1);
                                        assert.strictEqual(result.result[0].val, newValue);
                                        done();
                                    },
                                );
                            }, 2000);
                        },
                    );
                });
            }, 5000);
        });
    });

    it(`Test ${__filename}: Update a value of an unknown datapoint reports an error`, function (done) {
        this.timeout(10000);

        sendTo('sql.0', 'update', { id: 'sql.0.doesNotExist', state: { ts: Date.now(), val: 1 } }, function (result) {
            assert.ok(result.error, `Error expected, but got ${JSON.stringify(result)}`);
            assert.strictEqual(result.success, undefined);
            done();
        });
    });

    it(`Test ${__filename}: Delete all data of a datapoint with disabled logging`, function (done) {
        this.timeout(60000);

        const countIn = table =>
            `SELECT COUNT(*) AS cnt FROM ${table} WHERE id=(SELECT id FROM datapoints WHERE name='sql.0.testValue')`;

        // stop the logging first: the adapter forgets everything it knows about this datapoint
        sendTo('sql.0', 'disableHistory', { id: 'sql.0.testValue' }, function (result) {
            assert.strictEqual(result.error, undefined);
            assert.strictEqual(result.success, true);

            // wait till the adapter processed the object change and flushed its RAM buffer
            setTimeout(function () {
                // the test cases do not write counters, so add one manually
                const insertCounter = `INSERT INTO ts_counter (id, ts, val) VALUES ((SELECT id FROM datapoints WHERE name='sql.0.testValue'), ${Date.now()}, 1)`;

                sendTo('sql.0', 'query', insertCounter, function (result) {
                    assert.ok(!result.error, `${result.error}`);

                    sendTo('sql.0', 'query', countIn('ts_number'), function (result) {
                        assert.ok(!result.error, `${result.error}`);
                        console.log(`SQLite: ts_number rows before deleteAll: ${result.result[0].cnt}`);
                        assert.ok(result.result[0].cnt > 0, `${result.result[0].cnt} > 0`);

                        sendTo('sql.0', 'query', countIn('ts_counter'), function (result) {
                            assert.ok(!result.error, `${result.error}`);
                            console.log(`SQLite: ts_counter rows before deleteAll: ${result.result[0].cnt}`);
                            assert.ok(result.result[0].cnt > 0, `${result.result[0].cnt} > 0`);

                            sendTo('sql.0', 'deleteAll', { id: 'sql.0.testValue' }, function (result) {
                                assert.strictEqual(result.error, undefined);
                                assert.strictEqual(result.success, true);

                                setTimeout(function () {
                                    sendTo('sql.0', 'query', countIn('ts_number'), function (result) {
                                        assert.ok(!result.error, `${result.error}`);
                                        assert.strictEqual(result.result[0].cnt, 0);

                                        sendTo('sql.0', 'query', countIn('ts_counter'), function (result) {
                                            assert.ok(!result.error, `${result.error}`);
                                            assert.strictEqual(result.result[0].cnt, 0);
                                            done();
                                        });
                                    });
                                }, 3000);
                            });
                        });
                    });
                });
            }, 5000);
        });
    });

    it(`Test ${__filename}: Delete all data of an unknown datapoint reports an error`, function (done) {
        this.timeout(10000);

        sendTo('sql.0', 'deleteAll', { id: 'sql.0.doesNotExist' }, function (result) {
            assert.ok(result.error, `Error expected, but got ${JSON.stringify(result)}`);
            assert.strictEqual(result.success, undefined);
            done();
        });
    });

    after(`Test ${__filename} Stop js-controller`, function (done) {
        this.timeout(30000);

        setup.stopController(function (normalTerminated) {
            console.log(`SQLite: Adapter normal terminated: ${normalTerminated}`);
            setTimeout(done, 3000);
        });
    });
});
