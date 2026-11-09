/* jshint -W097 */
/*jslint node: true */
/*jshint expr: true*/
const assert = require('node:assert');
const { Client } = require('pg');
const setup = require('./lib/setup');

let objects = null;
let states = null;
let onStateChanged = null;
let sendToID = 1;

const adapterShortName = setup.adapterName.substring(setup.adapterName.indexOf('.') + 1);

// A database of its own, so this file neither reads nor leaves rows in the "iobroker" database that
// testPostgreSQL.js asserts on when the glob runs them in one process.
const DB_NAME = 'iob_synccommit';

const SUPER_USER = process.env.SQL_USER || 'postgres';
const SUPER_PASS = process.env.SQL_PASS || '';

async function asSuperuser(statements) {
    const client = new Client({
        host: '127.0.0.1',
        port: 5432,
        user: SUPER_USER,
        password: SUPER_PASS,
        database: 'postgres',
    });
    await client.connect();
    try {
        const results = [];
        for (const sql of statements) {
            results.push(await client.query(sql));
        }
        return results;
    } finally {
        await client.end();
    }
}

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
        if (state && state.val) {
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

        asSuperuser([`DROP DATABASE IF EXISTS ${DB_NAME}`])
            .then(() => {
                setup.setupController(async function () {
                    const config = await setup.getAdapterConfig();
                    config.common.enabled = true;
                    config.common.loglevel = 'debug';

                    config.native.enableDebugLogs = true;
                    config.native.host = '127.0.0.1';
                    config.native.port = 5432;
                    config.native.dbtype = 'postgresql';
                    config.native.user = SUPER_USER;
                    config.native.password = SUPER_PASS;
                    config.native.dbname = DB_NAME;
                    // the feature under test
                    config.native.pgSynchronousCommitOff = true;

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

                            await objects.setObjectAsync('system.adapter.test.0', {
                                common: {},
                                type: 'instance',
                            });
                            states.subscribeMessage('system.adapter.test.0');

                            await objects.setObjectAsync(`${adapterShortName}.0.syncCommitValue`, {
                                common: { type: 'number', role: 'state', read: true, write: true },
                                type: 'state',
                                native: {},
                            });

                            _done();
                        },
                    );
                });
            })
            .catch(_done);
    });

    it(`Test ${__filename}: adapter connects`, function (done) {
        this.timeout(120000);
        checkConnectionOfAdapter(function (error) {
            assert.ok(!error, error);
            let tries = 0;
            const poll = () => {
                states.getState(`${adapterShortName}.0.info.connection`, (err, state) => {
                    assert.ok(!err, err);
                    if (state && state.val === true) {
                        return done();
                    }
                    if (++tries > 200) {
                        return done(new Error('adapter never reported a connection'));
                    }
                    setTimeout(poll, 250);
                });
            };
            poll();
        });
    });

    it(`Test ${__filename}: the server default is still "on" for other sessions`, async function () {
        this.timeout(30000);
        // Control: without it an "off" from the adapter could just be the server's default, and the
        // next test would pass without the option doing anything.
        const [res] = await asSuperuser(['SHOW synchronous_commit']);
        console.log(`SyncCommit: test session synchronous_commit = ${res.rows[0].synchronous_commit}`);
        assert.strictEqual(res.rows[0].synchronous_commit, 'on');
    });

    it(`Test ${__filename}: synchronous_commit is off for the adapter's connections`, function (done) {
        this.timeout(30000);
        sendTo('sql.0', 'query', 'SHOW synchronous_commit', function (result) {
            console.log(`SyncCommit: adapter session = ${JSON.stringify(result.result)}`);
            assert.ok(!result.error, `query error: ${result.error}`);
            assert.strictEqual(result.result[0].synchronous_commit, 'off');
            done();
        });
    });

    it(`Test ${__filename}: writes and reads a value end-to-end`, function (done) {
        this.timeout(60000);
        const id = `${adapterShortName}.0.syncCommitValue`;
        const now = Date.now();

        sendTo(
            'sql.0',
            'enableHistory',
            {
                id,
                options: { changesOnly: false, debounce: 0, retention: 31536000, storageType: 'Number' },
            },
            function (result) {
                assert.strictEqual(result.error, undefined);
                assert.strictEqual(result.success, true);

                // let the adapter apply the new settings before writing
                setTimeout(function () {
                    states.setState(id, { val: 1516, ts: now, ack: true }, function () {
                        setTimeout(function () {
                            sendTo(
                                'sql.0',
                                'getHistory',
                                {
                                    id,
                                    options: { start: now - 10000, end: now + 10000, count: 100, aggregate: 'none' },
                                },
                                function (res) {
                                    assert.ok(!res.error, `getHistory error: ${res.error}`);
                                    const values = res.result.map(r => r.val);
                                    console.log(`SyncCommit getHistory: ${JSON.stringify(values)}`);
                                    assert.ok(values.includes(1516), 'the written value did not come back');
                                    done();
                                },
                            );
                        }, 3000);
                    });
                }, 3000);
            },
        );
    });

    after(`Test ${__filename} Stop js-controller`, function (done) {
        this.timeout(60000);
        setup.stopController(function (normalTerminated) {
            console.log(`SyncCommit: Adapter normal terminated: ${normalTerminated}`);
            asSuperuser([`DROP DATABASE IF EXISTS ${DB_NAME} WITH (FORCE)`])
                .then(() => setTimeout(done, 2000))
                .catch(() => setTimeout(done, 2000));
        });
    });
});
