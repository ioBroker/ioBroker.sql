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

// A role that deliberately cannot reach the maintenance database "postgres". This is what a managed
// PostgreSQL offering (or any locked-down role) looks like, and the situation reported in #404/#285:
// the two-phase connect used to stop right here, because it always opened "postgres" first.
const RESTRICTED_USER = 'iob_nocreate';
const RESTRICTED_PASS = 'iob_nocreate_pw';
const RESTRICTED_DB = 'iob_nocreate';

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
        for (const sql of statements) {
            await client.query(sql);
        }
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

        // Build the restricted setup: the database must already exist (the adapter is told not to
        // create it) and the role must have no CONNECT privilege on "postgres".
        asSuperuser([
            `DROP DATABASE IF EXISTS ${RESTRICTED_DB}`,
            `DROP ROLE IF EXISTS ${RESTRICTED_USER}`,
            `CREATE ROLE ${RESTRICTED_USER} LOGIN PASSWORD '${RESTRICTED_PASS}'`,
            `CREATE DATABASE ${RESTRICTED_DB} OWNER ${RESTRICTED_USER}`,
            `REVOKE CONNECT ON DATABASE postgres FROM PUBLIC`,
            `REVOKE CONNECT ON DATABASE postgres FROM ${RESTRICTED_USER}`,
        ])
            .then(() => {
                setup.setupController(async function () {
                    const config = await setup.getAdapterConfig();
                    config.common.enabled = true;
                    config.common.loglevel = 'debug';

                    config.native.enableDebugLogs = true;
                    config.native.host = '127.0.0.1';
                    config.native.port = 5432;
                    config.native.dbtype = 'postgresql';
                    config.native.user = RESTRICTED_USER;
                    config.native.password = RESTRICTED_PASS;
                    config.native.dbname = RESTRICTED_DB;
                    // the feature under test
                    config.native.doNotCreateDatabase = true;

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

                            await objects.setObjectAsync(`${adapterShortName}.0.testValue`, {
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

    it(`Test ${__filename}: connects without ever opening the maintenance database`, function (done) {
        this.timeout(120000);
        checkConnectionOfAdapter(function (error) {
            assert.ok(!error, error);
            // info.connection only becomes true once a working connection was actually established.
            // Before the fix the adapter opened "postgres" first, was refused (no CONNECT privilege)
            // and looped on the 30 s reconnect forever, so this never turned true.
            let tries = 0;
            const poll = () => {
                states.getState(`${adapterShortName}.0.info.connection`, (err, state) => {
                    assert.ok(!err, err);
                    if (state && state.val === true) {
                        return done();
                    }
                    if (++tries > 200) {
                        return done(
                            new Error('adapter never reported a connection - it could not reach its own database'),
                        );
                    }
                    setTimeout(poll, 250);
                });
            };
            poll();
        });
    });

    it(`Test ${__filename}: created its tables in the configured database`, async function () {
        this.timeout(60000);
        // The tables must exist in the restricted database, created by init() over the direct
        // connection - no maintenance database involved.
        const client = new Client({
            host: '127.0.0.1',
            port: 5432,
            user: RESTRICTED_USER,
            password: RESTRICTED_PASS,
            database: RESTRICTED_DB,
        });
        await client.connect();
        try {
            const res = await client.query(
                `SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name`,
            );
            const tables = res.rows.map(r => r.table_name);
            console.log(`NoCreateDb tables: ${JSON.stringify(tables)}`);
            for (const expected of ['datapoints', 'sources', 'ts_bool', 'ts_counter', 'ts_number', 'ts_string']) {
                assert.ok(tables.includes(expected), `table ${expected} was not created`);
            }
        } finally {
            await client.end();
        }
    });

    it(`Test ${__filename}: writes and reads a value end-to-end`, function (done) {
        this.timeout(60000);
        const id = `${adapterShortName}.0.testValue`;
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
                    states.setState(id, { val: 4711, ts: now, ack: true }, function () {
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
                                    console.log(`NoCreateDb getHistory: ${JSON.stringify(values)}`);
                                    assert.ok(values.includes(4711), 'the written value did not come back');
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
            console.log(`NoCreateDb: Adapter normal terminated: ${normalTerminated}`);
            // put the maintenance database back the way it was and drop the fixture
            asSuperuser([
                `GRANT CONNECT ON DATABASE postgres TO PUBLIC`,
                `DROP DATABASE IF EXISTS ${RESTRICTED_DB}`,
                `DROP ROLE IF EXISTS ${RESTRICTED_USER}`,
            ])
                .then(() => setTimeout(done, 2000))
                .catch(() => setTimeout(done, 2000));
        });
    });
});
