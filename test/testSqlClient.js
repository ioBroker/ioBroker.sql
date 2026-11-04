const assert = require('node:assert');
const { EventEmitter } = require('node:events');
const SQLClient = require('../build/lib/sql-client').default;
const { SQLClientPool } = require('../build/lib/sql-client-pool');

// All four drivers hand out an EventEmitter and emit `error` on it when the server drops the
// socket. This fake stands in for mysql2's `Connection`, pg's `Client`, mssql's `ConnectionPool`
// and sqlite3's `Database` alike.
class FakeConnection extends EventEmitter {
    constructor() {
        super();
        this.ended = false;
    }

    breakIt(code = 'PROTOCOL_CONNECTION_LOST') {
        this.emit('error', Object.assign(new Error('Connection lost: The server closed the connection.'), { code }));
    }
}

function fakeFactory() {
    const factory = {
        opened: [],
        openConnection(_options, callback) {
            const connection = new FakeConnection();
            factory.opened.push(connection);
            setImmediate(() => callback(null, connection));
        },
        closeConnection(connection, callback) {
            if (connection) {
                connection.ended = true;
            }
            setImmediate(() => callback(null));
        },
        execute(connection, _sql, callback) {
            setImmediate(() => callback(null, []));
        },
    };
    return factory;
}

describe('Test SQLClient driver error handling', function () {
    it('an error event with no listener does not throw and flags the client', function (done) {
        const factory = fakeFactory();
        const client = new SQLClient({}, factory);

        client.connect(err => {
            assert.ifError(err);
            assert.strictEqual(client.isBroken(), false, 'fresh client is not broken');
            assert.strictEqual(client.listenerCount('error'), 0, 'precondition: nobody listens');

            // Before the fix this reached Node as an unhandled 'error' event and terminated the
            // adapter with UNCAUGHT_EXCEPTION.
            assert.doesNotThrow(() => factory.opened[0].breakIt());

            assert.strictEqual(client.isBroken(), true);
            assert.strictEqual(client.getLastError().code, 'PROTOCOL_CONNECTION_LOST');
            done();
        });
    });

    it('forwards the error to a listener on the client', function (done) {
        const factory = fakeFactory();
        const client = new SQLClient({}, factory);

        client.connect(err => {
            assert.ifError(err);
            const seen = [];
            client.on('error', e => seen.push(e));

            factory.opened[0].breakIt('ECONNRESET');

            assert.strictEqual(seen.length, 1);
            assert.strictEqual(seen[0].code, 'ECONNRESET');
            assert.strictEqual(client.isBroken(), true);
            done();
        });
    });

    it('ignores a late error from a connection that was already replaced', function (done) {
        const factory = fakeFactory();
        const client = new SQLClient({}, factory);

        client.connect(err => {
            assert.ifError(err);
            const stale = factory.opened[0];

            client.disconnect(err => {
                assert.ifError(err);
                client.connect(err => {
                    assert.ifError(err);
                    assert.strictEqual(factory.opened.length, 2, 'a second connection was opened');
                    assert.strictEqual(client.isBroken(), false);

                    // mysql2 can still emit on a half-closed socket; that must not flag the
                    // healthy connection the client holds now.
                    assert.doesNotThrow(() => stale.breakIt());

                    assert.strictEqual(client.isBroken(), false, 'current connection stays usable');
                    assert.strictEqual(client.getLastError(), null);
                    done();
                });
            });
        });
    });

    it('resets the error state when the connection is replaced', function (done) {
        const factory = fakeFactory();
        const client = new SQLClient({}, factory);

        client.connect(() => {
            factory.opened[0].breakIt();
            assert.strictEqual(client.isBroken(), true);

            client.disconnect(() => {
                client.connect(() => {
                    assert.strictEqual(client.isBroken(), false, 'reconnect clears the flag');
                    assert.strictEqual(client.getLastError(), null);
                    done();
                });
            });
        });
    });
});

describe('Test SQLClientPool eviction of broken clients', function () {
    it('validate() rejects a broken client', function (done) {
        const factory = fakeFactory();
        const pool = new SQLClientPool({}, {}, factory);
        const client = new SQLClient({}, factory);

        client.connect(() => {
            pool.validate(client, (err, valid) => {
                assert.ifError(err);
                assert.strictEqual(valid, true, 'healthy client is valid');

                factory.opened[0].breakIt();

                pool.validate(client, (err, valid) => {
                    assert.ifError(err);
                    assert.strictEqual(valid, false, 'broken client is invalid');
                    done();
                });
            });
        });
    });

    it('borrow() replaces a broken pooled client with a fresh connection', function (done) {
        const factory = fakeFactory();
        const pool = new SQLClientPool({}, {}, factory);

        pool.open({ max_idle: 4 }, err => {
            assert.ifError(err);

            pool.borrow((err, first) => {
                assert.ifError(err);
                assert.strictEqual(factory.opened.length, 1);

                pool.return(first, () => {
                    // The server closes the connection while it sits idle in the pool - the case a
                    // shared-hosting MySQL produces on its own schedule.
                    factory.opened[0].breakIt();
                    assert.strictEqual(first.isBroken(), true);

                    pool.borrow((err, second) => {
                        assert.ifError(err);
                        assert.strictEqual(factory.opened.length, 2, 'a new connection was opened');
                        assert.strictEqual(second.isBroken(), false, 'the borrowed client is healthy');
                        assert.strictEqual(factory.opened[0].ended, true, 'the dead one was closed');
                        done();
                    });
                });
            });
        });
    });
});

describe('Test SQLClient detects a dead connection through a failed statement', function () {
    // mysql2 hands "connection lost" to the callback of the statement that is running when the
    // server closes the socket, and does NOT emit `error` on the connection in that case. Every
    // following statement then fails with "Can't add new command when connection is in closed
    // state", again only through the callback. Seen on a hosted MariaDB with wait_timeout=120:
    // all pooled clients ended up like this and every write failed until the adapter restarted.
    function closedState() {
        return Object.assign(new Error("Can't add new command when connection is in closed state"), {
            fatal: true,
        });
    }

    function failingFactory() {
        const factory = fakeFactory();
        factory.failNext = null;
        factory.execute = (connection, _sql, callback) => {
            const err = factory.failNext;
            factory.failNext = null;
            setImmediate(() => (err ? callback(err) : callback(null, [])));
        };
        return factory;
    }

    it('execute() flags the client when the driver reports a fatal error', function (done) {
        const factory = failingFactory();
        const client = new SQLClient({}, factory);

        client.connect(() => {
            factory.failNext = closedState();
            client.execute('SELECT 1', err => {
                assert.ok(err, 'the error is still passed on');
                assert.strictEqual(client.isBroken(), true);
                assert.match(client.getLastError().message, /closed state/);
                done();
            });
        });
    });

    it('executeAsync() flags the client as well', async function () {
        const factory = failingFactory();
        const client = new SQLClient({}, factory);
        await client.connectAsync();

        factory.failNext = Object.assign(new Error('Connection lost: The server closed the connection.'), {
            code: 'PROTOCOL_CONNECTION_LOST',
        });
        await assert.rejects(client.executeAsync('SELECT 1'));
        assert.strictEqual(client.isBroken(), true);
    });

    it('an ordinary SQL error does not flag the client', function (done) {
        const factory = failingFactory();
        const client = new SQLClient({}, factory);

        client.connect(() => {
            factory.failNext = Object.assign(new Error("Duplicate entry '1' for key 'PRIMARY'"), {
                code: 'ER_DUP_ENTRY',
                fatal: false,
            });
            client.execute('INSERT ...', err => {
                assert.ok(err);
                assert.strictEqual(client.isBroken(), false, 'the connection is fine');
                done();
            });
        });
    });

    it('notifies a listener on the client, like the error event of the connection', function (done) {
        const factory = failingFactory();
        const client = new SQLClient({}, factory);
        const seen = [];
        client.on('error', err => seen.push(err));

        client.connect(() => {
            factory.failNext = closedState();
            client.execute('SELECT 1', () => {
                factory.failNext = closedState();
                client.execute('SELECT 1', () => {
                    assert.strictEqual(seen.length, 1, 'once per broken client');
                    assert.match(seen[0].message, /closed state/);
                    done();
                });
            });
        });
    });

    it('borrow() replaces a client whose statement failed with a dead connection', function (done) {
        const factory = failingFactory();
        const pool = new SQLClientPool({}, {}, factory);

        pool.open({ max_idle: 4 }, err => {
            assert.ifError(err);
            pool.borrow((err, first) => {
                assert.ifError(err);
                factory.failNext = closedState();
                first.execute('INSERT ...', err => {
                    assert.ok(err);
                    pool.return(first, () => {
                        pool.borrow((err, second) => {
                            assert.ifError(err);
                            assert.strictEqual(factory.opened.length, 2, 'a new connection was opened');
                            assert.notStrictEqual(second, first, 'the dead client is not handed out again');
                            assert.strictEqual(second.isBroken(), false);
                            done();
                        });
                    });
                });
            });
        });
    });
});

describe('Test the adapter does not hand a broken client to a waiting borrower', function () {
    // The adapter limits the connections itself (maxConnections, poolBorrowGuard). A client given
    // back while somebody waits went to that waiter directly, without the pool's validate(), so a
    // client with a dead connection failed the writes of every queued waiter in turn.
    const { createAdapter } = require('./lib/stubAdapter');

    function closedState() {
        return Object.assign(new Error("Can't add new command when connection is in closed state"), { fatal: true });
    }

    it('a waiter gets a fresh connection when the returned client is broken', function (done) {
        const factory = fakeFactory();
        factory.failNext = null;
        factory.execute = (_connection, _sql, callback) => {
            const err = factory.failNext;
            factory.failNext = null;
            setImmediate(() => (err ? callback(err) : callback(null, [])));
        };
        const pool = new SQLClientPool({}, {}, factory);
        const adapter = createAdapter();
        adapter.config.maxConnections = 1;
        adapter.setConnected = () => {};

        pool.open({ max_idle: 4 }, err => {
            assert.ifError(err);
            adapter.clientPool = pool;
            adapter.borrowClientFromPool((err, first) => {
                assert.ifError(err);
                // maxConnections reached: the second borrower has to wait for the first client
                adapter.borrowClientFromPool((err, second) => {
                    assert.ifError(err);
                    assert.notStrictEqual(second, first, 'the broken client is not handed to the waiter');
                    assert.strictEqual(second.isBroken(), false);
                    assert.strictEqual(factory.opened.length, 2, 'a new connection was opened');
                    assert.strictEqual(adapter.activeConnections, 1);
                    done();
                });
                assert.strictEqual(adapter.poolBorrowGuard.length, 1, 'precondition: one waiter');

                factory.failNext = closedState();
                first.execute('INSERT ...', err => {
                    assert.ok(err);
                    assert.strictEqual(first.isBroken(), true);
                    adapter.returnClientToPool(first);
                });
            });
        });
    });

    it('a waiter still gets the returned client directly when it is healthy', function (done) {
        const factory = fakeFactory();
        const pool = new SQLClientPool({}, {}, factory);
        const adapter = createAdapter();
        adapter.config.maxConnections = 1;
        adapter.setConnected = () => {};

        pool.open({ max_idle: 4 }, err => {
            assert.ifError(err);
            adapter.clientPool = pool;
            adapter.borrowClientFromPool((err, first) => {
                assert.ifError(err);
                adapter.borrowClientFromPool((err, second) => {
                    assert.ifError(err);
                    assert.strictEqual(second, first);
                    assert.strictEqual(factory.opened.length, 1);
                    done();
                });
                adapter.returnClientToPool(first);
            });
        });
    });
});

describe('Test SQLClientPool replaces connections that were idle too long (max_age, #587)', function () {
    it('validate() rejects a client pooled longer than max_age', function (done) {
        const pool = new SQLClientPool({}, {}, fakeFactory());
        pool.open({ max_idle: 4, max_age: 1000 }, err => {
            assert.ifError(err);
            const client = new SQLClient({}, fakeFactory());
            client.pooled_at = Date.now() - 500;
            pool.validate(client, (err, valid) => {
                assert.strictEqual(valid, true, 'idle shorter than max_age');
                client.pooled_at = Date.now() - 1500;
                pool.validate(client, (err, valid) => {
                    assert.strictEqual(valid, false, 'idle longer than max_age, e.g. beyond wait_timeout');
                    done();
                });
            });
        });
    });
});

describe('Test SQLClientPool closes expired idle connections in the eviction run (#587)', function () {
    it('disconnects clients idle longer than max_age and broken ones, keeps the others', function (done) {
        const pool = new SQLClientPool({}, {}, fakeFactory());
        pool.open({ max_idle: 4, max_age: 1000 }, err => {
            assert.ifError(err);
            const make = (age, broken) => {
                const client = new SQLClient({}, fakeFactory());
                client.pooled_at = Date.now() - age;
                client.broken = broken;
                client.disconnected = false;
                client.disconnect = () => (client.disconnected = true);
                return client;
            };
            const fresh = make(100, false);
            const old = make(5000, false);
            const broken = make(100, true);
            pool.pool.push(fresh, old, broken);
            pool._evict(() => {
                assert.deepStrictEqual(pool.pool, [fresh]);
                assert.strictEqual(old.disconnected, true, 'closed before the server resets it');
                assert.strictEqual(broken.disconnected, true);
                assert.strictEqual(fresh.disconnected, false);
                done();
            });
        });
    });
});

describe('Test SQLClient passes bound values to the driver', function () {
    function recordingFactory(supportsParams) {
        const factory = fakeFactory();
        factory.supportsParams = supportsParams;
        factory.calls = [];
        factory.execute = (_connection, sql, callback, params) => {
            factory.calls.push({ sql, params });
            setImmediate(() => callback(null, []));
        };
        return factory;
    }

    it('hands SQL and values separately to a driver that binds them', async function () {
        const factory = recordingFactory(true);
        const client = new SQLClient({}, factory);
        await client.executeAsync({ sql: 'SELECT id FROM t WHERE name=?', params: ["it's"] });
        await client.executeAsync('SELECT 1');
        assert.deepStrictEqual(factory.calls, [
            { sql: 'SELECT id FROM t WHERE name=?', params: ["it's"] },
            { sql: 'SELECT 1', params: undefined },
        ]);
    });

    it('rejects a statement with values for a driver that does not bind them', function (done) {
        const factory = recordingFactory(false);
        const client = new SQLClient({}, factory);
        client.execute({ sql: 'SELECT id FROM t WHERE name=?', params: ['x'] }, err => {
            assert.match(String(err), /does not support bound parameters/);
            assert.strictEqual(factory.calls.length, 0, 'the SQL never reached the driver without its values');
            assert.strictEqual(client.isBroken(), false, 'not a connection error');
            done();
        });
    });
});

describe('Test the MySQL factory removes NO_BACKSLASH_ESCAPES from its sessions (#587)', function () {
    const { checkSqlMode, withoutNoBackslashEscapes } = require('../build/lib/mysql-client');

    /**
     * A connection whose session runs with `mode`; `setError` makes the SET fail.
     *
     * @param mode the session's sql_mode, or an Error for the SELECT
     * @param setError the error of `SET SESSION sql_mode`
     */
    function fakeConnection(mode, setError) {
        return {
            ended: false,
            set: null,
            query(sql, values, callback) {
                if (typeof values === 'function') {
                    callback = values;
                    values = undefined;
                }
                if (/^SET SESSION sql_mode = \?$/.test(sql)) {
                    this.set = values[0];
                    setImmediate(() => callback(setError || null));
                    return;
                }
                assert.match(sql, /@@SESSION\.sql_mode/);
                setImmediate(() => (mode instanceof Error ? callback(mode) : callback(null, [{ mode }])));
            },
            end(callback) {
                this.ended = true;
                setImmediate(() => callback?.());
            },
        };
    }

    it('withoutNoBackslashEscapes() removes only that mode, in any case', function () {
        assert.strictEqual(withoutNoBackslashEscapes('STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'), null);
        assert.strictEqual(withoutNoBackslashEscapes(''), null);
        assert.strictEqual(
            withoutNoBackslashEscapes('IGNORE_SPACE,no_backslash_escapes,STRICT_ALL_TABLES'),
            'IGNORE_SPACE,STRICT_ALL_TABLES',
        );
        assert.strictEqual(withoutNoBackslashEscapes('NO_BACKSLASH_ESCAPES'), '');
        assert.strictEqual(withoutNoBackslashEscapes(',NO_BACKSLASH_ESCAPES'), '', 'no stray comma');
    });

    it('accepts a session without the mode, with one query', function (done) {
        const connection = fakeConnection('STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION');
        checkSqlMode(
            connection,
            (err, conn) => {
                assert.ifError(err);
                assert.strictEqual(conn, connection);
                assert.strictEqual(connection.set, null, 'nothing set');
                assert.strictEqual(connection.ended, false);
                done();
            },
            () => assert.fail('nothing to report'),
        );
    });

    it('removes NO_BACKSLASH_ESCAPES from the session and reports the mode of the server', function (done) {
        const connection = fakeConnection('IGNORE_SPACE,no_backslash_escapes');
        let reported = null;
        checkSqlMode(
            connection,
            (err, conn) => {
                assert.ifError(err);
                assert.strictEqual(conn, connection);
                assert.strictEqual(connection.set, 'IGNORE_SPACE');
                assert.strictEqual(reported, 'IGNORE_SPACE,no_backslash_escapes');
                assert.strictEqual(connection.ended, false);
                done();
            },
            mode => (reported = mode),
        );
    });

    it('refuses and closes the connection if the mode cannot be removed, with an error that says why', function (done) {
        const connection = fakeConnection('NO_BACKSLASH_ESCAPES', new Error('Access denied'));
        checkSqlMode(connection, (err, conn) => {
            assert.match(String(err), /NO_BACKSLASH_ESCAPES/);
            assert.match(String(err), /Access denied/);
            assert.strictEqual(err.code, 'IOB_NO_BACKSLASH_ESCAPES');
            assert.strictEqual(conn, undefined);
            assert.strictEqual(connection.ended, true);
            done();
        });
    });

    it('passes the error of the check on and closes the connection', function (done) {
        const connection = fakeConnection(new Error('Lost connection'));
        checkSqlMode(connection, err => {
            assert.match(String(err), /Lost connection/);
            assert.strictEqual(connection.ended, true);
            done();
        });
    });
});
