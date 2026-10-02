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
