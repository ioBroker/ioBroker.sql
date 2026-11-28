const assert = require('node:assert');
const { EventEmitter } = require('node:events');
const { SQLClientPool } = require('../build/lib/sql-client-pool');
const { createAdapter } = require('./lib/stubAdapter');

// What the adapter does with its own connection limit (maxConnections, poolBorrowGuard) when the
// database cannot be reached, and how many connections the inserts use at once (#587).

/** A driver whose connects fail while `down` is set and take `delay` ms either way */
function fakeFactory(delay = 5) {
    const factory = {
        down: false,
        connects: 0,
        open: 0,
        maxOpen: 0,
        openConnection(_options, callback) {
            factory.connects++;
            setTimeout(() => {
                if (factory.down) {
                    callback(Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:3306'), { code: 'ECONNREFUSED' }));
                } else {
                    factory.open++;
                    factory.maxOpen = Math.max(factory.maxOpen, factory.open);
                    callback(null, new EventEmitter());
                }
            }, delay);
        },
        closeConnection(_connection, callback) {
            factory.open--;
            setImmediate(() => callback(null));
        },
        execute(_connection, _sql, callback) {
            setTimeout(() => callback(null, []), 2);
        },
    };
    return factory;
}

function setup(maxConnections, options, done) {
    const factory = fakeFactory();
    const pool = new SQLClientPool({}, {}, factory);
    const adapter = createAdapter();
    adapter.config.maxConnections = maxConnections;
    adapter.config.dbname = 'iobroker';
    adapter.setConnected = () => {};
    adapter.checkRetention = () => {};
    adapter.sqlFuncs = { insert: () => ['INSERT ...'] };
    pool.open({ max_idle: 10, ...options }, err => {
        assert.ifError(err);
        adapter.clientPool = pool;
        done(adapter, factory);
    });
}

function row(ts) {
    return { state: { val: ts, ts, ack: true, q: 0 }, from: 0, table: 'ts_number' };
}

describe('Test the connection limit while the database is not reachable (#587)', function () {
    let now;
    const realNow = Date.now;
    beforeEach(() => {
        now = realNow();
        Date.now = () => now;
    });
    afterEach(() => {
        Date.now = realNow;
    });

    it('a waiting borrower gets the error of a failed connect instead of waiting for ever', function (done) {
        setup(1, {}, (adapter, factory) => {
            factory.down = true;
            const results = [];
            adapter.borrowClientFromPool(err => results.push(['first', err]));
            // the only connection is being opened: this one has to wait
            adapter.borrowClientFromPool(err => results.push(['waiter', err]));
            assert.strictEqual(adapter.poolBorrowGuard.length, 1, 'precondition: one waiter');
            setTimeout(() => {
                assert.strictEqual(results.length, 2, 'both borrowers got an answer');
                const errors = Object.fromEntries(results);
                assert.strictEqual(errors.first.code, 'ECONNREFUSED');
                assert.strictEqual(errors.waiter.code, 'IOB_DB_UNREACHABLE');
                assert.strictEqual(adapter.poolBorrowGuard.length, 0);
                assert.strictEqual(adapter.activeConnections, 0);
                assert.strictEqual(factory.connects, 1, 'the waiter did not start a connect of its own');
                done();
            }, 100);
        });
    });

    it('after a failed connect, borrows fail at once; later one borrow at a time checks the server', function (done) {
        setup(100, {}, (adapter, factory) => {
            factory.down = true;
            adapter.borrowClientFromPool(err => {
                assert.ok(err);
                assert.strictEqual(factory.connects, 1);
                let fast = 0;
                for (let i = 0; i < 50; i++) {
                    adapter.borrowClientFromPool(err => {
                        assert.strictEqual(err.code, 'IOB_DB_UNREACHABLE');
                        assert.match(err.message, /Database not reachable: .*ECONNREFUSED/);
                        fast++;
                    });
                }
                assert.strictEqual(fast, 50, 'answered synchronously, without a connect');
                assert.strictEqual(factory.connects, 1);

                // the server is back, the retry delay is over
                factory.down = false;
                now += 5_000;
                adapter.borrowClientFromPool((err, client) => {
                    assert.ifError(err);
                    assert.ok(client);
                    // reachable again: borrows work normally
                    adapter.borrowClientFromPool((err, second) => {
                        assert.ifError(err);
                        adapter.returnClientToPool(client);
                        adapter.returnClientToPool(second);
                        assert.strictEqual(adapter.activeConnections, 0);
                        done();
                    });
                });
                // only the first borrow after the delay checks the server
                let other;
                adapter.borrowClientFromPool(err => (other = err));
                assert.strictEqual(other?.code, 'IOB_DB_UNREACHABLE');
                assert.strictEqual(factory.connects, 2);
            });
        });
    });

    it('keeps the rows of every write during the outage without an error line per write', function (done) {
        setup(100, {}, (adapter, factory) => {
            factory.down = true;
            adapter.multiRequests = true;
            const id = 'test.0.value';
            adapter.sqlDPs = { [id]: { index: 1, config: {}, list: [row(1)] } };
            adapter.storeCached(id, err => {
                assert.ok(err);
                // 20 more values, each with its own write (maxLength 0)
                let pending = 20;
                for (let i = 2; i <= 21; i++) {
                    adapter.sqlDPs[id].list.push(row(i));
                    adapter.storeCached(id, err => {
                        assert.ok(err);
                        if (!--pending) {
                            assert.strictEqual(factory.connects, 1, 'no connect per value');
                            assert.deepStrictEqual(
                                adapter.sqlDPs[id].list.map(r => r.state.ts),
                                Array.from({ length: 21 }, (_, i) => i + 1),
                                'every value is kept, in order',
                            );
                            const errors = adapter.logs.filter(l => l.startsWith('error:'));
                            assert.strictEqual(errors.length, 1, `one error line: ${errors.join(' | ')}`);
                            done();
                        }
                    });
                }
            });
        });
    });
});

describe('Test the inserts use a limited number of connections (#587)', function () {
    it('a flush of 200 datapoints opens at most 10 connections and writes every datapoint', function (done) {
        setup(100, {}, (adapter, factory) => {
            let pending = 200;
            for (let i = 0; i < 200; i++) {
                adapter._insertValuesIntoDB([row(i)], i, `test.0.dp${i}`, err => {
                    assert.ifError(err);
                    if (!--pending) {
                        assert.ok(factory.connects <= 10, `${factory.connects} connects`);
                        assert.strictEqual(adapter.activeConnections, 0);
                        assert.strictEqual(adapter.poolBorrowGuard.length, 0);
                        done();
                    }
                });
            }
        });
    });

    it('a lower maxConnections stays the limit', function (done) {
        setup(3, {}, (adapter, factory) => {
            let pending = 30;
            for (let i = 0; i < 30; i++) {
                adapter._insertValuesIntoDB([row(i)], i, `test.0.dp${i}`, err => {
                    assert.ifError(err);
                    if (!--pending) {
                        assert.ok(factory.connects <= 3, `${factory.connects} connects`);
                        done();
                    }
                });
            }
        });
    });

    it('stop: 600 datapoints are written over at most 10 connections', function (done) {
        setup(100, {}, (adapter, factory) => {
            adapter.config.writeNulls = false;
            adapter.multiRequests = true;
            for (let i = 0; i < 600; i++) {
                adapter.sqlDPs[`dp${i}`] = { index: i + 1, type: 0, list: [row(1000 + i)], inFlight: {}, config: {} };
            }
            let written = 0;
            const insert = adapter.sqlFuncs.insert;
            adapter.sqlFuncs.insert = (...args) => {
                written++;
                return insert(...args);
            };
            const start = Date.now();
            adapter.finish(() => {
                assert.strictEqual(written, 600, 'every datapoint was written');
                assert.ok(factory.maxOpen <= 10, `${factory.maxOpen} connections open at once`);
                assert.ok(Date.now() - start < 5000, `${Date.now() - start} ms`);
                done();
            });
        });
    });

    it('reads are not limited by the inserts', function (done) {
        setup(100, {}, (adapter, factory) => {
            const clients = [];
            for (let i = 0; i < 15; i++) {
                adapter.borrowClientFromPool((err, client) => {
                    assert.ifError(err);
                    clients.push(client);
                    if (clients.length === 15) {
                        assert.strictEqual(adapter.poolBorrowGuard.length, 0, 'nobody waits');
                        clients.forEach(c => adapter.returnClientToPool(c));
                        assert.ok(factory.connects >= 15);
                        done();
                    }
                });
            }
        });
    });
});
