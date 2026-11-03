const assert = require('node:assert');

// Imported per dialect, never from build/main: that would pull in @iobroker/adapter-core, which
// calls process.exit(10) when js-controller cannot be resolved.
const DIALECTS = {
    mysql: require('../build/lib/mysql'),
    postgresql: require('../build/lib/postgresql'),
    mssql: require('../build/lib/mssql'),
    sqlite: require('../build/lib/sqlite'),
};

// A statement that carries a second one after a semicolon. The trailing semicolon of the statement
// itself is not a match - only a semicolon with another statement behind it.
const CONCATENATED = /;\s*(INSERT|SELECT|UPDATE|DELETE)/i;

function rows() {
    // The batch from issue #294: a counter datapoint produces rows in ts_counter *and* ts_number
    // within the same flush, which is what used to end up in one query.
    return [
        { table: 'ts_counter', state: { val: 53.32, ts: 1676135872618 }, from: 2 },
        { table: 'ts_counter', state: { val: 53, ts: 1676138387812 }, from: 2 },
        { table: 'ts_number', state: { val: 53, ts: 1676138387812, ack: true }, from: 2 },
        { table: 'ts_counter', state: { val: 52.95, ts: 1676138670902 }, from: 2 },
        { table: 'ts_number', state: { val: 52.95, ts: 1676138670902, ack: true }, from: 2 },
    ];
}

describe('Test insert() never concatenates statements', function () {
    for (const [dialect, sql] of Object.entries(DIALECTS)) {
        it(`${dialect}: emits one statement per table instead of one joined query`, function () {
            const queries = sql.insert('iobroker', 5, rows());

            assert.ok(Array.isArray(queries), 'insert() returns a list of statements');
            assert.strictEqual(queries.length, 2, `ts_counter and ts_number, got ${queries.length}`);

            // This is issue #294: the adapter used to send
            //   INSERT INTO ...ts_counter ...;INSERT INTO ...ts_number ...;
            // as a single query, which MariaDB rejects with a syntax error because no driver here
            // is configured for multi-statement batches.
            for (const query of queries) {
                assert.ok(!CONCATENATED.test(query), `carries a second statement: ${query}`);
                assert.strictEqual((query.match(/INSERT/gi) || []).length, 1, `more than one INSERT in: ${query}`);
            }

            const tables = queries.map(q => (q.match(/ts_\w+/) || [])[0]);
            assert.deepStrictEqual([...tables].sort(), ['ts_counter', 'ts_number']);
        });

        it(`${dialect}: keeps the statements apart for every table`, function () {
            const queries = sql.insert('iobroker', 5, [
                { table: 'ts_number', state: { val: 1, ts: 1, ack: true }, from: 2 },
                { table: 'ts_string', state: { val: 'x', ts: 2, ack: true }, from: 2 },
                { table: 'ts_bool', state: { val: true, ts: 3, ack: true }, from: 2 },
                { table: 'ts_counter', state: { val: 4, ts: 4 }, from: 2 },
            ]);

            assert.strictEqual(queries.length, 4, 'one per table');
            for (const query of queries) {
                assert.ok(!CONCATENATED.test(query), `carries a second statement: ${query}`);
            }
        });

        it(`${dialect}: chunks a large batch without joining the chunks`, function () {
            const many = [];
            for (let i = 0; i < 1200; i++) {
                many.push({ table: 'ts_number', state: { val: i, ts: 1700000000000 + i, ack: true }, from: 2 });
            }

            const queries = sql.insert('iobroker', 5, many);

            // 500 rows per statement, so the chunks must stay separate queries rather than being
            // glued back together with semicolons.
            assert.strictEqual(queries.length, 3, `expected 500/500/200, got ${queries.length} chunks`);
            for (const query of queries) {
                assert.ok(!CONCATENATED.test(query), `carries a second statement: ${query}`);
                assert.strictEqual((query.match(/INSERT/gi) || []).length, 1);
            }

            const tuples = queries.map(q => (q.match(/\),\(/g) || []).length + 1);
            assert.deepStrictEqual(tuples, [500, 500, 200]);
        });
    }
});

// Issue #304: two rows with the same (id, ts) in one batch - the same datapoint logged from two
// sources that happened to land on the same millisecond - used to abort the whole INSERT with a
// primary key violation.
const DUPLICATE_GUARD = {
    mysql: /ON DUPLICATE KEY UPDATE/i,
    postgresql: /ON CONFLICT DO NOTHING/i,
    sqlite: /ON CONFLICT DO NOTHING/i,
    // No guard, and that is correct: init() creates `CREATE INDEX i_id on ...(id, ts)`, a plain
    // non-unique index rather than a primary key, so no uniqueness conflict can arise. Adding one
    // here to make the dialects look alike would be wrong.
    mssql: null,
};

/** The batch from issue #304: ts 1681102802872 appears twice for id 4, from two different sources */
function duplicateBatch() {
    return [
        { table: 'ts_number', state: { val: 54.7, ts: 1681102502871, ack: true }, from: 2 },
        { table: 'ts_number', state: { val: 54.7, ts: 1681102802872, ack: true }, from: 2 },
        { table: 'ts_number', state: { val: 54.7, ts: 1681102502853, ack: true }, from: 3 },
        { table: 'ts_number', state: { val: 54.9, ts: 1681102802872, ack: true }, from: 3 },
    ];
}

describe('Test insert() suppresses duplicate (id, ts) rows (#304)', function () {
    for (const [dialect, sql] of Object.entries(DIALECTS)) {
        const guard = DUPLICATE_GUARD[dialect];

        it(`${dialect}: ${guard ? 'guards the keyed tables' : 'needs no guard, its index is not unique'}`, function () {
            for (const table of ['ts_number', 'ts_string', 'ts_bool']) {
                const [query] = sql.insert('iobroker', 4, [
                    { table, state: { val: table === 'ts_string' ? 'a' : 1, ts: 100, ack: true }, from: 2 },
                ]);

                if (guard) {
                    assert.ok(guard.test(query), `${table} is unguarded: ${query}`);
                } else {
                    assert.ok(
                        !/ON DUPLICATE KEY|ON CONFLICT/i.test(query),
                        `${table} grew a guard; check whether init() still creates a non-unique index: ${query}`,
                    );
                }
            }
        });

        if (dialect === 'postgresql' || dialect === 'sqlite') {
            it(`${dialect}: uses DO NOTHING, not DO UPDATE`, function () {
                const [query] = sql.insert('iobroker', 4, duplicateBatch());

                // The duplicate sits *inside* one statement. DO NOTHING copes with that; turning it
                // into DO UPDATE - the obvious way to let the last value win - makes PostgreSQL
                // raise "cannot affect row a second time" and brings the bug straight back.
                assert.ok(!/DO UPDATE/i.test(query), `DO UPDATE reintroduces #304: ${query}`);
            });
        }
    }

    it('sqlite: the reported batch really inserts, against a live schema', function (done) {
        const sqlite3 = require('sqlite3');
        const db = new sqlite3.Database(':memory:');

        db.serialize(() => {
            // the real DDL from init()
            db.run(
                'CREATE TABLE ts_number (id INTEGER, ts BIGINT, val REAL, ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts))',
            );

            const [query] = DIALECTS.sqlite.insert('ignored', 4, duplicateBatch());
            db.run(query, function (err) {
                assert.ifError(err);

                db.all('SELECT ts, val FROM ts_number ORDER BY ts', function (err, rows) {
                    assert.ifError(err);
                    // four input rows, three distinct timestamps - the duplicate is dropped
                    assert.strictEqual(rows.length, 3, JSON.stringify(rows));
                    assert.strictEqual(rows[2].val, 54.7, 'the first value for a timestamp wins');
                    db.close(done);
                });
            });
        });
    });
});

describe("Test insert()/update() leave the caller's state untouched", function () {
    // pushValueIntoDB() hands over the same object that pushHistory() keeps as sqlDPs[id].state, the
    // reference value of "record changes only" (#295). When insert() wrote the SQL literal ('on' with
    // quotes) back into it, the next equal value was compared against "'on'" and stored again.
    for (const [dialect, sql] of Object.entries(DIALECTS)) {
        it(`${dialect}: insert() does not change state.val`, function () {
            const string = { val: 'on', ts: 1, ack: true };
            const bool = { val: true, ts: 2, ack: true };
            const empty = { val: null, ts: 3, ack: true };
            const query = sql.insert('iobroker', 5, [
                { table: 'ts_string', state: string, from: 2 },
                { table: 'ts_bool', state: bool, from: 2 },
                { table: 'ts_number', state: empty, from: 2 },
            ]);

            assert.strictEqual(string.val, 'on');
            assert.strictEqual(bool.val, true);
            assert.strictEqual(empty.val, null);
            // the statements themselves still carry the literals
            assert.ok(
                query.some(q => q.includes("'on'")),
                query.join('\n'),
            );
            assert.ok(
                query.some(q => q.includes('NULL')),
                query.join('\n'),
            );
        });

        it(`${dialect}: update() does not change state.val`, function () {
            const state = { val: 'on', ts: 1 };
            const query = sql.update('iobroker', 5, state, 2, 'ts_string');
            assert.strictEqual(state.val, 'on');
            assert.ok(query.includes("'on'"), query);
        });
    }
});

describe('Test destroy() keeps a database the adapter did not create', function () {
    for (const [dialect, sql] of Object.entries(DIALECTS)) {
        it(`${dialect}: "Do not create database" drops only the adapter tables`, function () {
            const queries = sql.destroy('iobroker', true);
            assert.ok(!queries.some(q => /DROP DATABASE/i.test(q)), queries.join('\n'));
            // server-level statements need rights a user of an existing database usually lacks
            assert.ok(!queries.some(q => /DBCC/i.test(q)), queries.join('\n'));
            for (const table of ['ts_counter', 'ts_number', 'ts_string', 'ts_bool', 'sources', 'datapoints']) {
                assert.ok(
                    queries.some(q => new RegExp(`DROP TABLE .*\\b${table};`).test(q)),
                    `${table} missing in: ${queries.join('\n')}`,
                );
            }
        });
    }

    for (const dialect of ['mysql', 'mssql']) {
        it(`${dialect}: without the option the database is still dropped`, function () {
            assert.ok(DIALECTS[dialect].destroy('iobroker').some(q => /DROP DATABASE/i.test(q)));
        });
    }
});

describe('Test the adapter keeps the reference value and survives a disabled datapoint', function () {
    const { createAdapter } = require('./lib/stubAdapter');

    it('pushHelper() converts a copy, not the reference value of "Record changes only"', function () {
        const adapter = createAdapter();
        const reference = { val: 'true', ts: 1, ack: true };
        adapter.sqlDPs.s = { state: reference, config: { storageType: false } };
        let pushed;
        adapter.pushValueIntoDB = (_id, state) => (pushed = state);

        adapter.pushHelper('s');

        assert.strictEqual(pushed.val, true, 'the converted value is written');
        assert.strictEqual(reference.val, 'true', 'the reference value is unchanged');
        assert.strictEqual(adapter.sqlDPs.s.state, reference);
    });

    it('getId() answers when the datapoint is disabled while its row is created', function (done) {
        const adapter = createAdapter();
        adapter.config.dbname = 'iobroker';
        adapter.sqlFuncs = require('../build/lib/mysql');
        adapter.sqlDPs['my.id'] = { config: {}, type: null, index: null };
        const answers = [[], [], [{ id: 7, type: 1 }]]; // select: none, insert, select: the new row
        const client = {
            execute(sql, callback) {
                const rows = answers.shift();
                if (/^INSERT/i.test(sql)) {
                    delete adapter.sqlDPs['my.id']; // disabled meanwhile
                }
                setImmediate(() => callback(null, rows));
            },
        };
        adapter.borrowClientFromPool = callback => callback(null, client);
        adapter.returnClientToPool = () => {};

        adapter.getId('my.id', 1, err => {
            assert.match(String(err), /no longer active/);
            done();
        });
    });
});

describe('Test a failed write keeps the values when the database was not reachable (#587)', function () {
    const { createAdapter } = require('./lib/stubAdapter');
    const row = (table, ts) => ({ table, state: { val: table === 'ts_string' ? 'x' : ts, ts, ack: true }, from: 0 });

    /** An adapter whose datapoint `dp` buffers `rows` and whose statements fail as `failAt` says */
    function setup(rows, failAt, error) {
        const adapter = createAdapter();
        adapter.config.dbname = 'iobroker';
        adapter.sqlFuncs = require('../build/lib/sqlite');
        adapter.sqlDPs.dp = { index: 7, type: 0, list: rows, inFlight: {}, config: {} };
        adapter.executed = [];
        const client = {
            execute(sql, callback) {
                const n = adapter.executed.push(sql);
                setImmediate(() => (n === failAt ? callback(error) : callback(null, [])));
            },
        };
        adapter.borrowClientFromPool = cb => cb(null, client);
        adapter.returnClientToPool = () => {};
        adapter.checkRetention = () => {};
        return adapter;
    }

    const lost = () =>
        Object.assign(new Error('Connection lost: The server closed the connection.'), {
            code: 'PROTOCOL_CONNECTION_LOST',
            fatal: true,
        });

    it('keeps the rows of the failed statement and all after it, and passes the error on', function (done) {
        const adapter = setup(
            [row('ts_number', 1), row('ts_string', 2), row('ts_number', 3), row('ts_bool', 4)],
            2,
            lost(),
        );
        adapter.storeCached('dp', err => {
            assert.match(String(err), /Connection lost/, 'the error reaches the caller (storeState)');
            // ts_number (1, 3) was written, ts_string failed, ts_bool never ran
            assert.strictEqual(adapter.executed.length, 2);
            assert.deepStrictEqual(
                adapter.sqlDPs.dp.list.map(r => r.state.ts),
                [2, 4],
            );
            assert.ok(
                adapter.logs.some(l => /kept for the next write/.test(l)),
                adapter.logs.join('\n'),
            );
            done();
        });
    });

    it('keeps everything when no connection could be borrowed', function (done) {
        const adapter = setup([row('ts_number', 1), row('ts_number', 2)], 0);
        adapter.borrowClientFromPool = cb =>
            cb(Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:3306'), { code: 'ECONNREFUSED' }));
        adapter.storeCached('dp', err => {
            assert.ok(err);
            assert.deepStrictEqual(
                adapter.sqlDPs.dp.list.map(r => r.state.ts),
                [1, 2],
            );
            done();
        });
    });

    it('puts the kept rows before the values logged in the meantime', function (done) {
        const adapter = setup([row('ts_number', 1)], 1, lost());
        const storeCached = adapter.storeCached.bind(adapter);
        storeCached('dp', () => {
            assert.deepStrictEqual(
                adapter.sqlDPs.dp.list.map(r => r.state.ts),
                [1, 2],
            );
            done();
        });
        // logged while the write is running
        adapter.sqlDPs.dp.list.push(row('ts_number', 2));
    });

    it('works the same through the task queue (SQLite)', function (done) {
        const adapter = setup([row('ts_number', 1), row('ts_string', 2)], 2, lost());
        adapter.multiRequests = false;
        adapter.storeCached('dp', err => {
            assert.ok(err);
            assert.deepStrictEqual(
                adapter.sqlDPs.dp.list.map(r => r.state.ts),
                [2],
            );
            setImmediate(() => {
                assert.strictEqual(adapter.tasks.length, 0, 'the task is done');
                done();
            });
        });
    });

    it('does not keep the rows of a datapoint that is being disabled or when the adapter stops', function (done) {
        const adapter = setup([row('ts_number', 1)], 1, lost());
        adapter.sqlDPs.dp.config.enabled = false;
        adapter.storeCached('dp', () => {
            assert.deepStrictEqual(adapter.sqlDPs.dp.list, []);
            assert.ok(
                adapter.logs.some(l => /were not stored/.test(l)),
                adapter.logs.join('\n'),
            );

            const stopping = setup([row('ts_number', 1)], 1, lost());
            stopping.finished = [() => {}];
            stopping.storeCached('dp', () => {
                assert.deepStrictEqual(stopping.sqlDPs.dp.list, []);
                done();
            });
        });
    });

    it('keeps the rows when the task queue is full', function (done) {
        const adapter = setup([row('ts_number', 1)], 0);
        adapter.multiRequests = false;
        for (let i = 0; i <= 100; i++) {
            adapter.tasks.push({ operation: 'query', query: 'SELECT 1', id: 'x' });
        }
        adapter.lockTasks = true; // the queue is busy
        adapter.storeCached('dp', err => {
            assert.match(String(err), /Cannot queue/);
            assert.deepStrictEqual(
                adapter.sqlDPs.dp.list.map(r => r.state.ts),
                [1],
            );
            done();
        });
    });

    it('does not keep the rows on an SQL error - they would fail again', function (done) {
        const sqlError = Object.assign(new Error('SQLITE_ERROR: no such table: ts_number'), { code: 'SQLITE_ERROR' });
        const adapter = setup([row('ts_number', 1), row('ts_number', 2)], 1, sqlError);
        adapter.storeCached('dp', err => {
            assert.match(String(err), /no such table/);
            assert.deepStrictEqual(adapter.sqlDPs.dp.list, []);
            done();
        });
    });
});
