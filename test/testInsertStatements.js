const assert = require('node:assert');

// Imported per dialect, never from build/main: that would pull in @iobroker/adapter-core, which
// calls process.exit(10) when js-controller cannot be resolved.
const DIALECTS = {
    mysql: require('../build/lib/mysql'),
    postgresql: require('../build/lib/postgresql'),
    mssql: require('../build/lib/mssql'),
    sqlite: require('../build/lib/sqlite'),
};

// The statement as the server receives it: for MySQL, mysql2 escapes the bound values into the text
const { format } = require('mysql2');
const text = q => (typeof q === 'string' ? q : format(q.sql, q.params));
const texts = list => list.map(text);

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
            const queries = texts(sql.insert('iobroker', 5, rows()));

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
            const queries = texts(
                sql.insert('iobroker', 5, [
                    { table: 'ts_number', state: { val: 1, ts: 1, ack: true }, from: 2 },
                    { table: 'ts_string', state: { val: 'x', ts: 2, ack: true }, from: 2 },
                    { table: 'ts_bool', state: { val: true, ts: 3, ack: true }, from: 2 },
                    { table: 'ts_counter', state: { val: 4, ts: 4 }, from: 2 },
                ]),
            );

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

            const queries = texts(sql.insert('iobroker', 5, many));

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
                const [query] = texts(
                    sql.insert('iobroker', 4, [
                        { table, state: { val: table === 'ts_string' ? 'a' : 1, ts: 100, ack: true }, from: 2 },
                    ]),
                );

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
            const query = texts(
                sql.insert('iobroker', 5, [
                    { table: 'ts_string', state: string, from: 2 },
                    { table: 'ts_bool', state: bool, from: 2 },
                    { table: 'ts_number', state: empty, from: 2 },
                ]),
            );

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
            const query = text(sql.update('iobroker', 5, state, 2, 'ts_string'));
            assert.strictEqual(state.val, 'on');
            assert.ok(query.includes("'on'"), query);
        });
    }
});

describe('Test getHistory() with `from` returns values stored without a source', function () {
    // A value stored without `from` has `_from` = 0, and `sources` has no row with id 0. With an
    // INNER JOIN such values were missing from every query with `from: true`, e.g. the history
    // data in admin.
    const options = { index: 5, from: true, ack: true, start: 50, end: 300 };

    for (const [dialect, sql] of Object.entries(DIALECTS)) {
        it(`${dialect}: joins sources with an outer join`, function () {
            // MySQL returns `{ sql, params }`, the other dialects the SQL text
            const statement = sql.getHistory('iobroker', 'ts_number', options);
            const query = typeof statement === 'string' ? statement : statement.sql;
            assert.ok(!/INNER JOIN/i.test(query), query);
            assert.strictEqual(
                (query.match(/LEFT (OUTER )?JOIN/gi) || []).length,
                3,
                `the query and both sub-queries: ${query}`,
            );
        });
    }

    it('sqlite: a value without a source is returned, against a live schema', function (done) {
        const sqlite3 = require('sqlite3');
        const db = new sqlite3.Database(':memory:');

        db.serialize(() => {
            for (const statement of DIALECTS.sqlite.init('ignored')) {
                db.run(statement);
            }
            db.run("INSERT INTO sources (id, name) VALUES (1, 'system.adapter.javascript.0')");
            db.run('INSERT INTO ts_number (id, ts, val, ack, _from, q) VALUES (5, 100, 1.5, 1, 1, 0)');
            db.run('INSERT INTO ts_number (id, ts, val, ack, _from, q) VALUES (5, 200, 2.5, 1, 0, 0)');

            db.all(DIALECTS.sqlite.getHistory('ignored', 'ts_number', options), function (err, rows) {
                assert.ifError(err);
                assert.deepStrictEqual(
                    rows.map(r => [r.ts, r.from]),
                    [
                        [100, 'system.adapter.javascript.0'],
                        [200, null],
                    ],
                );
                db.close(done);
            });
        });
    });
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
                if (/^INSERT/i.test(sql.sql ?? sql)) {
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

describe('Test getId()/getFrom() do not share the row of an ID that differs only in case (#588)', function () {
    const { createAdapter } = require('./lib/stubAdapter');
    const licht = { id: 3, name: 'alias.0.Licht', type: 2 };

    function mysqlAdapter() {
        const adapter = createAdapter();
        adapter.config.dbname = 'iobroker';
        adapter.sqlFuncs = require('../build/lib/mysql');
        return adapter;
    }

    /** Answers the statements in order, like a server with a case-insensitive collation, and records them */
    function answer(adapter, answers) {
        const statements = [];
        const client = {
            execute(statement, callback) {
                statements.push(statement.sql ?? statement);
                const rows = answers.shift();
                setImmediate(() => callback(null, rows));
            },
        };
        adapter.borrowClientFromPool = callback => callback(null, client);
        adapter.returnClientToPool = () => {};
        return statements;
    }

    it('getId() creates a row of its own instead of taking the one of alias.0.Licht', function (done) {
        const adapter = mysqlAdapter();
        adapter.sqlDPs['alias.0.licht'] = { config: {}, type: null, index: null };
        // the lookup finds alias.0.Licht, before and after the INSERT
        const statements = answer(adapter, [[licht], [], [licht, { id: 9, name: 'alias.0.licht', type: 0 }]]);

        adapter.getId('alias.0.licht', 0, err => {
            assert.strictEqual(err, null);
            assert.match(statements[1], /^INSERT/);
            assert.strictEqual(adapter.sqlDPs['alias.0.licht'].index, 9);
            assert.strictEqual(adapter.sqlDPs['alias.0.licht'].type, 0, 'not the boolean type of alias.0.Licht');
            done();
        });
    });

    it('getId() for reading finds nothing instead of the series of alias.0.Licht', function (done) {
        const adapter = mysqlAdapter();
        adapter.sqlDPs['alias.0.licht'] = { config: {}, type: null, index: null };
        const statements = answer(adapter, [[licht]]);

        adapter.getId('alias.0.licht', null, err => {
            assert.match(String(err), /id not found/);
            assert.strictEqual(statements.length, 1, 'nothing inserted');
            assert.strictEqual(adapter.sqlDPs['alias.0.licht'].index, null);
            done();
        });
    });

    it('getId() still takes a row whose name the database could not store exactly', function (done) {
        const adapter = mysqlAdapter();
        adapter.sqlDPs['alias.0.Кухня'] = { config: {}, type: null, index: null };
        // MS SQL `varchar` returns characters outside its code page as `?`
        const statements = answer(adapter, [[{ id: 4, name: 'alias.0.?????', type: 0 }]]);

        adapter.getId('alias.0.Кухня', 0, err => {
            assert.strictEqual(err, null);
            assert.strictEqual(statements.length, 1, 'no second row');
            assert.strictEqual(adapter.sqlDPs['alias.0.Кухня'].index, 4);
            done();
        });
    });

    it('getFrom() creates a source of its own as well', function (done) {
        const adapter = mysqlAdapter();
        const other = { id: 2, name: 'system.adapter.Javascript.0' };
        const statements = answer(adapter, [[other], [], [other, { id: 5, name: 'system.adapter.javascript.0' }]]);

        adapter.getFrom('system.adapter.javascript.0', err => {
            assert.ok(!err, String(err));
            assert.match(statements[0], /^SELECT id, name FROM/, 'the lookup returns the name to compare');
            assert.match(statements[1], /^INSERT/);
            assert.strictEqual(adapter.from['system.adapter.javascript.0'], 5);
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
            assert.match(String(err), /2 values are kept and written with the next write, do not send them again/);
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

    it('does not keep the rows on an SQL error - they would fail again', function (done) {
        const sqlError = Object.assign(new Error('SQLITE_ERROR: no such table: ts_number'), { code: 'SQLITE_ERROR' });
        const adapter = setup([row('ts_number', 1), row('ts_number', 2)], 1, sqlError);
        adapter.storeCached('dp', err => {
            assert.match(String(err), /no such table/);
            assert.doesNotMatch(String(err), /kept/, 'nothing is kept, so the caller must not be told so');
            assert.deepStrictEqual(adapter.sqlDPs.dp.list, []);
            done();
        });
    });

    it('keeps at most MAX_KEPT_ROWS of all datapoints together, with one warning for all of them', function (done) {
        this.timeout(20000);
        const { MAX_KEPT_ROWS, MAX_REQUEUED_ROWS } = require('../build/lib/write-buffer');
        const adapter = createAdapter();
        adapter.config.dbname = 'iobroker';
        adapter.sqlFuncs = require('../build/lib/sqlite');
        adapter.checkRetention = () => {};
        adapter.returnClientToPool = () => {};
        let reachable = false;
        adapter.borrowClientFromPool = cb =>
            cb(null, {
                execute: (sql, callback) => setImmediate(() => (reachable ? callback(null, []) : callback(lost()))),
            });
        const fullRows = () => Array.from({ length: MAX_REQUEUED_ROWS }, (_, i) => row('ts_number', i + 1));

        // these datapoints use up the total at the limit per datapoint, and one more comes after them
        const full = MAX_KEPT_ROWS / MAX_REQUEUED_ROWS;
        const ids = [];
        for (let i = 0; i <= full; i++) {
            ids.push(`dp${i}`);
            adapter.sqlDPs[`dp${i}`] = { index: i + 1, type: 0, list: fullRows(), inFlight: {}, config: {} };
        }
        const last = ids[full];

        const errors = {};
        const writeAll = (i, cb) =>
            i >= ids.length
                ? cb()
                : adapter.storeCached(ids[i], err => {
                      errors[ids[i]] = err;
                      writeAll(i + 1, cb);
                  });

        writeAll(0, () => {
            assert.strictEqual(adapter.sqlDPs.dp0.list.length, MAX_REQUEUED_ROWS, 'the first ones keep all rows');
            assert.strictEqual(adapter.sqlDPs[ids[full - 1]].list.length, MAX_REQUEUED_ROWS);
            assert.strictEqual(adapter.sqlDPs[last].list.length, 0, 'the total is used up');
            assert.match(String(errors.dp0), /kept and written with the next write/);
            assert.doesNotMatch(String(errors[last]), /kept/, 'its values are dropped, not kept');

            const warnings = adapter.logs.filter(l => /kept for the next write/.test(l));
            assert.strictEqual(warnings.length, 1, `one warning for all datapoints:\n${warnings.join('\n')}`);

            // dp0 is written now, so its rows no longer count and the last one can keep its rows
            reachable = true;
            adapter.storeCached('dp0', err => {
                assert.ifError(err);
                assert.ok(
                    adapter.logs.some(l =>
                        /While the database was not reachable, 2000 older values were dropped/.test(l),
                    ),
                    adapter.logs.join('\n'),
                );
                reachable = false;
                adapter.sqlDPs[last].list = fullRows();
                adapter.storeCached(last, err => {
                    assert.match(String(err), /2000 values are kept/);
                    assert.strictEqual(adapter.sqlDPs[last].list.length, MAX_REQUEUED_ROWS);
                    done();
                });
            });
        });
    });
});

describe('Test flushing many datapoints (#587, C5)', function () {
    const { createAdapter } = require('./lib/stubAdapter');
    const row = ts => ({ table: 'ts_number', state: { val: ts, ts, ack: true }, from: 0 });

    function adapterWith(count) {
        const adapter = createAdapter();
        adapter.config.dbname = 'iobroker';
        adapter.sqlFuncs = require('../build/lib/sqlite');
        for (let i = 0; i < count; i++) {
            adapter.sqlDPs[`dp${i}`] = { index: i + 1, type: 0, list: [row(1000 + i)], inFlight: {}, config: {} };
        }
        return adapter;
    }

    it('SQLite: the task queue takes the buffers of more than 100 datapoints', function (done) {
        const adapter = adapterWith(250);
        adapter.multiRequests = false;
        const written = [];
        adapter._insertValuesIntoDB = (list, index, id, cb) => {
            written.push(id);
            setImmediate(() => cb());
        };
        adapter.storeCached(undefined, err => {
            assert.ifError(err);
        });
        const check = () => {
            if (written.length < 250 && adapter.tasks.length) {
                return setTimeout(check, 20);
            }
            assert.strictEqual(written.length, 250, 'every datapoint was written');
            assert.ok(!adapter.logs.some(l => /Cannot queue/.test(l)), adapter.logs.join('\n'));
            done();
        };
        check();
    });

    it('SQLite: a datapoint gets one queued insert, further rows join it', function () {
        const adapter = adapterWith(1);
        adapter.multiRequests = false;
        adapter.lockTasks = true; // something else is being written
        adapter.tasks.push({ operation: 'query', query: 'SELECT 1', id: 'x' });
        const results = [];
        adapter.pushValuesIntoDB('dp0', [row(1)], (err, unwritten) => results.push(['a', err, unwritten?.length]));
        adapter.pushValuesIntoDB('dp0', [row(2)], (err, unwritten) => results.push(['b', err, unwritten?.length]));
        assert.strictEqual(adapter.tasks.length, 2, 'one query, one insert');
        assert.strictEqual(adapter.tasks[1].list.length, 2);
        // the first callback gets the unwritten rows of both, the second only the error
        const error = new Error('lost');
        adapter.tasks[1].callback(error, adapter.tasks[1].list);
        assert.deepStrictEqual(results, [
            ['a', error, 2],
            ['b', error, undefined],
        ]);
    });

    it('SQLite: every flush merged into one insert tells storeState that its values are kept', function () {
        const adapter = adapterWith(1);
        adapter.multiRequests = false;
        adapter.lockTasks = true; // something else is being written
        adapter.tasks.push({ operation: 'query', query: 'SELECT 1', id: 'x' });
        const errors = [];
        adapter.storeCached('dp0', err => errors.push(err));
        adapter.sqlDPs.dp0.list.push(row(2));
        adapter.storeCached('dp0', err => errors.push(err));
        assert.strictEqual(adapter.tasks.length, 2, 'both flushes are one insert');

        const lost = Object.assign(new Error('Connection lost'), { code: 'PROTOCOL_CONNECTION_LOST', fatal: true });
        adapter.tasks[1].callback(lost, adapter.tasks[1].list);
        assert.strictEqual(errors.length, 2);
        for (const err of errors) {
            assert.match(String(err), /kept and written with the next write, do not send them again/);
        }
        assert.deepStrictEqual(
            adapter.sqlDPs.dp0.list.map(r => r.state.ts),
            [1000, 2],
            'the rows of both flushes are kept once',
        );
    });

    it('SQLite: rows that arrive after a delete message are not written before it', function () {
        const adapter = adapterWith(1);
        adapter.multiRequests = false;
        adapter.lockTasks = true; // something else is being written
        adapter.tasks.push({ operation: 'query', query: 'SELECT 1', id: 'x' });
        adapter.pushValuesIntoDB('dp0', [row(1)]);
        // e.g. deleteAll from the data browser, while the insert above still waits
        adapter.tasks.push({ operation: 'query', query: 'DELETE FROM ts_number WHERE id=1', id: 'dp0' });
        adapter.pushValuesIntoDB('dp0', [row(2)]);
        assert.deepStrictEqual(
            adapter.tasks.map(t => `${t.operation}:${t.list?.length ?? ''}`),
            ['query:', 'insert:1', 'query:', 'insert:1'],
            'the second rows come after the delete',
        );

        // a read in between changes nothing, the rows still join the waiting insert
        adapter.tasks.push({ operation: 'select', query: 'SELECT 2', options: {}, callback: () => {} });
        adapter.pushValuesIntoDB('dp0', [row(3)]);
        assert.strictEqual(adapter.tasks.length, 5);
        assert.strictEqual(adapter.tasks[3].list.length, 2);
    });

    it('stop: waits for a flush that is still running before closing the pool', function (done) {
        const adapter = adapterWith(200);
        adapter.multiRequests = false;
        adapter.config.writeNulls = false;
        let closedAfter = -1;
        const written = [];
        adapter.clientPool = { close: () => (closedAfter = written.length) };
        adapter.setConnected = () => {};
        adapter._insertValuesIntoDB = (list, index, id, cb) => {
            if (closedAfter >= 0) {
                return cb(new Error('No database connection'));
            }
            written.push(id);
            setImmediate(() => cb());
        };
        adapter.storeCached(); // e.g. the 10-minute flush: the rows are in flight now
        adapter.finish(() => {
            assert.strictEqual(written.length, 200, 'every in-flight row was written');
            assert.strictEqual(closedAfter, 200, 'the pool was closed only afterwards');
            done();
        });
    });

    it('stop: all datapoints are flushed at once, not 1 s per 50', function (done) {
        const adapter = adapterWith(600);
        adapter.config.writeNulls = false;
        const flushed = new Set();
        adapter.pushValuesIntoDB = (id, list, cb) => {
            flushed.add(id);
            setImmediate(() => cb());
        };
        adapter.finish(() => {
            assert.strictEqual(flushed.size, 600);
            done();
        });
        // before: the last of 600 datapoints was dispatched after 12 s, beyond the 10 s stopTimeout
        setTimeout(() => assert.strictEqual(flushed.size, 600, 'all dispatched within 50 ms'), 50);
    });
});

describe('Test MySQL statements carry their values as bound parameters (#587)', function () {
    const MySQL = DIALECTS.mysql;
    const NASTY = "it's\\";

    it('no value ends up in the SQL text', function () {
        const statements = [
            ...MySQL.insert('iobroker', 5, [{ table: 'ts_string', state: { val: NASTY, ts: 1, ack: true }, from: 2 }]),
            MySQL.update('iobroker', 5, { val: NASTY, ts: 1 }, 2, 'ts_string'),
            MySQL.getIdSelect('iobroker', NASTY),
            MySQL.getIdInsert('iobroker', NASTY, 1),
            MySQL.getFromSelect('iobroker', NASTY),
            MySQL.getFromInsert('iobroker', NASTY),
        ];
        for (const statement of statements) {
            assert.ok(!statement.sql.includes(NASTY), statement.sql);
            assert.ok(statement.params.includes(NASTY), JSON.stringify(statement.params));
            assert.strictEqual(
                (statement.sql.match(/\?/g) || []).length,
                statement.params.length,
                `placeholders and values match: ${statement.sql}`,
            );
        }
    });

    it('the quote is kept and the backslash is escaped, so the batch stays valid', function () {
        const [statement] = MySQL.insert('iobroker', 5, [
            { table: 'ts_string', state: { val: NASTY, ts: 1, ack: true }, from: 2 },
        ]);
        // before: VALUES (5, 1, 'its\', 1, 2, 0) - the quote removed and the string never closed
        assert.ok(text(statement).includes("'it\\'s\\\\'"), text(statement));
    });

    it('placeholders and values match for history and counter queries', function () {
        const statements = [
            MySQL.getHistory('iobroker', 'ts_number', { index: 3, start: 10, end: 20, from: true, ack: true, q: true }),
            MySQL.getHistory('iobroker', 'ts_number', { index: null, end: 20, count: 5 }),
            MySQL.getCounterDiff('iobroker', { index: 3, start: 10, end: 20 }),
            MySQL.deleteFromTable('iobroker', 'ts_number', 3, 10, 20),
            MySQL.getRawEntries('iobroker', 'ts_number', 3, { start: 1, end: 2, limit: 10, offset: 0, sort: 'asc' }),
            MySQL.getRawEntriesCount('iobroker', 'ts_number', 3, { start: 1 }),
            MySQL.retention('iobroker', 3, 'ts_number', 60),
        ];
        for (const statement of statements) {
            assert.strictEqual(
                (statement.sql.match(/\?/g) || []).length,
                statement.params.length,
                `placeholders and values match: ${statement.sql} ${JSON.stringify(statement.params)}`,
            );
        }
    });

    it('LIMIT and OFFSET stay numbers in the text', function () {
        const statement = MySQL.getRawEntries('iobroker', 'ts_number', 3, {
            limit: '10; DROP TABLE x',
            offset: '5',
            sort: 'desc',
        });
        assert.ok(/LIMIT 10 OFFSET 5;$/.test(statement.sql), statement.sql);
        const history = MySQL.getHistory('iobroker', 'ts_number', { index: 3, end: 20, count: '10' });
        // before: '10' + 2 was "102"
        assert.ok(/LIMIT 12;$/.test(history.sql), history.sql);
    });
});

describe('Test MySQL binds timestamps from messages as numbers (#587)', function () {
    const MySQL = DIALECTS.mysql;

    it('a timestamp that is no number is bound as NULL, a numeric string as number', function () {
        // MySQL in strict mode refuses to compare the text with the BIGINT column, MariaDB only warns
        assert.deepStrictEqual(MySQL.deleteFromTable('iobroker', 'ts_number', 3, '0 OR 1=1').params, [3, null]);
        assert.deepStrictEqual(
            MySQL.deleteFromTable('iobroker', 'ts_number', 3, '1700', '1800').params,
            [3, 1700, 1800],
        );
        assert.deepStrictEqual(
            MySQL.update('iobroker', 3, { val: 1, ts: 'x', q: '0' }, undefined, 'ts_number').params,
            [1, 0, 3, null],
        );
        const history = MySQL.getHistory('iobroker', 'ts_number', { index: 3, start: '10', end: 'abc' });
        assert.ok(!history.params.some(p => typeof p === 'string'), JSON.stringify(history.params));
        assert.ok(
            !MySQL.getCounterDiff('iobroker', { index: 3, start: '1', end: {} }).params.some(
                p => typeof p === 'string',
            ),
        );
        const [insert] = MySQL.insert('iobroker', 3, [
            { table: 'ts_number', state: { val: 1, ts: '5', q: '2' }, from: 0 },
        ]);
        assert.deepStrictEqual(insert.params, [3, 5, 1, 0, 0, 2]);
    });
});

describe('Test MySQL value conversion', function () {
    const MySQL = DIALECTS.mysql;
    const valuesOf = (table, vals) =>
        MySQL.insert(
            'iobroker',
            1,
            vals.map((val, i) => ({ table, state: { val, ts: i, ack: false }, from: 0 })),
        )[0].params.filter((_p, i) => i % (table === 'ts_counter' ? 3 : 6) === 2);

    it('numbers: text that is not a number, NaN and Infinity become NULL, like in the other dialects', function () {
        assert.deepStrictEqual(valuesOf('ts_number', [1.5, '42', '12 kWh', '', NaN, Infinity, true, null]), [
            1.5,
            42,
            null,
            null,
            null,
            null,
            1,
            null,
        ]);
        assert.deepStrictEqual(valuesOf('ts_counter', [3, NaN]), [3, null]);
    });

    it('booleans and strings', function () {
        assert.deepStrictEqual(valuesOf('ts_bool', [true, false, 'false', 'on', 1, NaN]), [1, 0, 0, 1, 1, null]);
        // the usual texts for on and off, a number is 1 unless 0, anything else NULL (before: 1, and 5 as 5)
        assert.deepStrictEqual(
            valuesOf('ts_bool', ['off', ' Yes ', 'NO', 'TRUE', '', 'abc', 5, 0, -1, Infinity, { a: 1 }]),
            [0, 1, 0, 1, 0, null, 1, 0, 1, null, null],
        );
        assert.deepStrictEqual(valuesOf('ts_string', ['a', 5, { a: 1 }]), ['a', '5', '{"a":1}']);
    });

    it('update() always sets the value, so the SET is never empty', function () {
        const statement = MySQL.update('iobroker', 5, { ts: 1 }, undefined, 'ts_number');
        assert.strictEqual(statement.sql, 'UPDATE `iobroker`.ts_number SET val=? WHERE id=? AND ts=?;');
        assert.deepStrictEqual(statement.params, [null, 5, 1]);
    });
});
