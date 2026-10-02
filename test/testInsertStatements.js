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
