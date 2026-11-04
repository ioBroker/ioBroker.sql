const assert = require('node:assert');

// C1 of #587: the MySQL statements carry their values as bound parameters. Before, strings were
// inlined with only the single quotes removed, so "it's warm" was stored as "its warm", a value that
// ended with a backslash made the whole batch of up to 500 rows a syntax error, and datapoint IDs and
// sources went into the SQL unescaped. This talks to the server directly - no js-controller - and
// belongs to the `test/testMySQL*.js` glob of the MySQL CI job.
const MySQL = require('../build/lib/mysql');

const HOST = '127.0.0.1';
const USER = process.env.SQL_USER || 'root';
const PASS = process.env.SQL_PASS || 'root';
// Never the real `iobroker` database: this one is created and dropped by the test itself.
const DB = '__iob_bound_params__';

/** Strings that broke or were changed by the old inlining */
const NASTY = [
    "it's warm",
    'C:\\temp\\',
    'say "hi"',
    "\\'; DROP TABLE datapoints; --",
    'Grüße 😀',
    'tab\there\nnew line',
    'question?mark',
];

const run = (connection, statement) => connection.query(statement.sql, statement.params);

describe('Test the MySQL statements with bound values against a live server (#587)', function () {
    this.timeout(30000);

    let connection = null;
    let unreachable = '';

    before(async function () {
        let mysql;
        try {
            mysql = require('mysql2/promise');
        } catch (e) {
            unreachable = `mysql2 is an optionalDependency and is not installed: ${e.message}`;
            return;
        }

        try {
            connection = await mysql.createConnection({
                host: HOST,
                user: USER,
                password: PASS,
                connectTimeout: 5000,
                multipleStatements: false,
                charset: 'utf8mb4',
            });
            await connection.query(`DROP DATABASE IF EXISTS \`${DB}\``);
            await connection.query(`CREATE DATABASE \`${DB}\` DEFAULT CHARACTER SET utf8mb4`);
            // the real DDL from init()
            for (const statement of MySQL.init(DB, true)) {
                await connection.query(statement);
            }
        } catch (e) {
            unreachable = `${e.code || ''} ${e.message}`.trim();
            connection = null;
        }
    });

    after(async function () {
        if (connection) {
            await connection.query(`DROP DATABASE IF EXISTS \`${DB}\``);
            await connection.end();
        }
    });

    beforeEach(function () {
        // Skipping rather than failing keeps `npx mocha test/testMySQL*.js` usable on a machine
        // without a server. The MySQL CI job always has one, so there it really runs.
        if (!connection) {
            console.log(`Skipped: no MySQL at ${HOST} (${unreachable})`);
            this.skip();
        }
    });

    it('stores strings exactly as they are, in one batch', async function () {
        const values = NASTY.map((val, i) => ({
            table: 'ts_string',
            state: { val, ts: 1000 + i, ack: true },
            from: 1,
        }));
        const statements = MySQL.insert(DB, 7, values);
        assert.strictEqual(statements.length, 1, 'one batch');
        for (const val of NASTY) {
            assert.ok(!statements[0].sql.includes(val), `the value is not in the SQL text: ${val}`);
        }

        await run(connection, statements[0]); // a trailing backslash used to fail the whole batch

        const [rows] = await run(
            connection,
            MySQL.getHistory(DB, 'ts_string', { index: 7, start: 1, end: 2000, ack: true, from: false, q: false }),
        );
        assert.deepStrictEqual(
            rows.filter(r => Number(r.ts) >= 1000).map(r => r.val),
            NASTY,
        );
    });

    it('stores numbers, booleans and NULL; NaN and Infinity become NULL instead of failing the batch', async function () {
        await run(
            connection,
            MySQL.insert(DB, 8, [
                { table: 'ts_number', state: { val: 1.5, ts: 1, ack: true }, from: 1 },
                { table: 'ts_number', state: { val: NaN, ts: 2, ack: true }, from: 1 },
                { table: 'ts_number', state: { val: Infinity, ts: 3, ack: true }, from: 1 },
                { table: 'ts_number', state: { val: null, ts: 4, ack: true }, from: 1 },
                { table: 'ts_number', state: { val: '42', ts: 5, ack: false }, from: 1 },
            ])[0],
        );
        await run(
            connection,
            MySQL.insert(DB, 9, [
                { table: 'ts_bool', state: { val: true, ts: 1, ack: true }, from: 1 },
                { table: 'ts_bool', state: { val: false, ts: 2, ack: true }, from: 1 },
            ])[0],
        );

        const [numbers] = await connection.query(`SELECT ts, val, ack FROM \`${DB}\`.ts_number WHERE id=8 ORDER BY ts`);
        assert.deepStrictEqual(
            numbers.map(r => r.val),
            [1.5, null, null, null, 42],
        );
        assert.strictEqual(numbers[4].ack, 0);
        const [bools] = await connection.query(`SELECT val FROM \`${DB}\`.ts_bool WHERE id=9 ORDER BY ts`);
        assert.deepStrictEqual(
            bools.map(r => r.val),
            [1, 0],
        );
    });

    it('finds and creates datapoints and sources whose name contains a quote', async function () {
        const name = "alias.0.it's\\here";
        await run(connection, MySQL.getIdInsert(DB, name, 1));
        const [ids] = await run(connection, MySQL.getIdSelect(DB, name));
        assert.strictEqual(ids.length, 1);
        assert.strictEqual(ids[0].name, name);

        const source = "system.adapter.javascript.0.script.js.o'brien";
        await run(connection, MySQL.getFromInsert(DB, source));
        const [sources] = await run(connection, MySQL.getFromSelect(DB, source));
        assert.strictEqual(sources.length, 1);
    });

    it('a lookup also returns an ID that differs only in case, and pickRowByName() refuses it (#588)', async function () {
        const { pickRowByName } = require('../build/lib/names');
        await run(connection, MySQL.getIdInsert(DB, 'alias.0.Licht', 2));

        let [rows] = await run(connection, MySQL.getIdSelect(DB, 'alias.0.licht'));
        assert.deepStrictEqual(
            rows.map(r => r.name),
            ['alias.0.Licht'],
            'the default collation ignores case',
        );
        assert.strictEqual(pickRowByName(rows, 'alias.0.licht'), undefined, 'so getId() inserts a row of its own');

        await run(connection, MySQL.getIdInsert(DB, 'alias.0.licht', 0));
        [rows] = await run(connection, MySQL.getIdSelect(DB, 'alias.0.licht'));
        assert.strictEqual(rows.length, 2);
        assert.strictEqual(pickRowByName(rows, 'alias.0.licht').type, 0);
        assert.strictEqual(pickRowByName(rows, 'alias.0.Licht').type, 2);
    });

    it('updates and deletes single values', async function () {
        await run(
            connection,
            MySQL.insert(DB, 10, [{ table: 'ts_string', state: { val: 'old', ts: 50, ack: false }, from: 1 }])[0],
        );
        await run(connection, MySQL.update(DB, 10, { val: "new 'one'\\", ts: 50, ack: true, q: 0 }, 2, 'ts_string'));
        let [rows] = await connection.query(`SELECT val, ack, _from FROM \`${DB}\`.ts_string WHERE id=10`);
        assert.deepStrictEqual(rows, [{ val: "new 'one'\\", ack: 1, _from: 2 }]);

        await run(connection, MySQL.deleteFromTable(DB, 'ts_string', 10, 50));
        [rows] = await connection.query(`SELECT val FROM \`${DB}\`.ts_string WHERE id=10`);
        assert.strictEqual(rows.length, 0);
    });

    it('refuses a session with NO_BACKSLASH_ESCAPES against the real server', function (done) {
        const { checkSqlMode } = require('../build/lib/mysql-client');
        const mysql = require('mysql2');
        const raw = mysql.createConnection({ host: HOST, user: USER, password: PASS, connectTimeout: 5000 });
        raw.query("SET SESSION sql_mode = CONCAT(@@SESSION.sql_mode, ',NO_BACKSLASH_ESCAPES')", err => {
            assert.ifError(err);
            checkSqlMode(raw, err => {
                assert.match(String(err), /NO_BACKSLASH_ESCAPES/);
                done();
            });
        });
    });

    it('treats a value from a message as a value, not as SQL', async function () {
        // `delete`/`update` messages hand their ts through; inlined, this text deleted every row
        await run(
            connection,
            MySQL.insert(DB, 11, [{ table: 'ts_number', state: { val: 1, ts: 1, ack: true }, from: 1 }])[0],
        );
        await run(connection, MySQL.deleteFromTable(DB, 'ts_number', 11, '0 OR 1=1'));
        const [rows] = await connection.query(`SELECT COUNT(*) AS n FROM \`${DB}\`.ts_number WHERE id=11`);
        assert.strictEqual(Number(rows[0].n), 1, 'nothing was deleted');
    });
});
