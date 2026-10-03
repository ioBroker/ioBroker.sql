const assert = require('node:assert');

// Issue #304 reported a primary key violation on MariaDB/MySQL, so the dialect it was reported on
// gets an executable check rather than a string assertion. This talks to the server directly - no
// js-controller, no adapter instance - so it belongs in the MySQL job's `test/testMySQL*.js` glob
// and costs a second.
const MySQL = require('../build/lib/mysql');

const HOST = '127.0.0.1';
const USER = process.env.SQL_USER || 'root';
const PASS = process.env.SQL_PASS || 'root';
// Never the real `iobroker` database: this one is created and dropped by the test itself.
const DB = '__iob_insert_duplicates__';

/** The batch from the issue: ts 1681102802872 appears twice for id 4, logged from two sources */
function duplicateBatch() {
    return [
        { table: 'ts_number', state: { val: 54.7, ts: 1681102502871, ack: true }, from: 2 },
        { table: 'ts_number', state: { val: 54.7, ts: 1681102802872, ack: true }, from: 2 },
        { table: 'ts_number', state: { val: 54.7, ts: 1681102502853, ack: true }, from: 3 },
        { table: 'ts_number', state: { val: 54.9, ts: 1681102802872, ack: true }, from: 3 },
    ];
}

describe('Test insert() against a live MySQL (#304)', function () {
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
                // left off on purpose: the adapter never sends batches either, and the test would
                // stop covering what it is meant to cover
                multipleStatements: false,
            });
            await connection.query(`DROP DATABASE IF EXISTS \`${DB}\``);
            await connection.query(`CREATE DATABASE \`${DB}\``);
            // the real DDL from init(), PRIMARY KEY(id, ts) and all
            await connection.query(
                `CREATE TABLE \`${DB}\`.ts_number  (id INTEGER, ts BIGINT, val REAL,    ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));`,
            );
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

    it('stores a batch that contains the same (id, ts) twice', async function () {
        await connection.query(`TRUNCATE \`${DB}\`.ts_number`);

        const [query] = MySQL.insert(DB, 4, duplicateBatch());
        await connection.query(query); // used to throw ER_DUP_ENTRY

        const [rows] = await connection.query(`SELECT ts, val, _from FROM \`${DB}\`.ts_number ORDER BY ts`);
        assert.strictEqual(rows.length, 3, `four rows, three distinct timestamps: ${JSON.stringify(rows)}`);
        assert.strictEqual(Number(rows[2].ts), 1681102802872);
        assert.strictEqual(rows[2].val, 54.7, 'the first value for a timestamp wins, the duplicate is dropped');
    });

    it('would fail without the guard - this is the bug that was reported', async function () {
        await connection.query(`TRUNCATE \`${DB}\`.ts_number`);

        const [query] = MySQL.insert(DB, 4, duplicateBatch());
        const unguarded = query.replace(/ ON DUPLICATE KEY UPDATE id=id/i, '');
        assert.notStrictEqual(unguarded, query, 'precondition: the guard was in the statement');

        await assert.rejects(
            () => connection.query(unguarded),
            err => {
                // exactly what issue #304 shows: Duplicate entry '<id>-<ts>' for key 'PRIMARY'
                assert.strictEqual(err.code, 'ER_DUP_ENTRY', err.message);
                assert.ok(err.message.includes('4-1681102802872'), err.message);
                return true;
            },
        );
    });

    it('still reports a real error instead of swallowing it', async function () {
        await connection.query(`TRUNCATE \`${DB}\`.ts_number`);

        // The guard is `ON DUPLICATE KEY UPDATE id=id`, not `INSERT IGNORE`, so that only the
        // uniqueness conflict is suppressed - see the comment in src/lib/mysql.ts. An INSERT naming
        // a column that does not exist has to keep failing.
        await assert.rejects(
            () =>
                connection.query(
                    `INSERT INTO \`${DB}\`.ts_number (id, ts, nope) VALUES (1, 2, 3) ON DUPLICATE KEY UPDATE id=id;`,
                ),
            err => {
                assert.strictEqual(err.code, 'ER_BAD_FIELD_ERROR', err.message);
                return true;
            },
        );
    });
});
