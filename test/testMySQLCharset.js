const assert = require('node:assert');

// #588: the tables of the adapter use utf8mb4 for every text column and utf8mb4_bin for the `name`
// columns, whatever the default of the database is. Before, a database the adapter created had
// `utf8` (utf8mb3, no emoji) with the case-insensitive `utf8_general_ci`, and a database created by a
// hoster kept its own default (often latin1). This talks to the server directly - no js-controller -
// and belongs to the `test/testMySQL*.js` glob of the MySQL CI job.
const MySQL = require('../build/lib/mysql');

const HOST = '127.0.0.1';
const USER = process.env.SQL_USER || 'root';
const PASS = process.env.SQL_PASS || 'root';
// Never the real `iobroker` database: these are created and dropped by the test itself.
const OWN = '__iob_charset_own__';
const HOSTER = '__iob_charset_hoster__';

const run = (connection, statement) =>
    typeof statement === 'string' ? connection.query(statement) : connection.query(statement.sql, statement.params);

describe('Test the character set and collation of the MySQL tables against a live server (#588)', function () {
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
            });
        } catch (e) {
            // only a missing server skips the tests; an error of the statements below fails them
            unreachable = `${e.code || ''} ${e.message}`.trim();
            connection = null;
            return;
        }
        await connection.query(`DROP DATABASE IF EXISTS \`${OWN}\``);
        await connection.query(`DROP DATABASE IF EXISTS \`${HOSTER}\``);
        // the adapter creates the database itself
        for (const statement of MySQL.init(OWN)) {
            await run(connection, statement);
        }
        // "Do not create database": a hoster created it with an old default
        await connection.query(`CREATE DATABASE \`${HOSTER}\` DEFAULT CHARACTER SET latin1`);
        for (const statement of MySQL.init(HOSTER, true)) {
            await run(connection, statement);
        }
    });

    after(async function () {
        if (connection) {
            await connection.query(`DROP DATABASE IF EXISTS \`${OWN}\``);
            await connection.query(`DROP DATABASE IF EXISTS \`${HOSTER}\``);
            await connection.end();
        }
    });

    beforeEach(function () {
        if (!connection) {
            console.log(`Skipped: no MySQL at ${HOST} (${unreachable})`);
            this.skip();
        }
    });

    for (const db of [OWN, HOSTER]) {
        it(`${db}: the name columns compare exactly, the text columns are utf8mb4`, async function () {
            const [rows] = await connection.query(
                'SELECT TABLE_NAME AS t, COLUMN_NAME AS c, CHARACTER_SET_NAME AS cs, COLLATION_NAME AS co ' +
                    "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND DATA_TYPE='text' ORDER BY t, c",
                [db],
            );
            assert.deepStrictEqual(
                rows.map(r => `${r.t}.${r.c} ${r.cs} ${r.co}`),
                [
                    'datapoints.name utf8mb4 utf8mb4_bin',
                    'sources.name utf8mb4 utf8mb4_bin',
                    `ts_string.val utf8mb4 ${rows.find(r => r.t === 'ts_string').co}`,
                ],
            );
        });

        it(`${db}: IDs that differ only in case are two datapoints`, async function () {
            await run(connection, MySQL.getIdInsert(db, 'alias.0.Licht', 0));
            await run(connection, MySQL.getIdInsert(db, 'alias.0.licht', 1));
            const [rows] = await run(connection, MySQL.getIdSelect(db, 'alias.0.licht'));
            assert.deepStrictEqual(
                rows.map(r => r.name),
                ['alias.0.licht'],
            );
        });

        it(`${db}: an emoji in a string value, an ID and a source is stored`, async function () {
            const name = 'alias.0.Küche 😀';
            await run(connection, MySQL.getIdInsert(db, name, 1));
            const [ids] = await run(connection, MySQL.getIdSelect(db, name));
            assert.strictEqual(ids.length, 1);
            assert.strictEqual(ids[0].name, name);

            await run(connection, MySQL.getFromInsert(db, 'system.adapter.telegram.0 😀'));
            const [sources] = await run(connection, MySQL.getFromSelect(db, 'system.adapter.telegram.0 😀'));
            assert.strictEqual(sources[0].name, 'system.adapter.telegram.0 😀');

            const [statement] = MySQL.insert(db, ids[0].id, [
                { table: 'ts_string', state: { val: 'Grüße 😀', ts: 1000, ack: true }, from: sources[0].id },
            ]);
            await run(connection, statement);
            const [values] = await connection.query(`SELECT val FROM \`${db}\`.ts_string WHERE id=?`, [ids[0].id]);
            assert.deepStrictEqual(
                values.map(v => v.val),
                ['Grüße 😀'],
            );
        });
    }
});
