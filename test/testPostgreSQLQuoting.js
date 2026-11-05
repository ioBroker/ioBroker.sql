const assert = require('node:assert');

// #587: PostgreSQL reads backslashes in a plain '...' literal as escapes when
// standard_conforming_strings is off (possible per database, per role or in postgresql.conf). The
// dialect quotes with escape string literals (E'...'), which behave the same with either setting.
// This talks to the server directly - no js-controller - and belongs to the
// `test/testPostgreSQL*.js` glob of the PostgreSQL CI job.
const PostgreSQL = require('../build/lib/postgresql');

const NASTY = ["\\'; DROP TABLE datapoints; --", 'C:\\temp\\', "it's", 'a \\n is not a newline', 'Grüße 😀'];

describe('Test the PostgreSQL quoting against a live server with standard_conforming_strings off (#587)', function () {
    this.timeout(30000);

    let client = null;
    let unreachable = '';

    before(async function () {
        let pg;
        try {
            pg = require('pg');
        } catch (e) {
            unreachable = `pg is an optionalDependency and is not installed: ${e.message}`;
            return;
        }
        try {
            client = new pg.Client({
                host: '127.0.0.1',
                user: process.env.SQL_USER || 'postgres',
                password: process.env.SQL_PASS || '',
                database: 'postgres',
                connectionTimeoutMillis: 5000,
            });
            await client.connect();
            // a schema of its own, so the database of the other tests is not touched
            await client.query('DROP SCHEMA IF EXISTS iob_quoting CASCADE');
            await client.query('CREATE SCHEMA iob_quoting');
            await client.query('SET search_path TO iob_quoting');
            for (const statement of PostgreSQL.init('ignored')) {
                await client.query(statement);
            }
            await client.query('SET standard_conforming_strings = off');
            // with the setting off, a backslash in a plain literal warns by default
            await client.query('SET escape_string_warning = off');
        } catch (e) {
            unreachable = `${e.code || ''} ${e.message}`.trim();
            client = null;
        }
    });

    after(async function () {
        if (client) {
            await client.query('SET standard_conforming_strings = on');
            await client.query('DROP SCHEMA IF EXISTS iob_quoting CASCADE');
            await client.end();
        }
    });

    beforeEach(function () {
        if (!client) {
            console.log(`Skipped: no PostgreSQL at 127.0.0.1 (${unreachable})`);
            this.skip();
        }
    });

    it('the setting is really off for this session', async function () {
        const { rows } = await client.query('SHOW standard_conforming_strings');
        assert.strictEqual(rows[0].standard_conforming_strings, 'off');
    });

    it('stores strings exactly as they are and executes nothing of them', async function () {
        const [statement] = PostgreSQL.insert(
            'ignored',
            7,
            NASTY.map((val, i) => ({ table: 'ts_string', state: { val, ts: 1000 + i, ack: true }, from: 1 })),
        );
        await client.query(statement);
        const { rows } = await client.query('SELECT val FROM ts_string WHERE id=7 ORDER BY ts');
        assert.deepStrictEqual(
            rows.map(r => r.val),
            NASTY,
        );
        // the table the value names is still there
        await client.query('SELECT COUNT(*) FROM datapoints');
    });

    it('finds and creates datapoints and sources whose name contains a backslash and a quote', async function () {
        const name = "alias.0.x\\'; DROP TABLE datapoints; --";
        await client.query(PostgreSQL.getIdInsert('ignored', name, 1));
        const ids = await client.query(PostgreSQL.getIdSelect('ignored', name));
        assert.strictEqual(ids.rows.length, 1);
        assert.strictEqual(ids.rows[0].name, name);

        const source = 'system.adapter.javascript.0.script.js.C:\\temp\\';
        await client.query(PostgreSQL.getFromInsert('ignored', source));
        const sources = await client.query(PostgreSQL.getFromSelect('ignored', source));
        assert.strictEqual(sources.rows.length, 1);
        assert.strictEqual(sources.rows[0].name, source);
    });

    it('updates a value with a backslash and a quote', async function () {
        await client.query(
            PostgreSQL.insert('ignored', 8, [
                { table: 'ts_string', state: { val: 'old', ts: 5, ack: true }, from: 1 },
            ])[0],
        );
        await client.query(PostgreSQL.update('ignored', 8, { val: "new \\'one'\\", ts: 5 }, undefined, 'ts_string'));
        const { rows } = await client.query('SELECT val FROM ts_string WHERE id=8');
        assert.deepStrictEqual(rows, [{ val: "new \\'one'\\" }]);
    });
});
