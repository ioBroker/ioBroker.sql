const assert = require('node:assert');

// #588: new MS SQL tables store the names as nvarchar with a case-sensitive collation and the string
// values as nvarchar(max); literals are Unicode (N'...') and the database name is put in brackets.
// Before, `varchar` turned `alias.0.Кухня` or an emoji into `?`, the default collation found IDs that
// differ only in case, and a database name like `io-broker` was a syntax error. This talks to the server
// directly - no js-controller - and belongs to the `test/testMSSQL*.js` glob of the MS SQL CI job.
const MSSQL = require('../build/lib/mssql');

const HOST = '127.0.0.1';
const USER = process.env.SQL_USER || 'sa';
const PASS = process.env.SQL_PASS || 'Password12!';
// Never the real `iobroker` database: these are created and dropped by the test itself. The `-` needs
// the brackets.
const OWN = '__iob-charset-own__';
const HOSTER = '__iob-charset-hoster__';

const NAMES = ['alias.0.Кухня', 'alias.0.Küche 😀'];

describe('Test the Unicode columns and literals of MS SQL against a live server (#588)', function () {
    this.timeout(60000);

    let pool = null;
    let unreachable = '';
    const query = async sql => (await pool.request().query(sql)).recordset || [];

    before(async function () {
        let mssql;
        try {
            mssql = require('mssql');
        } catch (e) {
            unreachable = `mssql is an optionalDependency and is not installed: ${e.message}`;
            return;
        }
        try {
            pool = await new mssql.ConnectionPool({
                server: HOST,
                user: USER,
                password: PASS,
                options: { encrypt: false, trustServerCertificate: true },
                connectionTimeout: 10000,
            }).connect();
        } catch (e) {
            // only a missing server skips the tests; an error of the statements below fails them
            unreachable = `${e.code || ''} ${e.message}`.trim();
            pool = null;
            return;
        }
        for (const db of [OWN, HOSTER]) {
            await query(`IF DB_ID(N'${db}') IS NOT NULL DROP DATABASE [${db}]`);
        }
        // the adapter creates the database itself
        for (const statement of MSSQL.init(OWN)) {
            await query(statement);
        }
        // "Do not create database": created outside the adapter with a case-insensitive default
        await query(`CREATE DATABASE [${HOSTER}] COLLATE SQL_Latin1_General_CP1_CI_AS`);
        for (const statement of MSSQL.init(HOSTER, true)) {
            await query(statement);
        }
    });

    after(async function () {
        if (pool) {
            for (const db of [OWN, HOSTER]) {
                await query(`IF DB_ID(N'${db}') IS NOT NULL DROP DATABASE [${db}]`).catch(() => {});
            }
            await pool.close();
        }
    });

    beforeEach(function () {
        if (!pool) {
            console.log(`Skipped: no MS SQL at ${HOST} (${unreachable})`);
            this.skip();
        }
    });

    for (const db of [OWN, HOSTER]) {
        it(`${db}: the name columns are nvarchar and case-sensitive, the values nvarchar(max)`, async function () {
            const rows = await query(
                `SELECT t.name AS t, c.name AS c, TYPE_NAME(c.user_type_id) AS type, c.max_length AS len, c.collation_name AS co ` +
                    `FROM [${db}].sys.columns c JOIN [${db}].sys.tables t ON t.object_id = c.object_id ` +
                    `WHERE c.name IN ('name', 'val') AND t.name IN ('datapoints', 'sources', 'ts_string') ORDER BY t.name`,
            );
            assert.deepStrictEqual(
                rows.map(r => `${r.t}.${r.c} ${r.type}`),
                ['datapoints.name nvarchar', 'sources.name nvarchar', 'ts_string.val nvarchar'],
            );
            assert.strictEqual(rows.find(r => r.t === 'ts_string').len, -1, 'nvarchar(max)');
            for (const r of rows.filter(r => r.c === 'name')) {
                assert.strictEqual(r.co, 'Latin1_General_100_CS_AS_SC', `${r.t}.${r.c}`);
            }
        });

        it(`${db}: IDs that differ only in case are two datapoints`, async function () {
            await query(MSSQL.getIdInsert(db, 'alias.0.Licht', 0));
            await query(MSSQL.getIdInsert(db, 'alias.0.licht', 1));
            const rows = await query(MSSQL.getIdSelect(db, 'alias.0.licht'));
            assert.deepStrictEqual(
                rows.map(r => r.name),
                ['alias.0.licht'],
            );
        });

        it(`${db}: IDs that differ only in a trailing space are kept apart by pickRowByName()`, async function () {
            // SQL Server ignores trailing spaces in `=` with every collation, so each lookup may return
            // both rows. The adapter takes only the exact one.
            const { pickRowByName } = require('../build/lib/names');
            await query(MSSQL.getIdInsert(db, 'alias.0.Tor', 0));
            await query(MSSQL.getIdInsert(db, 'alias.0.Tor ', 1));
            for (const name of ['alias.0.Tor', 'alias.0.Tor ']) {
                const rows = await query(MSSQL.getIdSelect(db, name));
                assert.strictEqual(pickRowByName(rows, name)?.name, name);
            }
        });

        it(`${db}: Cyrillic and emojis in IDs, sources and values come back unchanged`, async function () {
            for (const [i, name] of NAMES.entries()) {
                await query(MSSQL.getIdInsert(db, name, 1));
                const ids = await query(MSSQL.getIdSelect(db, name));
                assert.deepStrictEqual(
                    ids.map(r => r.name),
                    [name],
                );

                const source = `system.adapter.telegram.0 ${name}`;
                await query(MSSQL.getFromInsert(db, source));
                const sources = await query(MSSQL.getFromSelect(db, source));
                assert.deepStrictEqual(
                    sources.map(r => r.name),
                    [source],
                );

                for (const statement of MSSQL.insert(db, ids[0].id, [
                    {
                        table: 'ts_string',
                        state: { val: `Grüße ${name}`, ts: 1000 + i, ack: true },
                        from: sources[0].id,
                    },
                ])) {
                    await query(statement);
                }
                const values = await query(`SELECT val FROM [${db}].dbo.ts_string WHERE id=${ids[0].id}`);
                assert.deepStrictEqual(
                    values.map(v => v.val),
                    [`Grüße ${name}`],
                );

                await query(MSSQL.update(db, ids[0].id, { val: `neu ${name}`, ts: 1000 + i }, undefined, 'ts_string'));
                const updated = await query(`SELECT val FROM [${db}].dbo.ts_string WHERE id=${ids[0].id}`);
                assert.deepStrictEqual(
                    updated.map(v => v.val),
                    [`neu ${name}`],
                );
            }
        });

        it(`${db}: the statements with the database name in brackets run`, async function () {
            await query(MSSQL.getTableSize(db, 'ts_string'));
            await query(MSSQL.getIdCounts(db, 'ts_string'));
            await query(MSSQL.getFirstTs(db, 'ts_string'));
        });
    }

    it('Reset DB drops the tables and the database with a "-" in its name', async function () {
        for (const statement of MSSQL.destroy(OWN)) {
            if (statement.startsWith('DBCC')) {
                continue;
            }
            await query(statement);
        }
        const rows = await query(`SELECT DB_ID(N'${OWN}') AS id`);
        assert.strictEqual(rows[0].id, null);
    });
});
