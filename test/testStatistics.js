const assert = require('node:assert');
const sqlite3 = require('sqlite3');

// Imported from their own modules, never from build/main: that would pull in
// @iobroker/adapter-core, which calls process.exit(10) when js-controller cannot be resolved.
const {
    classifyDatapoint,
    estimateBytes,
    summarize,
    selectForCleanup,
    resolveStatus,
} = require('../build/lib/statistics');
const SQLite = require('../build/lib/sqlite');

function stat(overrides) {
    return {
        id: 'sql.0.x',
        index: 1,
        type: 'Number',
        table: 'ts_number',
        count: 0,
        firstTs: null,
        lastTs: null,
        estimatedBytes: 0,
        status: 'active',
        ...overrides,
    };
}

describe('Test classifyDatapoint', function () {
    it('calls a datapoint whose object is gone objectMissing', function () {
        assert.strictEqual(classifyDatapoint(false, false), 'objectMissing');
    });

    it('calls a logged datapoint without an object of its own active (alias, #587)', function () {
        // the values of an alias are stored under the alias name, which has no object; it was
        // listed as objectMissing before and a confirmed cleanup deleted its history
        assert.strictEqual(classifyDatapoint(false, true), 'active');
    });

    it('separates a disabled datapoint from a deleted one', function () {
        assert.strictEqual(classifyDatapoint(true, false), 'loggingDisabled');
    });

    it('calls a logged, existing datapoint active', function () {
        assert.strictEqual(classifyDatapoint(true, true), 'active');
    });
});

describe('Test estimateBytes', function () {
    it('multiplies the row count by the average width', function () {
        assert.strictEqual(estimateBytes(1000, 42), 42000);
    });

    it('returns null instead of inventing a number when the width is unknown', function () {
        // the database could not report a width - a zero would read as "no data"
        for (const width of [null, undefined, 0, -1, NaN, Infinity]) {
            assert.strictEqual(estimateBytes(1000, width), null, `width=${width}`);
        }
    });

    it('rounds to whole bytes', function () {
        assert.strictEqual(estimateBytes(3, 10.4), 31);
    });
});

describe('Test summarize', function () {
    it('counts rows and datapoints per status', function () {
        const summary = summarize([
            stat({ id: 'a', count: 10, estimatedBytes: 100, status: 'active' }),
            stat({ id: 'b', count: 5, estimatedBytes: 50, status: 'loggingDisabled' }),
            stat({ id: 'c', count: 7, estimatedBytes: 70, status: 'objectMissing' }),
            stat({ id: 'd', count: 3, estimatedBytes: 30, status: 'objectMissing' }),
        ]);

        assert.strictEqual(summary.datapoints, 4);
        assert.strictEqual(summary.rows, 25);
        assert.strictEqual(summary.estimatedBytes, 250);
        assert.deepStrictEqual(summary.byStatus.objectMissing, { datapoints: 2, rows: 10 });
        assert.deepStrictEqual(summary.byStatus.loggingDisabled, { datapoints: 1, rows: 5 });
        assert.deepStrictEqual(summary.byStatus.active, { datapoints: 1, rows: 10 });
    });

    it('reports an unknown total rather than a partial sum', function () {
        const summary = summarize([
            stat({ id: 'a', count: 10, estimatedBytes: 100 }),
            stat({ id: 'b', count: 5, estimatedBytes: null }),
        ]);

        assert.strictEqual(summary.rows, 15, 'the row count stays exact');
        assert.strictEqual(summary.estimatedBytes, null, 'a partial sum would understate the footprint');
    });

    it('handles an empty database', function () {
        const summary = summarize([]);
        assert.strictEqual(summary.datapoints, 0);
        assert.strictEqual(summary.rows, 0);
        assert.strictEqual(summary.estimatedBytes, 0);
    });
});

describe('Test selectForCleanup', function () {
    const stats = [
        stat({ id: 'active', status: 'active', count: 9 }),
        stat({ id: 'disabled', status: 'loggingDisabled', count: 8 }),
        stat({ id: 'missing', status: 'objectMissing', count: 7 }),
    ];

    it('defaults to the deleted states only', function () {
        // the safe half: nobody can reach this data any more
        assert.deepStrictEqual(
            selectForCleanup(stats).map(s => s.id),
            ['missing'],
        );
        assert.deepStrictEqual(
            selectForCleanup(stats, {}).map(s => s.id),
            ['missing'],
        );
    });

    it('includes disabled datapoints only when asked explicitly', function () {
        assert.deepStrictEqual(
            selectForCleanup(stats, { loggingDisabled: true }).map(s => s.id),
            ['disabled', 'missing'],
        );
    });

    it('can be narrowed to the disabled ones', function () {
        assert.deepStrictEqual(
            selectForCleanup(stats, { objectMissing: false, loggingDisabled: true }).map(s => s.id),
            ['disabled'],
        );
    });

    it('never selects an active datapoint', function () {
        for (const scope of [undefined, {}, { objectMissing: true, loggingDisabled: true }]) {
            assert.ok(
                !selectForCleanup(stats, scope).some(s => s.status === 'active'),
                `scope=${JSON.stringify(scope)}`,
            );
        }
    });

    it('selects nothing when both are switched off', function () {
        assert.deepStrictEqual(selectForCleanup(stats, { objectMissing: false }), []);
    });
});

// The builders are only strings until a database accepts them.
describe('Test the statistics and cleanup SQL against SQLite', function () {
    let db;

    beforeEach(function (done) {
        db = new sqlite3.Database(':memory:');
        db.serialize(() => {
            db.run('CREATE TABLE datapoints (id INTEGER, name TEXT, type INTEGER)');
            db.run('CREATE TABLE ts_number (id INTEGER, ts BIGINT, val REAL)');
            db.run('CREATE TABLE ts_string (id INTEGER, ts BIGINT, val TEXT)');
            db.run("INSERT INTO datapoints VALUES (1,'sql.0.alive',0),(2,'sql.0.orphan',0)");
            db.run('INSERT INTO ts_number VALUES (1,1000,1),(1,2000,2),(1,3000,3)');
            db.run('INSERT INTO ts_number VALUES (2,1500,9),(2,2500,9)');
            // a datapoint whose storage type was switched has rows in two tables
            db.run("INSERT INTO ts_string VALUES (2,3500,'x')", done);
        });
    });

    afterEach(function (done) {
        db.close(done);
    });

    it('getIdCounts reports exact counts and the covered range', function (done) {
        db.all(SQLite.getIdCounts('ignored', 'ts_number'), function (err, rows) {
            assert.ifError(err);
            const byId = Object.fromEntries(rows.map(r => [r.id, r]));
            assert.strictEqual(byId[1].cnt, 3);
            assert.strictEqual(byId[1].first_ts, 1000);
            assert.strictEqual(byId[1].last_ts, 3000);
            assert.strictEqual(byId[2].cnt, 2);
            done();
        });
    });

    it('getTableSize answers with an average width', function (done) {
        db.all(SQLite.getTableSize('ignored', 'ts_number'), function (err, rows) {
            // dbstat is not in every sqlite3 build; the adapter treats a failure as "unknown"
            if (err) {
                return done();
            }
            assert.ok(rows[0].avg_row_length > 0, JSON.stringify(rows[0]));
            done();
        });
    });

    it('the cleanup removes the values from every table and the datapoints row', function (done) {
        const index = 2;
        db.serialize(() => {
            db.run(SQLite.deleteFromTable('ignored', 'ts_number', index));
            db.run(SQLite.deleteFromTable('ignored', 'ts_string', index));
            db.run(SQLite.deleteDatapoint('ignored', index));

            db.all('SELECT id FROM ts_number', function (err, rows) {
                assert.ifError(err);
                assert.deepStrictEqual(
                    rows.map(r => r.id),
                    [1, 1, 1],
                    'the surviving datapoint keeps all of its values',
                );

                db.all('SELECT id FROM ts_string', function (err, rows) {
                    assert.ifError(err);
                    assert.deepStrictEqual(rows, [], 'the second table is cleaned too');

                    db.all('SELECT id, name FROM datapoints', function (err, rows) {
                        assert.ifError(err);
                        assert.deepStrictEqual(
                            rows.map(r => r.name),
                            ['sql.0.alive'],
                            'only the orphan loses its datapoints row',
                        );
                        done();
                    });
                });
            });
        });
    });
});

describe('Test resolveStatus (#587)', function () {
    const objects = new Set(['javascript.0.x']);
    function sources(overrides) {
        const asked = [];
        return {
            asked,
            isLogged: name => name === 'alias.logged',
            configured: new Set(['alias.logged', 'alias.off']),
            objectExists: async name => {
                asked.push(name);
                return objects.has(name);
            },
            ...overrides,
        };
    }

    it('a datapoint logged under an alias name without an object is active', async function () {
        const src = sources();
        assert.strictEqual(await resolveStatus('alias.logged', src), 'active');
        // the object database is not asked at all
        assert.deepStrictEqual(src.asked, []);
    });

    it('an alias with logging switched off is loggingDisabled, not objectMissing', async function () {
        const src = sources();
        assert.strictEqual(await resolveStatus('alias.off', src), 'loggingDisabled');
        assert.deepStrictEqual(src.asked, []);
    });

    it('other names are looked up as objects', async function () {
        const src = sources();
        assert.strictEqual(await resolveStatus('javascript.0.x', src), 'loggingDisabled');
        assert.strictEqual(await resolveStatus('gone.0.x', src), 'objectMissing');
        assert.deepStrictEqual(src.asked, ['javascript.0.x', 'gone.0.x']);
    });

    it('an error of the object database fails instead of meaning "missing"', async function () {
        const src = sources({
            objectExists: async () => {
                throw new Error('Connection is closed.');
            },
        });
        await assert.rejects(resolveStatus('javascript.0.x', src), /Connection is closed/);
        // logged and configured names do not need the object database and still resolve
        assert.strictEqual(await resolveStatus('alias.logged', src), 'active');
    });

    it('nothing is selected for the cleanup when the status cannot be resolved', async function () {
        const src = sources({
            objectExists: async () => {
                throw new Error('DB closed');
            },
        });
        // the same order as #collectStatistics(): resolve every status first, then select
        const resolveAll = async names =>
            Promise.all(names.map(async id => stat({ id, status: await resolveStatus(id, src) })));
        let selected = null;
        await assert.rejects(async () => {
            selected = selectForCleanup(await resolveAll(['alias.logged', 'gone.0.x']));
        }, /DB closed/);
        assert.strictEqual(selected, null);
    });
});
