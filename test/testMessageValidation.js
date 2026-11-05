const assert = require('node:assert');

// Imported from the modules, not from build/main (see testMessageGuard.js for the reason)
const {
    toTimestamp,
    toQuality,
    isId,
    parseDeleteRequests,
    parseUpdateRequests,
    valueForTable,
} = require('../build/lib/messages');
const { quoteSqlString, numberOrNull } = require('../build/lib/sql-string');

const DIALECTS = {
    postgresql: require('../build/lib/postgresql'),
    sqlite: require('../build/lib/sqlite'),
    mssql: require('../build/lib/mssql'),
};

const NOW = 1_700_000_000_000;

describe('Message validation: timestamps and IDs', function () {
    it('accepts numbers, numeric strings and date strings', function () {
        assert.strictEqual(toTimestamp(1700000000000), 1700000000000);
        assert.strictEqual(toTimestamp('1700000000000'), 1700000000000);
        assert.strictEqual(toTimestamp(' 1700000000000 '), 1700000000000);
        assert.strictEqual(toTimestamp('2023-11-14T22:13:20.000Z'), 1700000000000);
        assert.strictEqual(toTimestamp(new Date(1700000000000)), 1700000000000);
        assert.strictEqual(toTimestamp(0), 0);
    });

    it('refuses everything that is not a point in time', function () {
        for (const value of ['31.02.2024', 'abc', '', '   ', NaN, Infinity, -Infinity, null, undefined, {}, [], true]) {
            assert.strictEqual(toTimestamp(value), undefined, `value ${JSON.stringify(value)}`);
        }
        assert.strictEqual(toTimestamp('0 OR 1=1'), undefined);
    });

    it('quality defaults to 0 and must be a non-negative integer', function () {
        assert.strictEqual(toQuality(undefined), 0);
        assert.strictEqual(toQuality(null), 0);
        assert.strictEqual(toQuality(0x42), 0x42);
        assert.strictEqual(toQuality('66'), 66);
        assert.strictEqual(toQuality(-1), undefined);
        assert.strictEqual(toQuality('1 OR 1=1'), undefined);
        assert.strictEqual(toQuality({}), undefined);
    });

    it('an ID is a non-empty string', function () {
        assert.ok(isId('hm-rpc.0.ABC.1.STATE'));
        for (const value of ['', undefined, null, 1, {}, [], true]) {
            assert.ok(!isId(value), `value ${JSON.stringify(value)}`);
        }
    });
});

describe('Message validation: delete requests', function () {
    const alias = id => (id === 'alias' ? 'real' : id);

    it('parses every shape the adapter accepted so far', function () {
        const cases = [
            [{ id: 'a', ts: 5 }, [{ id: 'a', ts: 5 }]],
            [{ id: 'a', state: { ts: 5 } }, [{ id: 'a', ts: 5 }]],
            [{ id: 'a', start: 1, end: 2 }, [{ id: 'a', start: 1, end: 2 }]],
            [{ id: 'a', start: 1 }, [{ id: 'a', start: 1, end: NOW }]],
            [{ id: 'a', state: { start: 1, end: 2 } }, [{ id: 'a', start: 1, end: 2 }]],
            [
                [
                    { id: 'a', ts: 5 },
                    { id: 'b', start: 1, end: 2 },
                ],
                [
                    { id: 'a', ts: 5 },
                    { id: 'b', start: 1, end: 2 },
                ],
            ],
            [[{ id: 'a', state: { ts: 5 } }], [{ id: 'a', ts: 5 }]],
            [
                { id: 'a', state: [{ ts: 5 }, { start: 1, end: 2 }, 7] },
                [
                    { id: 'a', ts: 5 },
                    { id: 'a', start: 1, end: 2 },
                    { id: 'a', ts: 7 },
                ],
            ],
            [
                { id: 'a', ts: [5, 6] },
                [
                    { id: 'a', ts: 5 },
                    { id: 'a', ts: 6 },
                ],
            ],
            [{ id: 'alias', ts: 5 }, [{ id: 'real', ts: 5 }]],
            [
                { id: 'a', start: '2023-11-14T22:13:20.000Z', end: '1700000000001' },
                [{ id: 'a', start: NOW, end: NOW + 1 }],
            ],
        ];
        for (const [message, expected] of cases) {
            const { requests, errors } = parseDeleteRequests(message, false, NOW, alias);
            assert.deepStrictEqual(errors, [], JSON.stringify(message));
            assert.deepStrictEqual(requests, expected, JSON.stringify(message));
        }
    });

    it('an unparsable or missing start never becomes "delete everything"', function () {
        for (const message of [
            { id: 'a', start: '31.02.2024' },
            { id: 'a', start: 'abc', end: 5 },
            { id: 'a', start: 1, end: 'abc' },
            { id: 'a', start: NaN },
            { id: 'a', start: {} },
            { id: 'a', ts: 'abc' },
            { id: 'a', ts: NaN },
            { id: 'a', state: {} },
            { id: 'a', state: { ts: null } },
            { id: 'a' },
            [{ id: 'a' }],
        ]) {
            const { requests, errors } = parseDeleteRequests(message, false, NOW);
            assert.deepStrictEqual(requests, [], JSON.stringify(message));
            assert.strictEqual(errors.length, 1, JSON.stringify(message));
        }
    });

    it('a start of 0 is a valid range start', function () {
        const { requests, errors } = parseDeleteRequests({ id: 'a', start: 0, end: 5 }, false, NOW);
        assert.deepStrictEqual(errors, []);
        assert.deepStrictEqual(requests, [{ id: 'a', start: 0, end: 5 }]);
    });

    it('an entry without an ID is refused, the others are kept', function () {
        const { requests, errors } = parseDeleteRequests(
            [{ ts: 5 }, { id: '', ts: 5 }, { id: 'b', ts: 6 }, null],
            false,
            NOW,
        );
        assert.deepStrictEqual(requests, [{ id: 'b', ts: 6 }]);
        assert.strictEqual(errors.length, 3);
    });

    it('deleteAll takes IDs only', function () {
        assert.deepStrictEqual(parseDeleteRequests({ id: 'a' }, true, NOW), { requests: [{ id: 'a' }], errors: [] });
        assert.deepStrictEqual(parseDeleteRequests([{ id: 'a' }, { id: 'alias' }], true, NOW, alias), {
            requests: [{ id: 'a' }, { id: 'real' }],
            errors: [],
        });
        const { requests, errors } = parseDeleteRequests([{}, { id: 5 }], true, NOW);
        assert.deepStrictEqual(requests, []);
        assert.strictEqual(errors.length, 2);
    });

    it('refuses a message that is no object', function () {
        for (const message of [undefined, null, 'a', 5]) {
            const { requests, errors } = parseDeleteRequests(message, false, NOW);
            assert.deepStrictEqual(requests, []);
            assert.strictEqual(errors.length, 1);
        }
    });
});

describe('Message validation: update requests', function () {
    it('parses the three shapes and normalizes the fields', function () {
        const { requests, errors } = parseUpdateRequests({
            id: 'a',
            state: { ts: '1700000000000', val: 5, q: '0', ack: 1, from: 'system.adapter.x.0' },
        });
        assert.deepStrictEqual(errors, []);
        assert.deepStrictEqual(requests, [
            { id: 'a', state: { ts: NOW, val: 5, q: 0, ack: true, from: 'system.adapter.x.0' } },
        ]);

        assert.deepStrictEqual(
            parseUpdateRequests([
                { id: 'a', state: { ts: 1 } },
                { id: 'b', state: { ts: 2, ack: false } },
            ]).requests,
            [
                { id: 'a', state: { ts: 1 } },
                { id: 'b', state: { ts: 2, ack: false } },
            ],
        );
        assert.deepStrictEqual(parseUpdateRequests({ id: 'a', state: [{ ts: 1, val: null }, { ts: 2 }] }).requests, [
            { id: 'a', state: { ts: 1, val: null } },
            { id: 'a', state: { ts: 2 } },
        ]);
    });

    it('refuses entries with an invalid timestamp, quality or without ID or state', function () {
        for (const message of [
            { id: 'a', state: { ts: 'abc' } },
            { id: 'a', state: { ts: 1, q: '0 WHERE 1=1' } },
            { id: 'a', state: { ts: 1, q: -1 } },
            { id: 'a', state: 5 },
            { id: 'a' },
            { state: { ts: 1 } },
            [{ id: 'a' }],
            [null],
            null,
        ]) {
            const { requests, errors } = parseUpdateRequests(message);
            assert.deepStrictEqual(requests, [], JSON.stringify(message));
            assert.strictEqual(errors.length, 1, JSON.stringify(message));
        }
    });
});

describe('Message validation: values per table', function () {
    it('number tables take finite numbers only', function () {
        assert.strictEqual(valueForTable('ts_number', 5), 5);
        assert.strictEqual(valueForTable('ts_number', '5.5'), 5.5);
        assert.strictEqual(valueForTable('ts_number', true), 1);
        assert.strictEqual(valueForTable('ts_counter', 7), 7);
        assert.strictEqual(valueForTable('ts_number', null), null);
        for (const val of ['abc', '0 WHERE 1=1 --', Infinity, NaN, '', {}, []]) {
            assert.ok(valueForTable('ts_number', val) instanceof Error, JSON.stringify(val));
        }
    });

    it('boolean tables take booleans, numbers and the texts for on and off', function () {
        assert.strictEqual(valueForTable('ts_bool', true), true);
        assert.strictEqual(valueForTable('ts_bool', 0), false);
        assert.strictEqual(valueForTable('ts_bool', 2), true);
        assert.strictEqual(valueForTable('ts_bool', 'true'), true);
        assert.strictEqual(valueForTable('ts_bool', 'false'), false);
        assert.strictEqual(valueForTable('ts_bool', ' Off '), false);
        assert.strictEqual(valueForTable('ts_bool', 'yes'), true);
        for (const val of ['abc', 'TRUE OR 1=1', {}, NaN]) {
            assert.ok(valueForTable('ts_bool', val) instanceof Error, JSON.stringify(val));
        }
    });

    it('string tables take everything as text, objects as JSON', function () {
        assert.strictEqual(valueForTable('ts_string', 'abc'), 'abc');
        assert.strictEqual(valueForTable('ts_string', 5), '5');
        assert.strictEqual(valueForTable('ts_string', false), 'false');
        assert.strictEqual(valueForTable('ts_string', { a: 1 }), '{"a":1}');
        assert.strictEqual(valueForTable('ts_string', null), null);
    });
});

describe('SQL string literals (PostgreSQL, SQLite, MSSQL)', function () {
    it('doubles quotes instead of removing them and drops NUL', function () {
        assert.strictEqual(quoteSqlString("it's"), "'it''s'");
        assert.strictEqual(quoteSqlString("x'; DROP TABLE ts_number; --"), "'x''; DROP TABLE ts_number; --'");
        assert.strictEqual(quoteSqlString('C:\\Users\\'), "'C:\\Users\\'");
        assert.strictEqual(quoteSqlString('a\0b'), "'ab'");
        assert.strictEqual(quoteSqlString(5), "'5'");
    });

    it('numbers that are no SQL literal become NULL', function () {
        assert.strictEqual(numberOrNull(5.5), '5.5');
        assert.strictEqual(numberOrNull(-0), '0');
        for (const val of [NaN, Infinity, -Infinity, '5', null, undefined, true]) {
            assert.strictEqual(numberOrNull(val), 'NULL', JSON.stringify(val));
        }
    });

    it('booleans: the same mapping for all dialects, as a literal each column takes', function () {
        const { boolOrNull } = require('../build/lib/sql-string');
        const vals = [true, false, 'on', 'off', ' YES ', 'no', '', '1', '0', 5, 0, 'abc', NaN, {}];
        assert.deepStrictEqual(
            vals.map(v => boolOrNull(v, true)),
            ['1', '0', '1', '0', '1', '0', '0', '1', '0', '1', '0', 'NULL', 'NULL', 'NULL'],
        );
        assert.deepStrictEqual(vals.map(v => boolOrNull(v, false)).slice(0, 4), ['TRUE', 'FALSE', 'TRUE', 'FALSE']);
        for (const [name, dialect] of Object.entries(DIALECTS)) {
            const sql = dialect
                .insert('db', 4, [
                    { table: 'ts_bool', from: 1, state: { ts: 1, val: 'off', ack: true, q: 0 } },
                    { table: 'ts_bool', from: 1, state: { ts: 2, val: 'abc', ack: true, q: 0 } },
                ])
                .join('\n');
            const off = name === 'postgresql' ? 'FALSE' : '0';
            assert.ok(sql.includes(`(4, 1, ${off},`), `${name}: ${sql}`);
            assert.ok(sql.includes('(4, 2, NULL,'), `${name}: ${sql}`);
            const update = dialect.update('db', 4, { ts: 1, val: 'on' }, undefined, 'ts_bool');
            assert.ok(update.includes(`val=${name === 'postgresql' ? 'TRUE' : '1'}`), `${name}: ${update}`);
        }
    });

    it('PostgreSQL: escape string literals, safe with standard_conforming_strings off', function () {
        const { quotePgString } = require('../build/lib/sql-string');
        const nasty = "\\'; DROP TABLE datapoints; --";
        // with a plain literal and the setting off, \' was an escaped quote and the rest SQL
        assert.strictEqual(quotePgString(nasty), "E'\\\\''; DROP TABLE datapoints; --'");
        assert.strictEqual(quotePgString('C:\\temp\\'), "E'C:\\\\temp\\\\'");
        assert.strictEqual(quotePgString("it's"), "E'it''s'");
        assert.strictEqual(quotePgString('a\0b'), "E'ab'");
        const pg = DIALECTS.postgresql;
        assert.ok(pg.getIdSelect('db', nasty).includes("name=E'\\\\''; DROP"), pg.getIdSelect('db', nasty));
        const [insert] = pg.insert('db', 4, [
            { table: 'ts_string', from: 1, state: { ts: 1, val: nasty, ack: true, q: 0 } },
        ]);
        assert.ok(insert.includes("E'\\\\''; DROP TABLE datapoints; --'"), insert);
        assert.ok(!/[^E]'\\'/.test(insert), insert);
    });

    for (const [name, dialect] of Object.entries(DIALECTS)) {
        // PostgreSQL quotes with escape string literals, see quotePgString()
        const p = name === 'postgresql' ? 'E' : '';

        it(`${name}: datapoint and source names are quoted`, function () {
            const id = "x' OR name<>'";
            assert.ok(
                dialect.getIdSelect('db', id).includes(`name=${p}'x'' OR name<>'''`),
                dialect.getIdSelect('db', id),
            );
            assert.ok(
                dialect.getIdInsert('db', id, 0).includes(`VALUES(${p}'x'' OR name<>''', 0)`),
                dialect.getIdInsert('db', id, 0),
            );
            assert.ok(
                dialect.getFromSelect('db', id).includes(`name=${p}'x'' OR name<>'''`),
                dialect.getFromSelect('db', id),
            );
            assert.ok(
                dialect.getFromInsert('db', id).includes(`VALUES(${p}'x'' OR name<>''')`),
                dialect.getFromInsert('db', id),
            );
        });

        it(`${name}: string values keep their quotes, Infinity is stored as NULL`, function () {
            const rows = [
                { table: 'ts_string', from: 1, state: { ts: 1, val: "it's", ack: true, q: 0 } },
                { table: 'ts_number', from: 1, state: { ts: 2, val: Infinity, ack: true, q: 0 } },
                { table: 'ts_number', from: 1, state: { ts: 3, val: -Infinity, ack: true, q: 0 } },
                { table: 'ts_number', from: 1, state: { ts: 4, val: 1.5, ack: true, q: 0 } },
                { table: 'ts_counter', from: 1, state: { ts: 5, val: NaN } },
            ];
            const sql = dialect.insert('db', 4, rows).join('\n');
            assert.ok(sql.includes(`${p}'it''s'`), sql);
            assert.ok(!/Infinity|NaN/.test(sql), sql);
            assert.ok(sql.includes('(4, 2, NULL,'), sql);
            assert.ok(sql.includes('(4, 3, NULL,'), sql);
            assert.ok(sql.includes('(4, 4, 1.5,'), sql);
            assert.ok(sql.includes('(4, 5, NULL)'), sql);
            // the caller's rows are not changed
            assert.strictEqual(rows[0].state.val, "it's");
            assert.strictEqual(rows[1].state.val, Infinity);

            const update = dialect.update('db', 4, { ts: 1, val: Infinity }, undefined, 'ts_number');
            assert.ok(update.includes('val=NULL'), update);
            assert.ok(!update.includes('_from'), update);
            const updateStr = dialect.update('db', 4, { ts: 1, val: "it's" }, 2, 'ts_string');
            assert.ok(updateStr.includes(`val=${p}'it''s'`), updateStr);
            assert.ok(updateStr.includes('_from=2'), updateStr);
        });
    }
});
