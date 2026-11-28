const assert = require('node:assert');

// Needs build/: the module of its own, not build/main.js
const { aggregate, lastChange, readThroughGetHistory, MAX_SCAN, MAX_VALUES } = require('../build/lib/standardHistory');

const H = 3_600_000;
/** Local midnight of 2026-11-20 */
const DAY = new Date(2026, 10, 20).getTime();
const NOW = DAY + 24 * H;

/** A source as the adapter reads its values: the oldest count in [start, end], or the newest count up to end */
function source(values, unit) {
    const queries = [];
    return {
        queries,
        read: (_id, query) => {
            queries.push(query);
            const sorted = [...values].sort((a, b) => a.ts - b.ts);
            if (query.start !== undefined) {
                return Promise.resolve(
                    sorted.filter(v => v.ts >= query.start && v.ts <= query.end).slice(0, query.count),
                );
            }
            return Promise.resolve(sorted.filter(v => v.ts <= query.end).slice(-query.count));
        },
        unit: () => Promise.resolve(unit),
    };
}

/** A door: closed before the day, open 8-9, 12-12:30 and from 20:00 on, closed again (relogged) at 12:30 */
const door = [
    { ts: DAY - 2 * H, val: false },
    { ts: DAY + 8 * H, val: true },
    { ts: DAY + 9 * H, val: false },
    { ts: DAY + 12 * H, val: true },
    { ts: DAY + 12.5 * H, val: false },
    { ts: DAY + 13 * H, val: false },
    { ts: DAY + 20 * H, val: true },
    { ts: DAY + 21 * H, val: null },
];

/** A counter that was reset to 0 at 12:00 */
const counter = [
    { ts: DAY - H, val: 100 },
    { ts: DAY + 6 * H, val: 110 },
    { ts: DAY + 11 * H, val: 125 },
    { ts: DAY + 12 * H, val: 0 },
    { ts: DAY + 18 * H, val: 7 },
];

describe('Test the standard commands aggregate and lastChange', function () {
    const day = { start: DAY, end: DAY + 24 * H };

    it('answers min, max with the time, avg, sum, first, last and the unit', async function () {
        const temps = source(
            [
                { ts: DAY - H, val: 30 },
                { ts: DAY + H, val: 20 },
                { ts: DAY + 2 * H, val: 25.5 },
                { ts: DAY + 3 * H, val: '18' },
                { ts: DAY + 4 * H, val: null },
            ],
            '°C',
        );
        const ask = fn => aggregate({ id: 'temp', fn, ...day }, temps, NOW);
        assert.deepStrictEqual(await ask('min'), { value: 18, count: 3, ts: DAY + 3 * H, unit: '°C' });
        assert.deepStrictEqual(await ask('max'), { value: 25.5, count: 3, ts: DAY + 2 * H, unit: '°C' });
        assert.deepStrictEqual(await ask('avg'), { value: 21.167, count: 3, unit: '°C' });
        assert.deepStrictEqual(await ask('sum'), { value: 63.5, count: 3, unit: '°C' });
        assert.deepStrictEqual(await ask('first'), { value: 20, count: 3, ts: DAY + H, unit: '°C' });
        assert.deepStrictEqual(await ask('last'), { value: '18', count: 3, ts: DAY + 3 * H, unit: '°C' });
        assert.deepStrictEqual(await ask('count'), { value: 3, count: 3 });
    });

    it('counts how often the state became the value, and how long it had it', async function () {
        const doors = source(door);
        assert.deepStrictEqual(await aggregate({ id: 'door', fn: 'count', value: true, ...day }, doors, NOW), {
            value: 3,
            count: 6,
        });
        // "false" and 0 are the same as false; the relogged false at 13:00 is no new closing
        assert.deepStrictEqual(await aggregate({ id: 'door', fn: 'count', value: 'false', ...day }, doors, NOW), {
            value: 2,
            count: 6,
        });
        // open 1 h + 0.5 h + from 20:00 until the gap at 21:00 (held: the null is ignored) until the end of the day
        assert.deepStrictEqual(await aggregate({ id: 'door', fn: 'duration', value: 1, ...day }, doors, NOW), {
            value: (1 + 0.5 + 4) * 3600,
            count: 6,
            unit: 's',
        });
        // the end of a period that has not ended yet is now
        assert.deepStrictEqual(
            await aggregate({ id: 'door', fn: 'duration', value: true, ...day }, doors, DAY + 8.5 * H),
            { value: 1800, count: 6, unit: 's' },
        );
    });

    it('sums the rise of a counter over a reset, from the value before start', async function () {
        const answer = await aggregate({ id: 'energy', fn: 'delta', ...day }, source(counter, 'kWh'), NOW);
        assert.deepStrictEqual(answer, { value: 10 + 15 + 0 + 7, count: 4, unit: 'kWh' });
    });

    it('integrates in value × hours from start on, the value before start held', async function () {
        const power = source(
            [
                { ts: DAY - H, val: 100 },
                { ts: DAY + 2 * H, val: 400 },
                { ts: DAY + 3 * H, val: 0 },
            ],
            'W',
        );
        const answer = await aggregate({ id: 'power', fn: 'integral', ...day }, power, NOW);
        assert.deepStrictEqual(answer, { value: 2 * 100 + 400, count: 2, unit: 'Wh' });
    });

    it('takes times as ms, numeric text and date text in local time, end default now', async function () {
        const doors = source(door);
        await aggregate({ id: 'door', fn: 'count', start: '2026-11-20', end: String(DAY + H) }, doors, NOW);
        await aggregate({ id: 'door', fn: 'count', start: '2026-11-20T08:00' }, doors, NOW);
        assert.deepStrictEqual(doors.queries, [
            { start: DAY, end: DAY + H, count: MAX_VALUES + 1 },
            { start: DAY + 8 * H, end: NOW, count: MAX_VALUES + 1 },
        ]);
    });

    it('refuses wrong parameters and too many values', async function () {
        const doors = source(door);
        for (const message of [
            null,
            { fn: 'min', ...day },
            { id: 'door', ...day },
            { id: 'door', fn: 'median', ...day },
            { id: 'door', fn: 'min' },
            { id: 'door', fn: 'min', start: 'yesterday' },
            { id: 'door', fn: 'min', start: DAY + H, end: DAY },
            { id: 'door', fn: 'duration', ...day },
        ]) {
            const answer = await aggregate(message, doors, NOW);
            assert.strictEqual(answer.reason, 'BAD_REQUEST', JSON.stringify(message));
            assert.strictEqual(typeof answer.error, 'string');
        }
        const many = Array.from({ length: MAX_VALUES + 1 }, (_, i) => ({ ts: DAY + i, val: i }));
        assert.strictEqual(
            (await aggregate({ id: 'x', fn: 'max', ...day }, source(many), NOW)).reason,
            'TOO_MANY_VALUES',
        );
        const broken = { read: () => Promise.reject(new Error('no database')) };
        assert.deepStrictEqual(await aggregate({ id: 'x', fn: 'max', ...day }, broken, NOW), { error: 'no database' });
    });

    it('finds the last change, to a value, before a time', async function () {
        const doors = source(door);
        assert.deepStrictEqual(await lastChange({ id: 'door' }, doors, NOW), { ts: DAY + 20 * H, val: true });
        // closed since 12:30, the relogged value at 13:00 is no change
        assert.deepStrictEqual(await lastChange({ id: 'door', value: false }, doors, NOW), {
            ts: DAY + 12.5 * H,
            val: false,
        });
        assert.deepStrictEqual(await lastChange({ id: 'door', value: true, before: DAY + 11 * H }, doors, NOW), {
            ts: DAY + 8 * H,
            val: true,
        });
        // the first stored value
        assert.deepStrictEqual(await lastChange({ id: 'door', value: false, before: DAY }, doors, NOW), {
            ts: DAY - 2 * H,
            val: false,
        });
        assert.strictEqual((await lastChange({ id: 'door', value: 'ajar' }, doors, NOW)).reason, 'NOT_FOUND');
        assert.strictEqual((await lastChange({ id: 'door', before: 'never' }, doors, NOW)).reason, 'BAD_REQUEST');
        assert.strictEqual((await lastChange({}, doors, NOW)).reason, 'BAD_REQUEST');
    });

    it('reads backwards page by page and stops after MAX_SCAN values', async function () {
        const values = [{ ts: 1000, val: 'off' }];
        for (let i = 1; i <= 2500; i++) {
            values.push({ ts: 1000 + i, val: 'on' });
        }
        const paged = source(values);
        assert.deepStrictEqual(await lastChange({ id: 'x' }, paged, NOW), { ts: 1001, val: 'on' });
        assert.deepStrictEqual(paged.queries, [
            { end: NOW, count: 1000 },
            { end: 1000 + 1501 - 1, count: 1000 },
            { end: 1000 + 501 - 1, count: 1000 },
        ]);
        const endless = Array.from({ length: MAX_SCAN + 10 }, (_, i) => ({ ts: i + 1, val: 1 }));
        assert.deepStrictEqual(await lastChange({ id: 'x' }, source(endless), NOW), {
            ts: 11,
            val: 1,
            limited: true,
        });
    });

    it('reads through getHistory with aggregate none', async function () {
        const messages = [];
        const read = readThroughGetHistory(message => {
            messages.push(message);
            return Promise.resolve({ result: [{ ts: 5, val: 1 }], step: null, error: null });
        });
        assert.deepStrictEqual(await read('a.0.x', { start: 1, end: 9, count: 3 }), [{ ts: 5, val: 1 }]);
        await read('a.0.x', { end: 9, count: 10 });
        assert.deepStrictEqual(JSON.parse(JSON.stringify(messages)), [
            {
                id: 'a.0.x',
                options: {
                    start: 1,
                    end: 9,
                    count: 3,
                    limit: 3,
                    aggregate: 'none',
                    returnNewestEntries: false,
                    removeBorderValues: true,
                },
            },
            {
                id: 'a.0.x',
                options: {
                    end: 9,
                    count: 10,
                    limit: 10,
                    aggregate: 'none',
                    returnNewestEntries: true,
                    removeBorderValues: true,
                },
            },
        ]);
        await assert.rejects(
            readThroughGetHistory(() => Promise.resolve({ result: [], error: 'Not enabled' }))('x', {
                end: 1,
                count: 1,
            }),
            /Not enabled/,
        );
        await assert.rejects(
            readThroughGetHistory(() => Promise.resolve(new Error('Timeout')))('x', {
                end: 1,
                count: 1,
            }),
            /Timeout/,
        );
    });
});
