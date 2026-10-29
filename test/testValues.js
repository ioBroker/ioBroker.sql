const assert = require('node:assert');

// From its own module, never from build/main: that pulls in @iobroker/adapter-core, which calls
// process.exit(10) when js-controller cannot be resolved.
const { isSameValue, parseLimit } = require('../build/lib/values');

describe('Test isSameValue', function () {
    it('treats equal primitives as unchanged', function () {
        assert.strictEqual(isSameValue(12.3, 12.3), true);
        assert.strictEqual(isSameValue('on', 'on'), true);
        assert.strictEqual(isSameValue(true, true), true);
        assert.strictEqual(isSameValue(null, null), true);
        assert.strictEqual(isSameValue(undefined, undefined), true);
        assert.strictEqual(isSameValue(0, 0), true);
    });

    it('treats different primitives as changed', function () {
        assert.strictEqual(isSameValue(12.3, 12.4), false);
        assert.strictEqual(isSameValue('on', 'off'), false);
        assert.strictEqual(isSameValue(true, false), false);
    });

    it('treats a transition to or from null as a change', function () {
        // the old condition had an explicit carve-out for this; the value comparison covers it
        assert.strictEqual(isSameValue(null, 5), false);
        assert.strictEqual(isSameValue(5, null), false);
        assert.strictEqual(isSameValue(null, 0), false);
        assert.strictEqual(isSameValue(0, null), false);
    });

    it('does not collapse a string and a number that look alike', function () {
        // they are stored in different tables - ts_string and ts_number - so this is a real
        // transition, not a repeat of the same value
        assert.strictEqual(isSameValue('12.3', 12.3), false);
        assert.strictEqual(isSameValue(0, false), false);
        assert.strictEqual(isSameValue('', null), false);
    });

    it('treats two NaN readings as unchanged', function () {
        // NaN === NaN is false, but a sensor reporting NaN twice has not changed
        assert.strictEqual(isSameValue(NaN, NaN), true);
        assert.strictEqual(isSameValue(NaN, 5), false);
        assert.strictEqual(isSameValue(5, NaN), false);
    });

    it('compares objects by content, because that is how they are stored', function () {
        // ts_string holds JSON.stringify()ed values, so two structurally equal objects are the same
        // row. A strict !== would compare references and defeat changesOnly for these datapoints.
        assert.strictEqual(isSameValue({ a: 1, b: [2, 3] }, { a: 1, b: [2, 3] }), true);
        assert.strictEqual(isSameValue({ a: 1 }, { a: 2 }), false);
        assert.strictEqual(isSameValue([1, 2], [1, 2]), true);
        assert.strictEqual(isSameValue([1, 2], [2, 1]), false);
    });

    it('survives a circular structure instead of throwing', function () {
        const circular = { name: 'x' };
        circular.self = circular;

        // such a value cannot be stored either - report a change and let the write path raise the
        // real error rather than crashing the comparison
        assert.strictEqual(isSameValue(circular, circular), true, 'identity still short-circuits');
        assert.doesNotThrow(() => isSameValue(circular, { name: 'x' }));
        assert.strictEqual(isSameValue(circular, { name: 'x' }), false);
    });

    it('answers the case from issue #295', function () {
        // alias read converter: Math.round(val * 10) / 10
        const convert = val => Math.round(val * 10) / 10;

        // the source changes, the converted value does not - this used to be stored anyway because
        // the alias carries the source's lc, so ts === lc arrived at the adapter
        assert.strictEqual(isSameValue(convert(12.34), convert(12.31)), true);
        assert.strictEqual(isSameValue(convert(12.34), convert(12.36)), false);
    });
});

describe('Test parseLimit', function () {
    // "ignore values below 0" was turned into "no limit" by `parseFloat(x) || null`
    it('keeps 0 as a limit', function () {
        assert.strictEqual(parseLimit('0'), 0);
        assert.strictEqual(parseLimit(0), 0);
        assert.strictEqual(parseLimit('-0.5'), -0.5);
        assert.strictEqual(parseLimit(12.5), 12.5);
        assert.strictEqual(parseLimit('12,5'), 12.5, 'decimal comma');
    });

    it('returns null for no number', function () {
        assert.strictEqual(parseLimit('abc'), null);
        assert.strictEqual(parseLimit(''), null);
        assert.strictEqual(parseLimit(null), null);
        assert.strictEqual(parseLimit(undefined), null);
        assert.strictEqual(parseLimit(NaN), null);
        assert.strictEqual(parseLimit(Infinity), null);
    });
});

describe('Test the write buffer helpers (#587)', function () {
    const { splitBatch, requeue } = require('../build/lib/write-buffer');
    const row = (table, ts) => ({ table, state: { val: ts, ts }, from: 0 });

    it('splitBatch() makes one group per table and at most 500 rows', function () {
        const list = [row('ts_number', 1), row('ts_counter', 2), row('ts_number', 3)];
        for (let i = 0; i < 1001; i++) {
            list.push(row('ts_string', 10 + i));
        }
        const groups = splitBatch(list);
        assert.deepStrictEqual(
            groups.map(g => `${g[0].table}:${g.length}`),
            ['ts_number:2', 'ts_counter:1', 'ts_string:500', 'ts_string:500', 'ts_string:1'],
        );
        assert.deepStrictEqual(
            groups[0].map(r => r.state.ts),
            [1, 3],
            'the order within a table is kept',
        );
    });

    it('requeue() puts the unwritten rows first and drops only the oldest of them beyond the cap', function () {
        assert.deepStrictEqual(requeue([3, 4], [1, 2], 10), { list: [1, 2, 3, 4], dropped: 0 });
        assert.deepStrictEqual(requeue([4, 5], [1, 2, 3], 4), { list: [2, 3, 4, 5], dropped: 1 });
        // the values logged in the meantime are never dropped
        assert.deepStrictEqual(requeue([3, 4, 5], [1, 2], 2), { list: [3, 4, 5], dropped: 2 });
        assert.deepStrictEqual(requeue(undefined, [1], 5), { list: [1], dropped: 0 });
    });
});
