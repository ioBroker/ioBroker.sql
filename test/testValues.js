const assert = require('node:assert');

// From its own module, never from build/main: that pulls in @iobroker/adapter-core, which calls
// process.exit(10) when js-controller cannot be resolved.
const { isSameValue } = require('../build/lib/values');

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
