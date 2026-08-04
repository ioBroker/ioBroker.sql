/* eslint-env mocha */
const assert = require('node:assert');

// Pfad ggf. anpassen: Modul muss finishAggregationForIntegralEx exportieren
const { finishAggregationForIntegralEx } = require('../build/lib/aggregate');

function makeIntervals(start, step, count) {
    const arr = [];
    for (let i = 0; i < count; i++) {
        arr.push({ start: start + i * step, end: start + (i + 1) * step });
    }
    return arr;
}

describe('finishAggregationForIntegralEx', () => {
    it('returns zeros when no pre‑interval data is available', () => {
        const intervals = makeIntervals(0, 1000, 2); // [0..1000), [1000..2000)
        const options = {
            timeIntervals: intervals,
            // integralDataPoints muss Länge = intervals.length + 2 haben (pre + buckets + post)
            integralDataPoints: [[], [], [], []],
            logDebug: false,
        };

        finishAggregationForIntegralEx(options);

        assert.strictEqual(options.result.length, 2);
        // Check midpoints
        assert.strictEqual(options.result[0].ts, 0 + Math.round((1000 - 0) / 2)); // 500
        assert.strictEqual(options.result[1].ts, 1000 + Math.round((2000 - 1000) / 2)); // 1500
        // Werte bleiben 0
        assert.strictEqual(options.result[0].val, 0);
        assert.strictEqual(options.result[1].val, 0);
    });

    it('fills start/end points via interpolation and yields positive integrals (increasing trend)', () => {
        const intervals = makeIntervals(0, 1000, 2);
        const buckets = [
            // pre
            [{ ts: -1, val: 0 }],
            // interval 0
            [],
            // interval 1
            [],
            // post
            [{ ts: 2000, val: 10 }],
        ];
        const options = {
            timeIntervals: intervals,
            integralDataPoints: buckets,
            logDebug: false,
        };

        finishAggregationForIntegralEx(options);

        assert.strictEqual(options.result.length, 2);
        // Buckets wurden mit Start (start) und Ende (end-1) befüllt
        for (let i = 0; i < intervals.length; i++) {
            const b = options.integralDataPoints[i + 1];
            assert.ok(Array.isArray(b) && b.length > 0);
            assert.strictEqual(b[0].ts, intervals[i].start);
            assert.strictEqual(b[b.length - 1].ts, intervals[i].end - 1);
        }
        // Midpoints
        assert.strictEqual(options.result[0].ts, 500);
        assert.strictEqual(options.result[1].ts, 1500);

        // Steigender Verlauf => Integrale > 0 und zweites Intervall größer als erstes
        assert.strictEqual(typeof options.result[0].val, 'number');
        assert.strictEqual(typeof options.result[1].val, 'number');
        assert.ok(options.result[0].val > 0, `${options.result[0].val} > 0`);
        assert.ok(options.result[1].val > options.result[0].val, `${options.result[1].val} > options.result[0].val`);
    });

    it('yields equal integrals for a constant value across all intervals', () => {
        const intervals = makeIntervals(0, 1000, 2);
        const buckets = [
            // pre: konstanter Wert 5 vor Start
            [{ ts: -1, val: 5 }],
            // interval 0
            [],
            // interval 1
            [],
            // post: gleicher Wert nach Ende
            [{ ts: 2000, val: 5 }],
        ];
        const options = {
            timeIntervals: intervals,
            integralDataPoints: buckets,
            logDebug: false,
        };

        finishAggregationForIntegralEx(options);

        assert.strictEqual(options.result.length, 2);
        // Beide Intervalle sollten das gleiche Integral liefern (konstante Funktion)
        assert.strictEqual(typeof options.result[0].val, 'number');
        assert.strictEqual(typeof options.result[1].val, 'number');
        assert.ok(
            Math.abs(options.result[0].val - options.result[1].val) < 1e-9,
            `${Math.abs(options.result[0].val - options.result[1].val)} < 1e-9`,
        );
    });

    it('computes exact integrals for a known linear function f(t) = 2t + 3', () => {
        const intervals = makeIntervals(0, 1000, 2);
        const f = t => 2 * t + 3;

        // Dataset: for each bucket provide only start and (end - 1) points
        const buckets = [
            [], // pre
            [
                { ts: intervals[0].start, val: f(intervals[0].start) }, // f(0) = 3
                { ts: intervals[0].end - 1, val: f(intervals[0].end - 1) }, // f(999) = 2001
            ],
            [
                { ts: intervals[1].start, val: f(intervals[1].start) }, // f(1000) = 2003
                { ts: intervals[1].end - 1, val: f(intervals[1].end - 1) }, // f(1999) = 4001
            ],
            [], // post
        ];

        const options = {
            timeIntervals: intervals,
            integralDataPoints: buckets,
            logDebug: false,
        };

        // Exact integrals via trapezoidal rule over [start, end - 1]
        const expected = intervals.map(iv => {
            const S = iv.start;
            const E1 = iv.end - 1;
            return (0.5 * (f(S) + f(E1)) * (E1 - S)) / 3_600_000; // in hours
        });
        // Expected: [0.278055, 0.833055]

        finishAggregationForIntegralEx(options);

        assert.strictEqual(options.result.length, 2);
        assert.ok(
            Math.abs(options.result[0].val - expected[0]) < 0.00000001,
            `${Math.abs(options.result[0].val - expected[0])} < 0.00000001`,
        ); // 0.278055
        assert.ok(
            Math.abs(options.result[1].val - expected[1]) < 0.00000001,
            `${Math.abs(options.result[1].val - expected[1])} < 0.00000001`,
        ); // 0.833055
    });
});
