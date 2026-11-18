// The streamed PostgreSQL read (#516) feeds the rows of an aggregated read to aggregation() in batches
// instead of as one array. This checks, against the real @iobroker/aggregate, that the answer stays
// the same for every aggregate the adapter streams.
const assert = require('node:assert');
const { initAggregate, aggregation, finishAggregation } = require('@iobroker/aggregate');

// The modes streamed by #canStream() in src/main.ts: everything except none, onchange and integralTotal
const STREAMED = ['average', 'min', 'max', 'total', 'count', 'minmax', 'percentile', 'quantile', 'integral'];
const BATCH_SIZES = [1, 3, 7, 64];

const START = 1_700_000_000_000;
const END = START + 600_000;

// Rows the way the PostgreSQL query returns them for an aggregated read with a start: sorted by ts, the
// last row before the start and the first row after the end. Both border rows lie several intervals away,
// which is the case aggregation() treats specially. Fixed pseudo-random values, gaps (null) included.
function makeRows() {
    let seed = 516;
    const random = () => {
        seed = (seed * 1103515245 + 12345) % 2147483648;
        return seed / 2147483648;
    };
    const rows = [{ ts: START - 300_000, val: 7.25, ack: true }];
    let ts = START + 500;
    while (ts < END) {
        const val = random() < 0.08 ? null : Math.round(random() * 1000) / 10;
        rows.push({ ts, val, ack: true });
        ts += 300 + Math.floor(random() * 4000);
    }
    rows.push({ ts: END + 250_000, val: 3.5, ack: true });
    return rows;
}

function optionsFor(aggregate, extra) {
    return {
        start: START,
        end: END,
        count: 12,
        limit: 2000,
        aggregate,
        percentile: aggregate === 'percentile' ? 75 : undefined,
        quantile: aggregate === 'quantile' ? 0.3 : undefined,
        integralUnit: aggregate === 'integral' ? 60 : undefined,
        integralInterpolation: aggregate === 'integral' ? 'linear' : null,
        ...extra,
    };
}

// What sendResponse() computes for the whole array (the buffered read)
function whole(options, rows) {
    const opts = initAggregate({ ...options }, 'sql.0.test');
    aggregation(opts, structuredClone(rows));
    finishAggregation(opts);
    return opts.result;
}

// What the streamed read computes: the same rows, batch by batch
function batched(options, rows, size, reverse) {
    const opts = initAggregate({ ...options }, 'sql.0.test');
    const copy = structuredClone(rows);
    const batches = [];
    for (let i = 0; i < copy.length; i += size) {
        batches.push(copy.slice(i, i + size));
    }
    if (reverse) {
        batches.reverse();
    }
    for (const batch of batches) {
        aggregation(opts, batch);
    }
    finishAggregation(opts);
    return { result: opts.result, batches: batches.length };
}

describe('Streamed aggregation (#516): batches give the same answer as one array', () => {
    const rows = makeRows();

    it('test data has gaps and border rows far outside the range', () => {
        assert.ok(rows.length > 150, `only ${rows.length} rows`);
        assert.ok(
            rows.some(r => r.val === null),
            'no gaps',
        );
        assert.ok(rows[0].ts < START - 120_000, 'no far border row before the start');
        assert.ok(rows[rows.length - 1].ts > END + 120_000, 'no far border row after the end');
    });

    for (const aggregate of STREAMED) {
        it(`${aggregate}: equal step`, () => {
            const expected = whole(optionsFor(aggregate), rows);
            assert.ok(expected.length > 0, 'no result to compare');
            for (const size of BATCH_SIZES) {
                const { result, batches } = batched(optionsFor(aggregate), rows, size);
                assert.ok(batches > 1, `batch size ${size} did not split the rows`);
                assert.deepStrictEqual(result, expected, `batch size ${size}`);
            }
        });

        it(`${aggregate}: interval borders from the client`, () => {
            const borders = [START, START + 60_000, START + 200_000, START + 210_000, START + 450_000, END];
            const expected = whole(optionsFor(aggregate, { intervalBorders: borders }), rows);
            assert.ok(expected.length > 0, 'no result to compare');
            for (const size of BATCH_SIZES) {
                const { result } = batched(optionsFor(aggregate, { intervalBorders: borders }), rows, size);
                assert.deepStrictEqual(result, expected, `batch size ${size}`);
            }
        });
    }

    it('boolean values', () => {
        const boolRows = rows.map((r, i) => ({ ...r, val: r.val === null ? null : i % 3 === 0 }));
        for (const aggregate of ['average', 'max', 'total', 'count', 'minmax']) {
            const expected = whole(optionsFor(aggregate), boolRows);
            const { result } = batched(optionsFor(aggregate), boolRows, 7);
            assert.deepStrictEqual(result, expected, aggregate);
        }
    });

    it('control: the comparison sees a different order of the rows', () => {
        // Without this, a comparison that cannot tell results apart would pass every test above
        const expected = whole(optionsFor('minmax'), rows);
        const { result } = batched(optionsFor('minmax'), rows, 7, true);
        assert.notDeepStrictEqual(result, expected);
    });
});
