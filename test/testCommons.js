const assert = require('node:assert');
const commons = require('@iobroker/aggregate');

describe('Test Common functions', function () {
    const log = {
        info: t => console.log(t),
        debug: t => console.log(t),
        error: t => console.error(t),
        warn: t => console.warn(t),
    };

    it('Test Common functions: counter 1', function (done) {
        const timeSeries = [
            { ts: 0, val: 100 },
            { ts: 10, val: 200 },
            { ts: 40, val: 500 },
            { ts: 50, val: 0 },
            { ts: 90, val: 400 },
            { ts: 100, val: 0 },
            { ts: 110, val: 100 },
        ];

        const adapter = {
            sendTo: function (from, command, result, callback) {
                assert.strictEqual(result.result, 700);
                done();
            },
            log,
        };

        commons.sendResponseCounter(adapter, {}, { start: 10, end: 100 }, timeSeries);
    });

    it('Test Common functions: counter 2', function (done) {
        const timeSeries = [
            { ts: 10, val: 200 },
            { ts: 40, val: 500 },
        ];

        const adapter = {
            sendTo: function (from, command, result, callback) {
                assert.strictEqual(result.result, 300);
                done();
            },
            log,
        };

        commons.sendResponseCounter(adapter, {}, { start: 10, end: 40 }, timeSeries);
    });

    it('Test Common functions: counter 3', function (done) {
        const timeSeries = [
            { ts: 0, val: 100 },
            { ts: 10, val: 200 },
            { ts: 40, val: 500 },
            { ts: 50, val: 0 },
            { ts: 90, val: 400 },
        ];

        const adapter = {
            sendTo: function (from, command, result, callback) {
                assert.strictEqual(result.result, 550);
                done();
            },
            log,
        };

        commons.sendResponseCounter(adapter, {}, { start: 5, end: 70 }, timeSeries);
    });

    it('Test Common functions: counter 4', function (done) {
        const timeSeries = [
            { ts: 0, val: 100 },
            { ts: 10, val: 200 },
            { ts: 40, val: 500 },
            { ts: 50, val: 0 },
            { ts: 90, val: 400 },
            { ts: 100, val: 0 },
            { ts: 110, val: 100 },
        ];

        const adapter = {
            sendTo: function (from, command, result, callback) {
                assert.strictEqual(result.result, 800);
                done();
            },
            log,
        };

        commons.sendResponseCounter(adapter, {}, { start: 5, end: 105 }, timeSeries);
    });

    it('Test Common functions: counter 5', function (done) {
        const timeSeries = [
            { ts: 0, val: 100 },
            { ts: 10, val: 200 },
            { ts: 40, val: 500 },
            { ts: 50, val: 0 },
            { ts: 90, val: 400 },
            { ts: 100, val: 0 },
        ];

        const adapter = {
            sendTo: function (from, command, result, callback) {
                assert.strictEqual(result.result, 750);
                done();
            },
            log,
        };

        commons.sendResponseCounter(adapter, {}, { start: 5, end: 95 }, timeSeries);
    });

    it('Test Common functions: counter 6', function (done) {
        const timeSeries = [
            { ts: 0, val: 100 },
            { ts: 10, val: 0 },
            { ts: 20, val: 100 },
            { ts: 40, val: 500 },
            { ts: 50, val: 0 },
            { ts: 90, val: 400 },
            { ts: 100, val: 0 },
        ];

        const adapter = {
            sendTo: function (from, command, result, callback) {
                assert.strictEqual(result.result, 900);
                done();
            },
            log,
        };

        commons.sendResponseCounter(adapter, {}, { start: 5, end: 95 }, timeSeries);
    });

    // "onchange" ("raw" in the e-charts UI) must be passed through untouched. aggregationLogic() has no
    // branch for it, so running it through the bucket aggregation returns one entry per interval with
    // val === null - i.e. an empty chart (issue #522).
    it('Test Common functions: aggregate onchange returns the raw values', function (done) {
        const start = 1000;
        const end = 2000;
        const timeSeries = [
            { ts: 1000, val: 1 },
            { ts: 1200, val: 2 },
            { ts: 1500, val: 3 },
            { ts: 1800, val: 4 },
            { ts: 2000, val: 5 },
        ];

        const adapter = {
            sendTo: function (from, command, result) {
                assert.deepStrictEqual(result.result, [
                    { ts: 1000, val: 1 },
                    { ts: 1200, val: 2 },
                    { ts: 1500, val: 3 },
                    { ts: 1800, val: 4 },
                    { ts: 2000, val: 5 },
                ]);
                assert.strictEqual(result.step, 0);
                done();
            },
            log,
        };

        commons.sendResponse(
            adapter,
            { from: 'system.adapter.test.0', command: 'getHistory' },
            'test.0.value',
            { start, end, aggregate: 'onchange', count: 500, limit: 2000, ignoreNull: false },
            timeSeries,
            Date.now(),
        );
    });

    it('Test Common functions: aggregate onchange draws steps to the borders', function (done) {
        const start = 1000;
        const end = 2000;
        // the value before start and the value after end are delivered by the UNION sub-queries of getHistory
        const timeSeries = [
            { ts: 800, val: 1 },
            { ts: 1200, val: 2 },
            { ts: 1800, val: 3 },
            { ts: 2200, val: 4 },
        ];

        const adapter = {
            sendTo: function (from, command, result) {
                // 800 is cut off and becomes the step value at start, 2200 becomes the step value at end
                assert.deepStrictEqual(result.result, [
                    { ts: 1000, val: 1 },
                    { ts: 1200, val: 2 },
                    { ts: 1800, val: 3 },
                    { ts: 2000, val: 4 },
                ]);
                done();
            },
            log,
        };

        commons.sendResponse(
            adapter,
            { from: 'system.adapter.test.0', command: 'getHistory' },
            'test.0.value',
            { start, end, aggregate: 'onchange', count: 500, limit: 2000, ignoreNull: false },
            timeSeries,
            Date.now(),
        );
    });
});
