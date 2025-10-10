const expect = require('chai').expect;
const commons = require('../build/lib/aggregate');

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
                expect(result.result).to.be.equal(700);
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
                expect(result.result).to.be.equal(300);
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
                expect(result.result).to.be.equal(550);
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
                expect(result.result).to.be.equal(800);
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
                expect(result.result).to.be.equal(750);
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
                expect(result.result).to.be.equal(900);
                done();
            },
            log,
        };

        commons.sendResponseCounter(adapter, {}, { start: 5, end: 95 }, timeSeries);
    });
});
