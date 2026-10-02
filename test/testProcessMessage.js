const assert = require('node:assert');

// `processMessage` is reached from the message box as soon as the adapter is ready, while `main()`
// is still awaiting `system.config` and `sqlFuncs` is therefore still null. Rather than booting a
// real adapter, the method is called on a minimal stand-in: it only touches `this.sqlFuncs`,
// `this.log` and `this.sendTo` on the guarded path.
const { SqlAdapter } = require('../build/main');

function makeStub(overrides = {}) {
    const stub = {
        sqlFuncs: null,
        sent: [],
        warnings: [],
        log: {
            warn: text => stub.warnings.push(text),
            error: text => stub.warnings.push(text),
            debug: () => {},
            info: () => {},
        },
        sendTo(from, command, result) {
            stub.sent.push({ from, command, result });
        },
        ...overrides,
    };
    return stub;
}

function send(stub, command) {
    SqlAdapter.prototype.processMessage.call(stub, {
        command,
        from: 'system.adapter.test.0',
        message: {},
        callback: { id: 1, ack: false, time: Date.now(), message: {} },
    });
}

// Every command the README documents as reaching the database.
const DB_COMMANDS = [
    'getHistory',
    'getCounter',
    'destroy',
    'query',
    'update',
    'delete',
    'deleteAll',
    'deleteRange',
    'storeState',
    'getRawEntries',
    'getDatapoints',
    'getDpOverview',
];

describe('Test processMessage before the adapter is initialized', function () {
    for (const command of DB_COMMANDS) {
        it(`answers "${command}" with an error instead of throwing`, function () {
            const stub = makeStub();

            // Before the fix this threw
            // "TypeError: Cannot read properties of null (reading 'getIdSelect')"
            // and terminated the adapter with UNCAUGHT_EXCEPTION (issue #527).
            assert.doesNotThrow(() => send(stub, command));

            assert.strictEqual(stub.sent.length, 1, 'the message is answered');
            assert.strictEqual(stub.sent[0].command, command);
            assert.strictEqual(stub.sent[0].result.error, 'Adapter is not initialized yet');
            assert.deepStrictEqual(stub.sent[0].result.result, [], 'shape stays compatible with getHistory consumers');
            assert.strictEqual(stub.warnings.length, 1, 'and logged once');
        });
    }

    it('still answers "features", which needs no database', function () {
        const stub = makeStub();

        send(stub, 'features');

        assert.strictEqual(stub.sent.length, 1);
        assert.ok(
            Array.isArray(stub.sent[0].result.supportedFeatures),
            `expected the real feature list, got ${JSON.stringify(stub.sent[0].result)}`,
        );
        assert.strictEqual(stub.warnings.length, 0, 'no warning for a command that does not need the DB');
    });

    it('does not intercept the database commands once the dialect is known', function () {
        let called = null;
        const stub = makeStub({
            sqlFuncs: {},
            getHistorySql: msg => (called = msg.command),
        });

        send(stub, 'getHistory');

        assert.strictEqual(called, 'getHistory', 'the real handler runs');
        assert.strictEqual(stub.sent.length, 0, 'the guard did not answer');
        assert.strictEqual(stub.warnings.length, 0);
    });
});
