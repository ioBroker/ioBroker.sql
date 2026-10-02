const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');

// Imported from its own module, NOT from build/main: requiring main.js pulls in
// @iobroker/adapter-core, which calls process.exit(10) at load time when js-controller cannot be
// resolved - and js-controller is not a dependency of this repository. That is why this test can
// run in CI while its predecessor, which drove SqlAdapter.prototype.processMessage directly, could
// not. See https://github.com/ioBroker/ioBroker.sql/issues/527
const { guardUninitialized, COMMANDS_REQUIRING_DB } = require('../build/lib/messages');

// Every command the adapter answers out of the database.
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
    'getDpStatistics',
    'cleanupOrphaned',
];

// These answer from memory or only write objects, so they work before the dialect is known.
const DB_FREE_COMMANDS = ['features', 'enableHistory', 'disableHistory', 'getEnabledDPs', 'stopInstance'];

describe('Test guardUninitialized', function () {
    it('covers exactly the database-backed commands', function () {
        assert.deepStrictEqual([...COMMANDS_REQUIRING_DB].sort(), [...DB_COMMANDS].sort());
    });

    for (const command of DB_COMMANDS) {
        it(`rejects "${command}" while the dialect is unknown`, function () {
            const response = guardUninitialized(command, false);

            assert.ok(response, 'a response is returned instead of letting the handler run');
            assert.strictEqual(response.error, 'Adapter is not initialized yet');
            // the shape has to stay compatible with the getHistory/getCounter consumers
            assert.deepStrictEqual(response.result, []);
            assert.strictEqual(response.step, null);
        });
    }

    for (const command of DB_COMMANDS) {
        it(`lets "${command}" through once the dialect is known`, function () {
            assert.strictEqual(guardUninitialized(command, true), null);
        });
    }

    for (const command of DB_FREE_COMMANDS) {
        it(`never blocks "${command}", which needs no database`, function () {
            assert.strictEqual(guardUninitialized(command, false), null);
            assert.strictEqual(guardUninitialized(command, true), null);
        });
    }

    it('ignores an unknown command rather than blocking it', function () {
        assert.strictEqual(guardUninitialized('somethingElse', false), null);
    });
});

// The pure function above cannot catch the guard being dropped from processMessage(). Asserting
// against the compiled source keeps that wiring covered without importing it.
describe('Test processMessage wiring', function () {
    it('calls the guard before dispatching any command', function () {
        const main = fs.readFileSync(path.join(__dirname, '..', 'build', 'main.js'), 'utf8');

        // tsc emits the call as `(0, messages_1.guardUninitialized)(...)`. Matching a call pattern
        // rather than the bare name keeps this from being satisfied by the import alone.
        const call = /guardUninitialized\)?\(/.exec(main);
        assert.ok(call, 'processMessage must call guardUninitialized()');
        const guard = call.index;

        const dispatch = main.indexOf("msg.command === 'features'");
        assert.notStrictEqual(dispatch, -1, 'precondition: the command dispatch is recognizable');
        assert.ok(guard < dispatch, 'the guard has to run before the first command is dispatched');
    });
});
