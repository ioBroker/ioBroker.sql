const assert = require('node:assert');
const { formatError, counterTypeMismatch, isConnectionError } = require('../build/lib/errors');

describe('Test formatError', function () {
    it('unwraps an AggregateError', function () {
        // this is what Node's happy-eyeballs connect produces when a database is switched off:
        // an AggregateError with an empty message, so String(err) is just "AggregateError"
        const first = Object.assign(new Error('connect ECONNREFUSED ::1:3306'), { code: 'ECONNREFUSED' });
        const second = Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:3306'), { code: 'ECONNREFUSED' });
        const err = new AggregateError([first, second]);

        assert.strictEqual(err.toString(), 'AggregateError', 'precondition: toString() says nothing');

        const text = formatError(err);
        assert.ok(text.includes('connect ECONNREFUSED ::1:3306'), text);
        assert.ok(text.includes('connect ECONNREFUSED 127.0.0.1:3306'), text);
        // the class name says nothing about the cause and must not clutter the log line
        assert.ok(!text.includes('AggregateError'), text);
        assert.ok(!text.includes('\n'), 'must stay on one line');
    });

    it('reports identical sub-errors only once', function () {
        const err = new AggregateError([
            new Error('connect ECONNREFUSED 127.0.0.1:3306'),
            new Error('connect ECONNREFUSED 127.0.0.1:3306'),
        ]);
        const text = formatError(err);
        assert.strictEqual(text.indexOf('ECONNREFUSED'), text.lastIndexOf('ECONNREFUSED'), text);
    });

    it('limits the number of nested errors', function () {
        const err = new AggregateError(new Array(9).fill(0).map((_, i) => new Error(`address ${i} failed`)));
        const text = formatError(err);
        assert.ok(text.includes('address 4 failed'), text);
        assert.ok(!text.includes('address 5 failed'), text);
        assert.ok(text.includes('and 4 more'), text);
    });

    it('keeps a normal error readable and adds the code', function () {
        const err = Object.assign(new Error('Access denied for user'), { code: 'ER_ACCESS_DENIED_ERROR', errno: 1045 });
        assert.strictEqual(formatError(err), 'Access denied for user (code: ER_ACCESS_DENIED_ERROR) (errno: 1045)');
    });

    it('does not repeat a code that is already in the message', function () {
        const err = Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:5432'), { code: 'ECONNREFUSED' });
        assert.strictEqual(formatError(err), 'connect ECONNREFUSED 127.0.0.1:5432');
    });

    it('keeps a name that tells something', function () {
        // mssql names its errors, and "ConnectionError: Login failed" is more helpful than "Login failed"
        const err = Object.assign(new Error('Login failed for user'), { name: 'ConnectionError' });
        assert.strictEqual(formatError(err), 'ConnectionError: Login failed for user');
    });

    it('unwraps the cause', function () {
        const err = new Error('Cannot open connection', { cause: new Error('getaddrinfo ENOTFOUND mysql-host') });
        const text = formatError(err);
        assert.ok(text.includes('Cannot open connection'), text);
        assert.ok(text.includes('getaddrinfo ENOTFOUND mysql-host'), text);
    });

    it('survives strings, null and plain objects', function () {
        assert.strictEqual(formatError('No database connection'), 'No database connection');
        assert.strictEqual(formatError(null), 'Unknown error');
        assert.strictEqual(formatError(undefined), 'Unknown error');
        assert.strictEqual(formatError({ severity: 'FATAL' }), '{"severity":"FATAL"}');
        const circular = {};
        circular.self = circular;
        assert.ok(formatError(circular).length > 0);
    });
});

describe('Test counterTypeMismatch', function () {
    it('names the datapoint, which the old message did not', function () {
        const text = counterTypeMismatch('modbus.0.holdingRegisters.1234_Zaehler', 'String');

        // the whole point of issue #320: the old text was 'Counter must have type "number"!'
        // and left people searching their configuration for which datapoint it meant
        assert.ok(text.includes('modbus.0.holdingRegisters.1234_Zaehler'), text);
        assert.ok(text.includes('String'), 'says how it is stored now');
        assert.ok(text.includes('Number'), 'says what it has to be');
        assert.ok(text.includes('counter option'), 'offers the other way out');
        assert.ok(!text.includes('\n'), 'must stay on one line');
    });

    it('survives an unmapped storage type', function () {
        // storageTypes[] has no entry for every value the datapoints table could hold
        assert.ok(counterTypeMismatch('sql.0.x', 7).includes('"7"'));
        assert.ok(counterTypeMismatch('sql.0.x', undefined).includes('unknown'));
    });
});

describe('Test isConnectionError', function () {
    const err = (message, extra = {}) => Object.assign(new Error(message), extra);

    it('recognises a dead connection for every driver', function () {
        // mysql2
        assert.strictEqual(
            isConnectionError(err("Can't add new command when connection is in closed state", { fatal: true })),
            true,
        );
        assert.strictEqual(
            isConnectionError(
                err('Connection lost: The server closed the connection.', { code: 'PROTOCOL_CONNECTION_LOST' }),
            ),
            true,
        );
        assert.strictEqual(isConnectionError(err('read ECONNRESET', { code: 'ECONNRESET' })), true);
        // the server cannot be reached at all
        assert.strictEqual(
            isConnectionError(err('connect ECONNREFUSED 127.0.0.1:3306', { code: 'ECONNREFUSED' })),
            true,
        );
        assert.strictEqual(isConnectionError(err('getaddrinfo ENOTFOUND db.example', { code: 'ENOTFOUND' })), true);
        // pg
        assert.strictEqual(isConnectionError(err('Client was closed and is not queryable')), true);
        assert.strictEqual(isConnectionError(err('Connection terminated unexpectedly')), true);
        // mssql
        assert.strictEqual(isConnectionError(err('Connection is closed.', { code: 'ECONNCLOSED' })), true);
        // sqlite3
        assert.strictEqual(
            isConnectionError(err('SQLITE_MISUSE: Database is closed', { code: 'SQLITE_MISUSE' })),
            true,
        );
    });

    it('does not treat an SQL error as a dead connection', function () {
        assert.strictEqual(
            isConnectionError(err("Duplicate entry '1' for key 'PRIMARY'", { code: 'ER_DUP_ENTRY', fatal: false })),
            false,
        );
        assert.strictEqual(
            isConnectionError(err('You have an error in your SQL syntax', { code: 'ER_PARSE_ERROR' })),
            false,
        );
        assert.strictEqual(isConnectionError(err('relation "ts_number" does not exist', { code: '42P01' })), false);
        // an SQL error whose text quotes the statement does not count, even if it contains a pattern
        assert.strictEqual(
            isConnectionError(
                err("You have an error in your SQL syntax near 'closed state'", {
                    code: 'ER_PARSE_ERROR',
                    errno: 1064,
                }),
            ),
            false,
        );
        assert.strictEqual(
            isConnectionError(err('SQLITE_ERROR: no such column: connection terminated', { code: 'SQLITE_ERROR' })),
            false,
        );
        assert.strictEqual(isConnectionError(null), false);
        assert.strictEqual(isConnectionError('text'), false);
    });
});
