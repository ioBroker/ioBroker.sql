const assert = require('node:assert');
const { formatError } = require('../build/lib/errors');

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
