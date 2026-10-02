const assert = require('node:assert');
// Imported from its own module, NOT from build/main: requiring main.js pulls in
// @iobroker/adapter-core, which calls process.exit(10) at load time when js-controller cannot be
// resolved - and js-controller is not a dependency of this repository.
const { buildMySQLOptions } = require('../build/lib/connection-options');

function config(overrides = {}) {
    return {
        dbtype: 'mysql',
        host: 'localhost',
        socketPath: '',
        port: 3306,
        user: 'iobroker',
        password: 'secret',
        encrypt: false,
        rejectUnauthorized: false,
        ...overrides,
    };
}

describe('Test buildMySQLOptions', function () {
    it('uses host and port when no socket is configured', function () {
        const options = buildMySQLOptions(config());

        assert.strictEqual(options.host, 'localhost');
        assert.strictEqual(options.port, 3306);
        assert.strictEqual(options.socketPath, undefined);
    });

    it('uses the socket and drops host and port when one is configured', function () {
        const options = buildMySQLOptions(config({ socketPath: '/var/run/mysqld/mysqld.sock' }));

        assert.strictEqual(options.socketPath, '/var/run/mysqld/mysqld.sock');
        // mysql2 ignores host/port for a socket connection, so passing them on would only make the
        // logged options and the error messages misleading
        assert.strictEqual(options.host, undefined);
        assert.strictEqual(options.port, undefined);
    });

    it('keeps credentials either way', function () {
        for (const socketPath of ['', '/tmp/mysql.sock']) {
            const options = buildMySQLOptions(config({ socketPath }));
            assert.strictEqual(options.user, 'iobroker', `socketPath=${JSON.stringify(socketPath)}`);
            assert.strictEqual(options.password, 'secret', `socketPath=${JSON.stringify(socketPath)}`);
        }
    });

    it('passes ssl through for a socket connection too', function () {
        const plain = buildMySQLOptions(config({ socketPath: '/tmp/mysql.sock' }));
        assert.strictEqual(plain.ssl, undefined, 'no ssl when encrypt is off');

        const encrypted = buildMySQLOptions(
            config({ socketPath: '/tmp/mysql.sock', encrypt: true, rejectUnauthorized: true }),
        );
        assert.deepStrictEqual(encrypted.ssl, { rejectUnauthorized: true });
    });

    it('omits an empty port rather than sending 0', function () {
        const options = buildMySQLOptions(config({ port: 0 }));
        assert.strictEqual(options.port, undefined);
    });

    it('falls back to empty credentials instead of undefined', function () {
        const options = buildMySQLOptions(config({ user: '', password: '' }));
        assert.strictEqual(options.user, '');
        assert.strictEqual(options.password, '');
    });
});
