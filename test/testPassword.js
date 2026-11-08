const assert = require('node:assert');
const { isPlainPassword, ENCRYPTED_PREFIX } = require('../build/lib/password');

// #587: `password` is an `encryptedNative` attribute since 4.4.2. A password stored by <= 4.4.1 is
// plain text and is encrypted once on start (`migratePassword()` in main.ts).
describe('Test the detection of a plain-text database password', function () {
    it('a typed password without the prefix is plain text', function () {
        assert.strictEqual(isPlainPassword('secret'), true);
        assert.strictEqual(isPlainPassword(' '), true);
        // only the full prefix counts, not a password that happens to start like it
        assert.strictEqual(isPlainPassword('$/aes'), true);
        assert.strictEqual(isPlainPassword('aes-192-cbc:00:11'), true);
    });

    it('a value written by encrypt() is not migrated again', function () {
        assert.strictEqual(isPlainPassword(`${ENCRYPTED_PREFIX}00112233445566778899aabbccddeeff:abcdef`), false);
    });

    it('no password, or no string, needs no migration', function () {
        assert.strictEqual(isPlainPassword(''), false);
        assert.strictEqual(isPlainPassword(undefined), false);
        assert.strictEqual(isPlainPassword(null), false);
        assert.strictEqual(isPlainPassword(123), false);
    });

});
