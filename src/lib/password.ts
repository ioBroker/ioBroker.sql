/**
 * The database password is stored encrypted (`encryptedNative` in io-package.json, #587).
 *
 * Kept out of `main.ts` so it can be unit tested (see `values.ts` for why main.ts cannot be loaded
 * in a unit test).
 */

/** Prefix of every value js-controller encrypts with `encrypt()` (aes-192-cbc, js-controller >= 3.3) */
export const ENCRYPTED_PREFIX = '$/aes-192-cbc:';

/**
 * Whether the raw `native.password` of the instance object is still stored as plain text.
 *
 * Up to 4.4.1 the adapter had no `encryptedNative`, so admin stored the password as it was typed.
 * js-controller decrypts every `encryptedNative` attribute on start, and a value without the prefix
 * goes through its legacy XOR decryption - a plain password becomes garbage in `this.config` and
 * the database refuses the login. Such a value has to be encrypted once (`migratePassword()` in
 * main.ts). Admin >= 7 and `encrypt()` always write the prefix, so a value with it is encrypted
 * already; the legacy XOR format was never written for this adapter.
 *
 * @param raw `native.password` as read with `getForeignObject` (which does not decrypt)
 */
export function isPlainPassword(raw: unknown): raw is string {
    return typeof raw === 'string' && raw !== '' && !raw.startsWith(ENCRYPTED_PREFIX);
}
