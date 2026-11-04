"use strict";
/**
 * Value comparison for the `changesOnly` filter.
 *
 * Kept out of `main.ts` so it can be unit tested: importing `main.ts` pulls in
 * `@iobroker/adapter-core`, which calls `process.exit(10)` at module load time when js-controller
 * cannot be resolved, and js-controller is not a dependency of this repository.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.isSameValue = isSameValue;
exports.parseLimit = parseLimit;
exports.toBool = toBool;
/**
 * Whether a new value is the same as the one that was stored last.
 *
 * `pushHistory()` used to decide this from `state.ts !== state.lc`, i.e. from the controller's own
 * "last change" timestamp. That is right for an ordinary state - js-controller only moves `lc` when
 * the value really changes - but wrong for an alias: an alias has no value of its own, so it carries
 * the `lc` of its source. A source that changes from 12.34 to 12.31 moves `lc`, and the alias
 * arrives with `ts === lc` although its read converter maps both to 12.3. `changesOnly` therefore
 * stored a row for every source change. See https://github.com/ioBroker/ioBroker.sql/issues/295
 *
 * Objects are compared by their JSON because that is how they are stored: `ts_string` holds
 * `JSON.stringify`ed values, so two structurally equal objects are the same row. A strict `!==`
 * would compare references, call every update a change and defeat `changesOnly` for those
 * datapoints - the opposite of the bug being fixed here.
 *
 * @param lastStored the value of the last state that was written, `sqlDPs[id].state.val`
 * @param next the value of the state that just arrived
 */
function isSameValue(lastStored, next) {
    if (lastStored === next) {
        return true;
    }
    // NaN === NaN is false, but two NaN readings are not a change worth storing
    if (typeof lastStored === 'number' && typeof next === 'number') {
        return Number.isNaN(lastStored) && Number.isNaN(next);
    }
    if (lastStored && next && typeof lastStored === 'object' && typeof next === 'object') {
        try {
            return JSON.stringify(lastStored) === JSON.stringify(next);
        }
        catch {
            // circular structures cannot be stored either, so treat them as different and let the
            // write path report the real problem
            return false;
        }
    }
    // Different types are a change. `'12.3'` and `12.3` are stored differently - string values go
    // into ts_string, numbers into ts_number - so collapsing them here would lose a real transition.
    return false;
}
/**
 * Parses the "ignore values below/above" limit of a datapoint.
 *
 * `parseFloat(x) || null` turned a limit of `0` into "no limit", so "ignore values below 0" - which
 * is also what the old `ignoreBelowZero` option is migrated to - never filtered anything.
 *
 * @param value the configured limit, a number or the text from the admin input
 * @returns the limit, or null if the value is not a number
 */
function parseLimit(value) {
    // a decimal comma from `enableHistory` via sendTo, which the admin validation does not check
    const limit = typeof value === 'number' ? value : parseFloat(String(value).replace(',', '.'));
    return Number.isFinite(limit) ? limit : null;
}
/** Texts that are stored as `true` or `false` in `ts_bool`; compared trimmed and in lower case */
const TRUE_TEXTS = ['true', 'on', 'yes', '1'];
const FALSE_TEXTS = ['false', 'off', 'no', '0', ''];
/**
 * A value for `ts_bool`: 1, 0 or NULL - the same for all dialects.
 *
 * The usual texts for on and off are recognized, a number is 1 unless it is 0, and anything else
 * is NULL, like a text in a number column. Before, MySQL stored every text except `false`, `0` and
 * `''` as true, PostgreSQL failed the batch on a text or a number, SQLite and MS SQL stored any
 * non-empty text (`off` too) as true.
 *
 * @param val the value of the state
 */
function toBool(val) {
    if (typeof val === 'number') {
        return Number.isFinite(val) ? (val ? 1 : 0) : null;
    }
    if (typeof val === 'string') {
        const text = val.trim().toLowerCase();
        if (TRUE_TEXTS.includes(text)) {
            return 1;
        }
        return FALSE_TEXTS.includes(text) ? 0 : null;
    }
    return null;
}
//# sourceMappingURL=values.js.map