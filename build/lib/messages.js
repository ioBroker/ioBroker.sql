"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.COMMANDS_REQUIRING_DB = void 0;
exports.guardUninitialized = guardUninitialized;
exports.toTimestamp = toTimestamp;
exports.toInteger = toInteger;
exports.isId = isId;
exports.toQuality = toQuality;
exports.parseDeleteRequests = parseDeleteRequests;
exports.parseUpdateRequests = parseUpdateRequests;
exports.valueForTable = valueForTable;
/**
 * Messages that cannot be answered before the SQL dialect is known, i.e. before `sqlFuncs` has been
 * picked from the configuration in `main()`.
 *
 * The `message` handler is installed in the constructor, so the message box starts delivering as
 * soon as the adapter is ready - while `main()` is still awaiting `system.config`. Charts that kept
 * polling during a restart have their `getHistory` requests queued and delivered in one batch right
 * at that moment, which is how a single restart used to produce an immediate UNCAUGHT_EXCEPTION.
 * `stateChange` and `objectChange` cannot hit this window: both subscribe only after the dialect is
 * set. See https://github.com/ioBroker/ioBroker.sql/issues/527
 *
 * `features`, `enableHistory`, `disableHistory`, `getEnabledDPs` and `stopInstance` are deliberately
 * absent: they answer from memory or only write objects, so they work before the dialect is known.
 */
exports.COMMANDS_REQUIRING_DB = new Set([
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
]);
/**
 * Decide whether a message has to be rejected because the dialect is not known yet.
 *
 * This lives outside `main.ts` so that it can be unit tested: importing `main.ts` pulls in
 * `@iobroker/adapter-core`, which calls `process.exit(10)` at module load time when js-controller
 * cannot be resolved - and js-controller is not a dependency of this repository.
 *
 * @param command the message command, as delivered in `msg.command`
 * @param dialectKnown whether `sqlFuncs` has been assigned
 * @returns the response to answer with, or `null` when the message may be processed
 */
function guardUninitialized(command, dialectKnown) {
    if (!dialectKnown && exports.COMMANDS_REQUIRING_DB.has(command)) {
        return { result: [], step: null, error: 'Adapter is not initialized yet' };
    }
    return null;
}
/**
 * Validation of the values that `sendTo` messages carry into SQL statements.
 *
 * `getHistory`, `getCounter`, `update`, `delete`, `deleteRange`, `deleteAll` and `storeState` put
 * the ID, timestamps, `q` and the value from the message into the statement. A timestamp that is not
 * a number (a date string, `"31.02.2024"`, an object) became `NaN` or text in the SQL: a syntax error
 * for the client at best, and for `deleteRange` the "delete everything of this datapoint" branch, as
 * `NaN` is falsy. A missing ID selected the first datapoint of the table. The functions below are the
 * one place that turns message input into numbers and strings; everything else refuses the message.
 */
/** Timestamp in ms, from a number, a numeric string or a date string; `undefined` when it is none of them */
function toTimestamp(value) {
    if (typeof value === 'number') {
        return Number.isFinite(value) ? value : undefined;
    }
    if (typeof value === 'string' && value.trim()) {
        // "1700000000000" is a number: `new Date()` would read it as a date and return NaN
        const ts = /^-?\d+(\.\d+)?$/.test(value.trim()) ? Number(value) : new Date(value).getTime();
        return Number.isFinite(ts) ? ts : undefined;
    }
    if (value instanceof Date) {
        const ts = value.getTime();
        return Number.isFinite(ts) ? ts : undefined;
    }
    return undefined;
}
/** A finite integer from a number or a numeric string; `undefined` otherwise */
function toInteger(value) {
    const num = typeof value === 'string' ? Number(value.trim() || NaN) : value;
    return typeof num === 'number' && Number.isFinite(num) ? Math.trunc(num) : undefined;
}
/** A non-empty datapoint ID */
function isId(value) {
    return typeof value === 'string' && value.length > 0;
}
/** Quality `q` of a state: a finite integer, defaults to 0 */
function toQuality(value) {
    if (value === undefined || value === null) {
        return 0;
    }
    const q = toInteger(value);
    return q !== undefined && q >= 0 ? q : undefined;
}
/**
 * Turn the message of `delete`, `deleteRange` or `deleteAll` into a list of requests.
 *
 * The accepted shapes are the ones the adapter answered so far: `{id, ts}`, `{id, start, end?}`,
 * `{id, state: {ts}}`, `{id, state: {start, end?}}`, each also as an array, plus `{id, state: [...]}`
 * and `{id, ts: [...]}` for several entries of one datapoint; `deleteAll` takes `{id}` or `[{id}]`.
 * Every entry is checked: an ID is required, `ts`/`start`/`end` must be timestamps (see
 * `toTimestamp`), `end` defaults to `now`. An invalid entry is reported and not executed, so an
 * unparsable `start` can never widen a range delete into deleting everything.
 *
 * @param message `msg.message` as delivered
 * @param all `true` for `deleteAll`: entries are IDs only, every stored value of the datapoint
 * @param now the timestamp that `end` defaults to
 * @param resolveId maps an alias to the real ID
 */
function parseDeleteRequests(message, all, now, resolveId = id => id) {
    const requests = [];
    const errors = [];
    const one = (id, entry) => {
        if (!isId(id)) {
            errors.push(`Missing ID in ${JSON.stringify(entry)}`);
            return;
        }
        const realId = resolveId(id);
        if (all) {
            requests.push({ id: realId });
            return;
        }
        // a plain number is a timestamp (`{id, ts: [1, 2]}`), an object carries ts or start/end
        const spec = entry && typeof entry === 'object' ? entry : { ts: entry };
        if (spec.ts !== undefined && spec.ts !== null) {
            const ts = toTimestamp(spec.ts);
            if (ts === undefined) {
                errors.push(`Invalid timestamp ${JSON.stringify(spec.ts)} for ${id}`);
            }
            else {
                requests.push({ id: realId, ts });
            }
            return;
        }
        if (spec.start !== undefined && spec.start !== null) {
            const start = toTimestamp(spec.start);
            const end = spec.end === undefined || spec.end === null ? now : toTimestamp(spec.end);
            if (start === undefined || end === undefined) {
                errors.push(`Invalid range ${JSON.stringify(spec.start)} - ${JSON.stringify(spec.end)} for ${id}`);
            }
            else {
                requests.push({ id: realId, start, end });
            }
            return;
        }
        errors.push(`Neither ts nor start given for ${id}: ${JSON.stringify(entry)}`);
    };
    if (Array.isArray(message)) {
        for (const item of message) {
            if (!item || typeof item !== 'object') {
                errors.push(`Invalid entry ${JSON.stringify(item)}`);
            }
            else if (all) {
                one(item.id, item);
            }
            else {
                // `{id, ts}` / `{id, start, end}` / `{id, state: {...}}`
                one(item.id, item.state && typeof item.state === 'object' ? item.state : item);
            }
        }
    }
    else if (message && typeof message === 'object') {
        const msg = message;
        if (all) {
            one(msg.id, msg);
        }
        else if (Array.isArray(msg.state)) {
            msg.state.forEach(entry => one(msg.id, entry));
        }
        else if (Array.isArray(msg.ts)) {
            msg.ts.forEach(ts => one(msg.id, ts));
        }
        else if (msg.state && typeof msg.state === 'object') {
            one(msg.id, msg.state);
        }
        else {
            one(msg.id, msg);
        }
    }
    else {
        errors.push(`Invalid call: ${JSON.stringify(message)}`);
    }
    return { requests, errors };
}
/**
 * Turn the message of `update` into a list of `{id, state}` requests.
 *
 * Shapes: `{id, state}`, `[{id, state}, ...]` and `{id, state: [...]}`. `state.ts` must be a timestamp
 * (it selects the row); `q`, if given, a non-negative integer; `ack` is read as a boolean; `from` as a
 * string; `val` is checked against the stored type when the statement is built.
 *
 * @param message `msg.message` as delivered
 * @param resolveId maps an alias to the real ID
 */
function parseUpdateRequests(message, resolveId = id => id) {
    const requests = [];
    const errors = [];
    const one = (id, state) => {
        if (!isId(id)) {
            errors.push(`Missing ID in ${JSON.stringify(state)}`);
            return;
        }
        if (!state || typeof state !== 'object') {
            errors.push(`Invalid state for ${id}: ${JSON.stringify(state)}`);
            return;
        }
        const s = state;
        const ts = toTimestamp(s.ts);
        if (ts === undefined) {
            errors.push(`Invalid timestamp ${JSON.stringify(s.ts)} for ${id}`);
            return;
        }
        const result = { ts };
        if (s.val !== undefined) {
            result.val = s.val;
        }
        if (s.q !== undefined && s.q !== null) {
            const q = toQuality(s.q);
            if (q === undefined) {
                errors.push(`Invalid quality ${JSON.stringify(s.q)} for ${id}`);
                return;
            }
            result.q = q;
        }
        if (s.ack !== undefined && s.ack !== null) {
            result.ack = s.ack === true || s.ack === 1 || s.ack === 'true';
        }
        if (typeof s.from === 'string' && s.from) {
            result.from = s.from;
        }
        requests.push({ id: resolveId(id), state: result });
    };
    if (Array.isArray(message)) {
        for (const item of message) {
            if (!item || typeof item !== 'object') {
                errors.push(`Invalid entry ${JSON.stringify(item)}`);
            }
            else {
                one(item.id, item.state);
            }
        }
    }
    else if (message && typeof message === 'object') {
        const msg = message;
        if (Array.isArray(msg.state)) {
            msg.state.forEach(state => one(msg.id, state));
        }
        else {
            one(msg.id, msg.state);
        }
    }
    else {
        errors.push(`Invalid call: ${JSON.stringify(message)}`);
    }
    return { requests, errors };
}
/**
 * The value of an `update` or `storeState` entry, as it is written into the table of the datapoint.
 *
 * The dialects put numbers and booleans into the statement as they are, so a string in `val` of a
 * numeric datapoint would be SQL. Returns the value to store, or an `Error` when it does not fit the
 * stored type. `null` and `undefined` are stored as NULL.
 *
 * @param table the table the datapoint is stored in
 * @param val the value from the message
 */
function valueForTable(table, val) {
    if (val === null || val === undefined) {
        return null;
    }
    if (table === 'ts_number' || table === 'ts_counter') {
        const num = typeof val === 'string' ? Number(val.trim() || NaN) : typeof val === 'boolean' ? +val : val;
        return typeof num === 'number' && Number.isFinite(num)
            ? num
            : new Error(`Value ${JSON.stringify(val)} is not a number`);
    }
    if (table === 'ts_bool') {
        if (typeof val === 'boolean') {
            return val;
        }
        if (val === 1 || val === 0 || val === 'true' || val === 'false') {
            return val === 1 || val === 'true';
        }
        return new Error(`Value ${JSON.stringify(val)} is not a boolean`);
    }
    // ts_string: objects are stored as JSON, like the logged values
    if (typeof val === 'string') {
        return val;
    }
    if (typeof val === 'number' || typeof val === 'boolean' || typeof val === 'bigint') {
        return val.toString();
    }
    return JSON.stringify(val);
}
//# sourceMappingURL=messages.js.map