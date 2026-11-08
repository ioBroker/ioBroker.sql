"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.quoteSqlString = quoteSqlString;
exports.quotePgString = quotePgString;
exports.numberOrNull = numberOrNull;
exports.boolOrNull = boolOrNull;
const values_1 = require("./values");
/**
 * Quoting for values that are written into an SQL statement as text.
 *
 * The dialect modules build their statements as strings. Until now a `'` in a value was simply
 * removed (`it's` was stored as `its`), and everything else went into the statement as it was: an ID
 * or a value with a `'` (plus anything after it) became part of the SQL, which is a syntax error at
 * best and an injected statement at worst. Datapoint IDs and `from` names come from the object
 * tree, string values from the states and from `storeState`/`update` messages.
 *
 * Standard SQL (SQLite, MSSQL) knows only one escape in a string literal: `'` is doubled. A NUL byte
 * can never be part of a literal and is dropped. PostgreSQL reads backslashes in a plain literal as
 * escapes when `standard_conforming_strings` is off, so it gets its own quoting (`quotePgString`).
 * MySQL escapes with a backslash; its dialect module binds the values instead of quoting them.
 */
/**
 * Return `value` as an SQL string literal, including the surrounding quotes.
 *
 * @param value the text to quote; anything else is converted with `String()`
 */
function quoteSqlString(value) {
    const text = typeof value === 'string' ? value : String(value);
    return `'${text.replace(/\0/g, '').replace(/'/g, "''")}'`;
}
/**
 * Return `value` as a PostgreSQL escape string literal (`E'...'`), including the prefix and quotes.
 *
 * A plain `'...'` literal depends on `standard_conforming_strings`: with it off - possible per
 * database, per role or in postgresql.conf, although the default is on since 9.1 - a backslash
 * escapes the next character, so `\'; DROP TABLE datapoints; --` turned the doubled quote into an
 * escaped one plus a closing quote, and the rest was executed. An escape string literal reads
 * backslashes as escapes with either setting, so doubling both `\` and `'` is always right.
 *
 * @param value the text to quote; anything else is converted with `String()`
 */
function quotePgString(value) {
    const text = typeof value === 'string' ? value : String(value);
    return `E'${text.replace(/\0/g, '').replace(/\\/g, '\\\\').replace(/'/g, "''")}'`;
}
/**
 * Return `value` as an SQL number literal, or `NULL` when it is not a finite number.
 *
 * `NaN` and `Infinity` are no SQL literals: `Infinity` is read as a column name, which fails the
 * statement and loses every row written with it.
 *
 * @param value the number to write
 */
function numberOrNull(value) {
    return typeof value === 'number' && Number.isFinite(value) ? String(value) : 'NULL';
}
/**
 * Return `value` as an SQL boolean literal for `ts_bool`, or `NULL` when it is none (see `toBool`).
 *
 * @param value the value to write
 * @param numeric `true` for a column that takes 1/0 (SQLite, MS SQL `BIT`), `false` for a real
 *        `BOOLEAN` (PostgreSQL), which refuses a number
 */
function boolOrNull(value, numeric) {
    const b = typeof value === 'boolean' ? (value ? 1 : 0) : (0, values_1.toBool)(value);
    if (b === null) {
        return 'NULL';
    }
    if (numeric) {
        return String(b);
    }
    return b ? 'TRUE' : 'FALSE';
}
//# sourceMappingURL=sql-string.js.map