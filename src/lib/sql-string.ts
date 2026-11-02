/**
 * Quoting for values that are written into an SQL statement as text.
 *
 * The dialect modules build their statements as strings. Until now a `'` in a value was simply
 * removed (`it's` was stored as `its`), and everything else went into the statement as it was: an ID
 * or a value with a `'` (plus anything after it) became part of the SQL, which is a syntax error at
 * best and an injected statement at worst. Datapoint IDs and `from` names come from the object
 * tree, string values from the states and from `storeState`/`update` messages.
 *
 * Standard SQL (PostgreSQL with `standard_conforming_strings`, SQLite, MSSQL) knows only one escape in
 * a string literal: `'` is doubled. A NUL byte can never be part of a literal and is dropped.
 * MySQL escapes with a backslash; its dialect module binds the values instead of quoting them.
 */

/**
 * Return `value` as an SQL string literal, including the surrounding quotes.
 *
 * @param value the text to quote; anything else is converted with `String()`
 */
export function quoteSqlString(value: unknown): string {
    const text = typeof value === 'string' ? value : String(value);
    return `'${text.replace(/\0/g, '').replace(/'/g, "''")}'`;
}

/**
 * Return `value` as an SQL number literal, or `NULL` when it is not a finite number.
 *
 * `NaN` and `Infinity` are no SQL literals: `Infinity` is read as a column name, which fails the
 * statement and loses every row written with it.
 *
 * @param value the number to write
 */
export function numberOrNull(value: unknown): string {
    return typeof value === 'number' && Number.isFinite(value) ? String(value) : 'NULL';
}
