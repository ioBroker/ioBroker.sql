import type { BoundStatement, RawEntriesOptions, SqlParam, TableName } from '../types';

// Values go into the statements as `?` placeholders, which mysql2 escapes (#587, C1). Before, strings
// were inlined with only the single quotes removed: "it's" was stored as "its", a value ending with a
// backslash broke the whole batch, and IDs and sources were not escaped at all.
// The database and table names stay in the SQL text as identifiers in backticks (mysql2 does not
// take a `?` inside backticks for a placeholder).

/**
 * A number that goes into the SQL text, e.g. for LIMIT, where MySQL does not accept a quoted value.
 *
 * @param value a number, or the text of one from a message
 * @param fallback used if the value is not a number
 */
function toInt(value: unknown, fallback = 0): number {
    const n = typeof value === 'number' ? value : parseInt(value as string, 10);
    return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

/**
 * A timestamp as it is bound: a number, or NULL when the value is none.
 *
 * Timestamps of `delete`, `update` and `storeState` messages come from the caller. Bound as text,
 * `'0 OR 1=1'` is no SQL any more, but MySQL in strict mode refuses to compare it with the BIGINT
 * column ("Truncated incorrect DOUBLE value") while MariaDB only warns. NULL matches no row on both.
 *
 * @param value the timestamp as given
 */
function toTs(value: unknown): number | null {
    const n = typeof value === 'number' ? value : typeof value === 'string' && value.trim() ? Number(value) : NaN;
    return Number.isFinite(n) ? n : null;
}

export function init(dbName: string, doNotCreateDatabase?: boolean): string[] {
    const commands = [
        `CREATE TABLE \`${dbName}\`.sources    (id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT, name TEXT);`,
        `CREATE TABLE \`${dbName}\`.datapoints (id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT, name TEXT, type INTEGER);`,
        `CREATE TABLE \`${dbName}\`.ts_number  (id INTEGER, ts BIGINT, val REAL,    ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));`,
        `CREATE TABLE \`${dbName}\`.ts_string  (id INTEGER, ts BIGINT, val TEXT,    ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));`,
        `CREATE TABLE \`${dbName}\`.ts_bool    (id INTEGER, ts BIGINT, val BOOLEAN, ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));`,
        `CREATE TABLE \`${dbName}\`.ts_counter (id INTEGER, ts BIGINT, val REAL);`,
    ];

    !doNotCreateDatabase &&
        commands.unshift(`CREATE DATABASE \`${dbName}\` DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;`);

    return commands;
}

/**
 * Statements for "Reset DB".
 *
 * @param dbName name of the database
 * @param doNotCreateDatabase the database was created outside the adapter (e.g. by a hoster): only the adapter's
 *   tables are dropped. The adapter could not create the database again and other tables in it would be lost.
 */
export function destroy(dbName: string, doNotCreateDatabase?: boolean): string[] {
    const commands = [
        `DROP TABLE \`${dbName}\`.ts_counter;`,
        `DROP TABLE \`${dbName}\`.ts_number;`,
        `DROP TABLE \`${dbName}\`.ts_string;`,
        `DROP TABLE \`${dbName}\`.ts_bool;`,
        `DROP TABLE \`${dbName}\`.sources;`,
        `DROP TABLE \`${dbName}\`.datapoints;`,
    ];
    if (!doNotCreateDatabase) {
        commands.push(`DROP DATABASE \`${dbName}\`;`);
    }
    return commands;
}

export function getFirstTs(dbName: string, table: TableName): string {
    return `SELECT id, MIN(ts) AS ts FROM \`${dbName}\`.${table} GROUP BY id;`;
}

/**
 * Count the rows and the covered time range per datapoint index.
 *
 * One query per table instead of one per datapoint: a database that has collected data for years
 * holds thousands of datapoints, and `GROUP BY id` lets the engine do the work in a single pass.
 *
 * @param dbName name of the database
 * @param table the time series table to summarize
 */
export function getIdCounts(dbName: string, table: TableName): string {
    return `SELECT id, COUNT(*) AS cnt, MIN(ts) AS first_ts, MAX(ts) AS last_ts FROM \`${dbName}\`.${table} GROUP BY id;`;
}

/**
 * Average bytes per row and total bytes of one time series table.
 *
 * There is no portable way to ask for the size of the rows belonging to a single datapoint, so the
 * statistics multiply this average by the row count. The result is an estimate and has to be
 * presented as one.
 *
 * @param dbName name of the database
 * @param table the time series table to measure
 */
export function getTableSize(dbName: string, table: TableName): BoundStatement {
    // information_schema knows the real storage footprint, so the estimate does not have to
    // guess at column widths. AVG_ROW_LENGTH is 0 for an empty table.
    return {
        sql: 'SELECT AVG_ROW_LENGTH AS avg_row_length, DATA_LENGTH + INDEX_LENGTH AS total_bytes FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?;',
        params: [dbName, table],
    };
}

/** Texts that are stored as `true` or `false` in `ts_bool`; compared trimmed and in lower case */
const TRUE_TEXTS = ['true', 'on', 'yes', '1'];
const FALSE_TEXTS = ['false', 'off', 'no', '0', ''];

/**
 * A value for `ts_bool`: 1, 0 or NULL.
 *
 * Before, every text except `false`, `0` and `''` was stored as 1, `"off"` too, and a number was
 * stored as it was (5 in a boolean column). Now the usual texts for on and off are recognized, a
 * number is 1 unless it is 0, and anything else is NULL, like a text in a number column.
 *
 * @param val the value of the state
 */
function toBool(val: unknown): 0 | 1 | null {
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

/**
 * The value of a state as it is stored in its table.
 *
 * @param table the time series table
 * @param val the value of the state
 */
function toDbValue(table: TableName, val: unknown): SqlParam {
    if (val === null || val === undefined) {
        return null;
    }
    if (table === 'ts_string') {
        return typeof val === 'object' ? JSON.stringify(val) : (val as string | number | boolean).toString();
    }
    if (typeof val === 'boolean') {
        return val ? 1 : 0;
    }
    if (table === 'ts_bool') {
        return toBool(val);
    }
    // ts_number, ts_counter: like the other dialects, a text that is not a number
    // is stored as NULL ("12 kWh" too), and so are NaN and Infinity, which MySQL would reject together
    // with the whole batch
    const n = typeof val === 'string' && val.trim() === '' ? NaN : Number(val);
    return Number.isFinite(n) ? n : null;
}

export function insert(
    dbName: string,
    index: number,
    values: {
        table: TableName;
        state: { val: any; ts: number; ack?: boolean; q?: number };
        from?: number;
    }[],
): BoundStatement[] {
    // the caller's objects are not changed: `state` is the reference value of "record changes only" (#295)
    const rows: { [table: string]: SqlParam[][] } = {};
    for (const value of values) {
        rows[value.table] ||= [];
        const val = toDbValue(value.table, value.state?.val);
        if (value.table === 'ts_counter') {
            rows[value.table].push([index, toTs(value.state.ts), val]);
        } else {
            rows[value.table].push([
                index,
                toTs(value.state.ts),
                val,
                value.state.ack ? 1 : 0,
                value.from || 0,
                toInt(value.state.q),
            ]);
        }
    }

    const statements: BoundStatement[] = [];
    for (const table in rows) {
        while (rows[table].length) {
            const chunk = rows[table].splice(0, 500);
            const placeholders = chunk.map(row => `(${row.map(() => '?').join(', ')})`).join(',');
            const params = chunk.flat();
            if (table === 'ts_counter') {
                // no ON DUPLICATE KEY here: ts_counter has no primary key in MySQL (unlike SQLite),
                // so there is no uniqueness conflict to suppress
                statements.push({
                    sql: `INSERT INTO \`${dbName}\`.ts_counter (id, ts, val) VALUES ${placeholders};`,
                    params,
                });
            } else {
                // ts_number/ts_string/ts_bool have PRIMARY KEY(id, ts). Importing history writes rows
                // that may already exist, and a single duplicate would otherwise abort the whole batch.
                // "id=id" is a no-op assignment, so this suppresses the duplicate key error and nothing
                // else - INSERT IGNORE would also hide truncation and conversion errors.
                statements.push({
                    sql: `INSERT INTO \`${dbName}\`.${table} (id, ts, val, ack, _from, q) VALUES ${placeholders} ON DUPLICATE KEY UPDATE id=id;`,
                    params,
                });
            }
        }
    }

    return statements;
}

export function retention(dbName: string, index: number, table: TableName, retention: number): BoundStatement {
    const d = new Date();
    d.setSeconds(-retention);
    return { sql: `DELETE FROM \`${dbName}\`.${table} WHERE id=? AND ts < ?;`, params: [index, d.getTime()] };
}

export function getIdSelect(dbName: string, name?: string): BoundStatement {
    if (!name) {
        return { sql: `SELECT id, type, name FROM \`${dbName}\`.datapoints;`, params: [] };
    }
    return { sql: `SELECT id, type, name FROM \`${dbName}\`.datapoints WHERE name=?;`, params: [name] };
}

export function getIdInsert(dbName: string, name: string, type: 0 | 1 | 2): BoundStatement {
    return { sql: `INSERT INTO \`${dbName}\`.datapoints (name, type) VALUES(?, ?);`, params: [name, type] };
}

export function getIdUpdate(dbName: string, id: number, type: 0 | 1 | 2): BoundStatement {
    return { sql: `UPDATE \`${dbName}\`.datapoints SET type=? WHERE id=?;`, params: [type, id] };
}

export function getFromSelect(dbName: string, name?: string): BoundStatement {
    if (name) {
        return { sql: `SELECT id, name FROM \`${dbName}\`.sources WHERE name=?;`, params: [name] };
    }
    return { sql: `SELECT id, name FROM \`${dbName}\`.sources;`, params: [] };
}

export function getFromInsert(dbName: string, values: string): BoundStatement {
    return { sql: `INSERT INTO \`${dbName}\`.sources (name) VALUES(?);`, params: [values] };
}

export function getCounterDiff(
    dbName: string,
    options: {
        index: number;
        start: number;
        end: number;
    },
): BoundStatement {
    const { index } = options;
    const start = toTs(options.start);
    const end = toTs(options.end);
    // Take first real value after start
    const subQueryStart = `SELECT ts, val FROM \`${dbName}\`.ts_number  WHERE id=? AND ts>=? AND ts<? AND val IS NOT NULL ORDER BY ts ASC LIMIT 1`;
    // Take last real value before the end
    const subQueryEnd = `SELECT ts, val FROM \`${dbName}\`.ts_number  WHERE id=? AND ts>=? AND ts<? AND val IS NOT NULL ORDER BY ts DESC LIMIT 1`;
    // Take last value before start
    const subQueryFirst = `SELECT ts, val FROM \`${dbName}\`.ts_number  WHERE id=? AND ts< ? ORDER BY ts DESC LIMIT 1`;
    // Take next value after end
    const subQueryLast = `SELECT ts, val FROM \`${dbName}\`.ts_number  WHERE id=? AND ts>= ? ORDER BY ts ASC LIMIT 1`;
    // get values from counters where counter changed from up to down (e.g. counter changed).
    // No ORDER BY here: MySQL 8 ignores it in a union member without LIMIT, and the outer
    // ORDER BY sorts the combined result anyway.
    const subQueryCounterChanges = `SELECT ts, val FROM \`${dbName}\`.ts_counter WHERE id=? AND ts>? AND ts<? AND val IS NOT NULL`;

    // The ORDER BY belongs in the OUTER query, not inside the derived table: sendResponseCounter
    // consumes the rows positionally, and a derived table's ordering is not guaranteed to survive
    // the SELECT DISTINCT above it.
    return {
        sql:
            `SELECT DISTINCT a.ts, a.val FROM ((${subQueryFirst})\n` +
            `UNION ALL (${subQueryStart})\n` +
            `UNION ALL (${subQueryEnd})\n` +
            `UNION ALL (${subQueryLast})\n` +
            `UNION ALL (${subQueryCounterChanges})) a ORDER BY a.ts;`,
        // in the order of the placeholders above
        params: [index, start, index, start, end, index, start, end, index, end, index, start, end],
    };
}

export function getHistory(
    dbName: string,
    table: string,
    options: ioBroker.GetHistoryOptions & { index: number | null },
): BoundStatement {
    // the values in the order of the placeholders
    const params: SqlParam[] = [];
    const columns = `ts, val${options.index === null ? `, ${table}.id as id` : ''}${options.ack ? ', ack' : ''}${
        options.from ? `, \`${dbName}\`.sources.name as 'from'` : ''
    }${options.q ? ', q' : ''} FROM \`${dbName}\`.${table}${
        // LEFT JOIN: a value stored without `from` has `_from` = 0, which has no row in `sources`.
        // With an INNER JOIN, such values were missing from every query that asked for `from`.
        options.from ? ` LEFT JOIN \`${dbName}\`.sources ON \`${dbName}\`.sources.id=\`${dbName}\`.${table}._from` : ''
    }`;
    let query = `SELECT ${columns}`;

    let where = '';

    if (options.index !== null) {
        where += ` \`${dbName}\`.${table}.id=?`;
        params.push(options.index);
    }
    if (options.end) {
        where += `${where ? ' AND' : ''} \`${dbName}\`.${table}.ts < ?`;
        params.push(toTs(options.end));
    }
    if (options.start) {
        where += `${where ? ' AND' : ''} \`${dbName}\`.${table}.ts >= ?`;
        params.push(toTs(options.start));

        // add the last value before start
        let subWhere = '';
        if (options.index !== null) {
            subWhere += ` \`${dbName}\`.${table}.id=?`;
            params.push(options.index);
        }
        subWhere += `${subWhere ? ' AND' : ''} \`${dbName}\`.${table}.ts < ?`;
        params.push(toTs(options.start));
        where += ` UNION ALL ( SELECT ${columns} WHERE ${subWhere} ORDER BY \`${dbName}\`.${table}.ts DESC LIMIT 1)`;

        // add next value after end
        subWhere = '';
        if (options.index !== null) {
            subWhere += ` \`${dbName}\`.${table}.id=?`;
            params.push(options.index);
        }
        subWhere += `${subWhere ? ' AND' : ''} \`${dbName}\`.${table}.ts >= ?`;
        params.push(toTs(options.end));
        where += ` UNION ALL ( SELECT ${columns} WHERE ${subWhere} ORDER BY \`${dbName}\`.${table}.ts ASC LIMIT 1)`;
    }

    if (where) {
        query += ` WHERE ${where}`;
    }

    query += ' ORDER BY ts';

    if (
        (!options.start && options.count) ||
        (options.aggregate === 'none' && options.count && options.returnNewestEntries)
    ) {
        query += ' DESC';
    } else {
        query += ' ASC';
    }

    if ((!options.start && options.count) || (options.aggregate === 'none' && options.count)) {
        // LIMIT does not take a quoted value, so the number goes into the text
        query += ` LIMIT ${toInt(options.count) + 2}`;
    }

    query += ';';
    return { sql: query, params };
}

export function deleteFromTable(
    dbName: string,
    table: TableName,
    index: number,
    start?: number,
    end?: number,
): BoundStatement {
    let sql = `DELETE FROM \`${dbName}\`.${table} WHERE id=?`;
    const params: SqlParam[] = [index];

    if (start && end) {
        sql += ' AND ts>=? AND ts<=?';
        params.push(toTs(start), toTs(end));
    } else if (start) {
        sql += ' AND ts=?';
        params.push(toTs(start));
    }

    return { sql: `${sql};`, params };
}

/**
 * Remove one row from the `datapoints` lookup table.
 *
 * Used by the cleanup: deleting only the values would leave the ID behind, so it would keep
 * showing up in the statistics with zero rows.
 *
 * @param dbName name of the database
 * @param index the integer key of the datapoint
 */
export function deleteDatapoint(dbName: string, index: number): BoundStatement {
    return { sql: `DELETE FROM \`${dbName}\`.datapoints WHERE id=?;`, params: [index] };
}

export function update(
    dbName: string,
    index: number,
    state: { val: number | string | boolean | null | undefined; ts: number; q?: number; ack?: boolean },
    from: number | undefined,
    table: 'ts_bool' | 'ts_number' | 'ts_string' | 'ts_counter',
): BoundStatement {
    // the caller's state object is not changed (see insert())
    const vals: string[] = [];
    const params: SqlParam[] = [];
    // always set, as before: an undefined value is written as NULL (and the SET is never empty)
    vals.push('val=?');
    params.push(toDbValue(table, state.val));
    if (state.q !== undefined) {
        vals.push('q=?');
        params.push(toInt(state.q));
    }
    if (from !== undefined) {
        vals.push('_from=?');
        params.push(from);
    }
    if (state.ack !== undefined) {
        vals.push('ack=?');
        params.push(state.ack ? 1 : 0);
    }
    params.push(index, toTs(state.ts));

    return { sql: `UPDATE \`${dbName}\`.${table} SET ${vals.join(', ')} WHERE id=? AND ts=?;`, params };
}

function rawEntriesWhere(
    dbName: string,
    table: TableName,
    index: number,
    options: { start?: number; end?: number },
): BoundStatement {
    let sql = `\`${dbName}\`.${table}.id=?`;
    const params: SqlParam[] = [index];
    if (options.start) {
        sql += ` AND \`${dbName}\`.${table}.ts>=?`;
        params.push(toTs(options.start));
    }
    if (options.end) {
        sql += ` AND \`${dbName}\`.${table}.ts<=?`;
        params.push(toTs(options.end));
    }
    return { sql, params };
}

export function getRawEntries(
    dbName: string,
    table: TableName,
    index: number,
    options: RawEntriesOptions,
): BoundStatement {
    const where = rawEntriesWhere(dbName, table, index, options);
    return {
        sql:
            `SELECT \`${dbName}\`.${table}.ts, \`${dbName}\`.${table}.val, \`${dbName}\`.${table}.ack, \`${dbName}\`.${table}.q, \`${dbName}\`.sources.name AS 'from' FROM \`${dbName}\`.${table}` +
            ` LEFT JOIN \`${dbName}\`.sources ON \`${dbName}\`.sources.id=\`${dbName}\`.${table}._from` +
            ` WHERE ${where.sql}` +
            ` ORDER BY \`${dbName}\`.${table}.ts ${options.sort === 'asc' ? 'ASC' : 'DESC'}` +
            // LIMIT/OFFSET do not take quoted values
            ` LIMIT ${toInt(options.limit)} OFFSET ${toInt(options.offset)};`,
        params: where.params,
    };
}

export function getRawEntriesCount(
    dbName: string,
    table: TableName,
    index: number,
    options: { start?: number; end?: number },
): BoundStatement {
    const where = rawEntriesWhere(dbName, table, index, options);
    return { sql: `SELECT COUNT(*) AS total FROM \`${dbName}\`.${table} WHERE ${where.sql};`, params: where.params };
}
