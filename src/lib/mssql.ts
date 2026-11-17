import { boolOrNull, numberOrNull, quoteMsIdentifier, quoteNString, quoteSqlString } from './sql-string';
import type { RawEntriesOptions, TableName } from '../types';

/**
 * Per database: the `name` columns that are still `varchar` (tables created before #588, unless converted by hand).
 *
 * Such a column keeps only the code page of its collation: `alias.0.Кухня` is stored as `alias.0.?????`. A plain
 * literal is converted the same way and finds that row. With an `N'...'` literal SQL Server converts the column to
 * `nvarchar` instead, the row is not found, the insert stores `?????` again, the select after it finds nothing, and
 * every value of the datapoint is lost while `datapoints` grows. So these columns keep plain literals.
 */
const varcharNames = new Map<string, Set<'datapoints' | 'sources'>>();

/**
 * Statement that reads the type of the `name` columns of `datapoints` and `sources` (see `setNameColumnTypes()`).
 *
 * @param dbName name of the database
 */
export function getNameColumnTypes(dbName: string): string {
    const db = quoteMsIdentifier(dbName);
    return (
        `SELECT t.name AS tbl, TYPE_NAME(c.user_type_id) AS type FROM ${db}.sys.columns c ` +
        `JOIN ${db}.sys.tables t ON t.object_id = c.object_id ` +
        `WHERE c.name = 'name' AND t.name IN ('datapoints', 'sources');`
    );
}

/**
 * Remember which `name` columns of a database are still `varchar`, from the rows of `getNameColumnTypes()`.
 *
 * @param dbName name of the database
 * @param rows the rows returned by `getNameColumnTypes()`
 */
export function setNameColumnTypes(dbName: string, rows: { tbl: string; type: string }[]): void {
    const varchar = new Set<'datapoints' | 'sources'>();
    for (const row of rows) {
        if ((row.tbl === 'datapoints' || row.tbl === 'sources') && !String(row.type).toLowerCase().startsWith('n')) {
            varchar.add(row.tbl);
        }
    }
    varcharNames.set(dbName, varchar);
}

/**
 * A name as literal for the `name` column of `table`: `N'...'` for `nvarchar`, plain for a `varchar` column of an
 * older table (see `varcharNames`).
 *
 * @param dbName name of the database
 * @param table `datapoints` or `sources`
 * @param name the name
 */
function nameLiteral(dbName: string, table: 'datapoints' | 'sources', name: string): string {
    return varcharNames.get(dbName)?.has(table) ? quoteSqlString(name) : quoteNString(name);
}

/**
 * `nvarchar` keeps every character: `varchar` stores only the code page of the collation, so `alias.0.Кухня`
 * or an emoji came back as `?`. The collation compares case-sensitively like ioBroker IDs (#588); `_SC`
 * makes 4-byte characters (emojis) compare as one character. Trailing spaces are still ignored by `=` (SQL
 * Server pads every comparison), so `pickRowByName()` keeps `alias.0.Tor` and `alias.0.Tor ` apart.
 */
const NAME = 'nvarchar(255) COLLATE Latin1_General_100_CS_AS_SC';

/**
 * Statements that create the database (unless it exists already) and the tables of the adapter.
 *
 * Only new tables get `NAME` and `nvarchar(max)` for `ts_string`; existing tables are not changed. The README
 * explains how to convert them by hand.
 *
 * @param dbName name of the database
 * @param doNotCreateDatabase the database was created outside the adapter (e.g. by a hoster)
 */
export function init(dbName: string, doNotCreateDatabase?: boolean): string[] {
    const db = quoteMsIdentifier(dbName);
    const commands = [
        `CREATE TABLE ${db}.dbo.sources     (id INTEGER NOT NULL PRIMARY KEY IDENTITY(1,1), name ${NAME});`,
        `CREATE TABLE ${db}.dbo.datapoints  (id INTEGER NOT NULL PRIMARY KEY IDENTITY(1,1), name ${NAME}, type INTEGER);`,
        `CREATE TABLE ${db}.dbo.ts_number   (id INTEGER, ts BIGINT, val REAL, ack BIT, _from INTEGER, q INTEGER);`,
        `CREATE INDEX i_id on ${db}.dbo.ts_number (id, ts);`,
        `CREATE TABLE ${db}.dbo.ts_string   (id INTEGER, ts BIGINT, val nvarchar(max), ack BIT, _from INTEGER, q INTEGER);`,
        `CREATE INDEX i_id on ${db}.dbo.ts_string (id, ts);`,
        `CREATE TABLE ${db}.dbo.ts_bool     (id INTEGER, ts BIGINT, val BIT,  ack BIT, _from INTEGER, q INTEGER);`,
        `CREATE INDEX i_id on ${db}.dbo.ts_bool (id, ts);`,
        `CREATE TABLE ${db}.dbo.ts_counter  (id INTEGER, ts BIGINT, val REAL);`,
        `CREATE INDEX i_id on ${db}.dbo.ts_counter (id, ts);`,
    ];
    if (!doNotCreateDatabase) {
        commands.unshift(`CREATE DATABASE ${db};`);
    }

    return commands;
}

/**
 * Statements for "Reset DB".
 *
 * @param dbName name of the database
 * @param doNotCreateDatabase the database was created outside the adapter: only the adapter's tables are dropped,
 *   because the adapter could not create the database again and other tables in it would be lost.
 */
export function destroy(dbName: string, doNotCreateDatabase?: boolean): string[] {
    const db = quoteMsIdentifier(dbName);
    const commands = [
        `DROP TABLE ${db}.dbo.ts_counter;`,
        `DROP TABLE ${db}.dbo.ts_number;`,
        `DROP TABLE ${db}.dbo.ts_string;`,
        `DROP TABLE ${db}.dbo.ts_bool;`,
        `DROP TABLE ${db}.dbo.sources;`,
        `DROP TABLE ${db}.dbo.datapoints;`,
    ];
    if (!doNotCreateDatabase) {
        commands.push(`DROP DATABASE ${db};`);
        // needs ALTER SERVER STATE, which a user of an existing (hosted) database usually lacks
        commands.push('DBCC FREEPROCCACHE;');
    }
    return commands;
}

export function getFirstTs(dbName: string, table: TableName): string {
    const db = quoteMsIdentifier(dbName);
    return `SELECT id, MIN(ts) AS ts FROM ${db}.dbo.${table} GROUP BY id;`;
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
    const db = quoteMsIdentifier(dbName);
    return `SELECT id, COUNT(*) AS cnt, MIN(ts) AS first_ts, MAX(ts) AS last_ts FROM ${db}.dbo.${table} GROUP BY id;`;
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
export function getTableSize(dbName: string, table: TableName): string {
    const db = quoteMsIdentifier(dbName);
    // Pages are 8 KB; used_page_count covers data and indexes.
    return `SELECT CASE WHEN SUM(p.row_count) > 0 THEN SUM(p.used_page_count) * 8192 / SUM(p.row_count) ELSE 0 END AS avg_row_length, SUM(p.used_page_count) * 8192 AS total_bytes FROM ${db}.sys.dm_db_partition_stats p WHERE p.object_id = OBJECT_ID(${quoteNString(`${db}.dbo.${table}`)}) AND p.index_id IN (0, 1);`;
}

export function insert(
    dbName: string,
    index: number,
    values: {
        table: TableName;
        state: { val: any; ts: number; ack?: boolean; q?: number };
        from?: number;
    }[],
): string[] {
    const db = quoteMsIdentifier(dbName);
    const insertValues: { [table: string]: string[] } = {};
    values.forEach(original => {
        // work on a copy: the caller keeps `state` as the reference value for "record changes only" (#295),
        // so writing the SQL literal back into it would make the next equal value look like a change
        const value = { ...original, state: { ...original.state } };
        // state, from, db
        insertValues[value.table] ||= [];

        if (!value.state || value.state.val === null || value.state.val === undefined) {
            value.state.val = 'NULL';
        } else if (value.table === 'ts_string') {
            value.state.val = quoteNString(value.state.val);
        } else if (value.table === 'ts_bool') {
            value.state.val = boolOrNull(value.state.val, true);
        } else if (value.table === 'ts_number' || value.table === 'ts_counter') {
            value.state.val = numberOrNull(value.state.val);
        }

        if (value.table === 'ts_counter') {
            insertValues[value.table].push(`(${index}, ${value.state.ts}, ${value.state.val})`);
        } else {
            insertValues[value.table].push(
                `(${index}, ${value.state.ts}, ${value.state.val}, ${value.state.ack ? 1 : 0}, ${value.from || 0}, ${value.state.q || 0})`,
            );
        }
    });

    const query: string[] = [];
    // The other dialects suppress duplicate (id, ts) rows here so that importing history does not
    // abort a whole batch. MS SQL has no equivalent: init() creates plain CREATE INDEX, not a unique
    // index, so no uniqueness conflict can occur in the first place - duplicates are already inserted
    // silently. Making the index unique would only affect new installations and would turn today's
    // silent duplicates into hard errors, so the insert is deliberately left unguarded.
    for (const table in insertValues) {
        if (table === 'ts_counter') {
            while (insertValues[table].length) {
                query.push(
                    `INSERT INTO ${db}.dbo.ts_counter (id, ts, val) VALUES ${insertValues[table].splice(0, 500).join(',')};`,
                );
            }
        } else {
            while (insertValues[table].length) {
                query.push(
                    `INSERT INTO ${db}.dbo.${table} (id, ts, val, ack, _from, q) VALUES ${insertValues[table].splice(0, 500).join(',')};`,
                );
            }
        }
    }

    return query;
}

export function retention(dbName: string, index: number, table: TableName, retention: number): string {
    const db = quoteMsIdentifier(dbName);
    const d = new Date();
    d.setSeconds(-retention);
    let query = `DELETE FROM ${db}.dbo.${table} WHERE`;
    query += ` id=${index}`;
    query += ` AND ts < ${d.getTime()}`;
    query += ';';
    return query;
}

export function getIdSelect(dbName: string, name?: string): string {
    const db = quoteMsIdentifier(dbName);
    if (!name) {
        return `SELECT id, type, name FROM ${db}.dbo.datapoints;`;
    }
    return `SELECT id, type, name FROM ${db}.dbo.datapoints WHERE name=${nameLiteral(dbName, 'datapoints', name)};`;
}

export function getIdInsert(dbName: string, name: string, type: 0 | 1 | 2): string {
    const db = quoteMsIdentifier(dbName);
    return `INSERT INTO ${db}.dbo.datapoints (name, type) VALUES(${nameLiteral(dbName, 'datapoints', name)}, ${type});`;
}

export function getIdUpdate(dbName: string, id: number, type: 0 | 1 | 2): string {
    const db = quoteMsIdentifier(dbName);
    return `UPDATE ${db}.dbo.datapoints SET type=${type} WHERE id=${id};`;
}

export function getFromSelect(dbName: string, name?: string): string {
    const db = quoteMsIdentifier(dbName);
    if (name) {
        return `SELECT id, name FROM ${db}.dbo.sources WHERE name=${nameLiteral(dbName, 'sources', name)};`;
    }
    return `SELECT id, name FROM ${db}.dbo.sources;`;
}

export function getFromInsert(dbName: string, values: string): string {
    const db = quoteMsIdentifier(dbName);
    return `INSERT INTO ${db}.dbo.sources (name) VALUES(${nameLiteral(dbName, 'sources', values)});`;
}

export function getCounterDiff(
    dbName: string,
    options: {
        index: number;
        start: number;
        end: number;
    },
): string {
    const db = quoteMsIdentifier(dbName);
    // Take first real value after start
    const subQueryStart = `SELECT TOP 1 val, ts FROM ${db}.dbo.ts_number WHERE ${db}.dbo.ts_number.id=${options.index} AND ${db}.dbo.ts_number.ts>=${options.start} AND ${db}.dbo.ts_number.ts<${options.end} AND ${db}.dbo.ts_number.val IS NOT NULL ORDER BY ${db}.dbo.ts_number.ts ASC`;
    // Take last real value before the end
    const subQueryEnd = `SELECT TOP 1 val, ts FROM ${db}.dbo.ts_number WHERE ${db}.dbo.ts_number.id=${options.index} AND ${db}.dbo.ts_number.ts>=${options.start} AND ${db}.dbo.ts_number.ts<${options.end} AND ${db}.dbo.ts_number.val IS NOT NULL ORDER BY ${db}.dbo.ts_number.ts DESC`;
    // Take last value before start
    const subQueryFirst = `SELECT TOP 1 val, ts FROM ${db}.dbo.ts_number WHERE ${db}.dbo.ts_number.id=${options.index} AND ${db}.dbo.ts_number.ts< ${options.start} ORDER BY ${db}.dbo.ts_number.ts DESC`;
    // Take next value after end
    const subQueryLast = `SELECT TOP 1 val, ts FROM ${db}.dbo.ts_number WHERE ${db}.dbo.ts_number.id=${options.index} AND ${db}.dbo.ts_number.ts>=${options.end} ORDER BY ${db}.dbo.ts_number.ts ASC`;
    // get values from counters where counter values changed. No ORDER BY here: T-SQL forbids it in a
    // union member without TOP, and the outer ORDER BY sorts the combined result anyway.
    const subQueryCounterChanges = `SELECT val, ts FROM ${db}.dbo.ts_counter WHERE ${db}.dbo.ts_counter.id=${options.index} AND ${db}.dbo.ts_counter.ts>${options.start} AND ${db}.dbo.ts_counter.ts<${options.end} AND ${db}.dbo.ts_counter.val IS NOT NULL`;

    // Same shape as the other dialects: DISTINCT over the union, ordered by ts in the OUTER query -
    // sendResponseCounter consumes the rows positionally, so the order must be guaranteed here.
    return (
        `SELECT DISTINCT a.ts, a.val FROM ((${subQueryFirst})\n` +
        `UNION ALL (${subQueryStart})\n` +
        `UNION ALL (${subQueryEnd})\n` +
        `UNION ALL (${subQueryLast})\n` +
        `UNION ALL (${subQueryCounterChanges})) a ORDER BY a.ts;`
    );
}

export function getHistory(
    dbName: string,
    table: string,
    options: ioBroker.GetHistoryOptions & { index: number | null },
): string {
    const db = quoteMsIdentifier(dbName);
    let query = 'SELECT * FROM (SELECT ';
    if ((!options.start && options.count) || (options.aggregate === 'none' && options.count)) {
        query += ` TOP ${options.count}`;
    }
    query += ` ts, val${options.index === null ? `, ${table}.id as id` : ''}${options.ack ? `, ack` : ''}${
        options.from ? `, ${db}.dbo.sources.name as 'from'` : ''
    }${options.q ? `, q` : ''} FROM ${db}.dbo.${table}`;

    // LEFT JOIN: a value stored without `from` has `_from` = 0, which has no row in `sources`.
    // With an INNER JOIN, such values were missing from every query that asked for `from`.
    if (options.from) {
        query += ` LEFT OUTER JOIN ${db}.dbo.sources ON ${db}.dbo.sources.id=${db}.dbo.${table}._from`;
    }

    let where = '';

    if (options.index !== null) {
        where += ` ${db}.dbo.${table}.id=${options.index}`;
    }
    if (options.end) {
        where += `${where ? ` AND` : ''} ${db}.dbo.${table}.ts < ${options.end}`;
    }
    if (options.start) {
        where += `${where ? ` AND` : ''} ${db}.dbo.${table}.ts >= ${options.start}`;
    }
    if ((!options.start && options.count) || (options.aggregate === 'none' && options.count)) {
        where += ` ORDER BY ts`;

        if (
            (!options.start && options.count) ||
            (options.aggregate === 'none' && options.count && options.returnNewestEntries)
        ) {
            where += ` DESC`;
        } else {
            where += ` ASC`;
        }
    }
    where += `) AS t`;
    if (options.start) {
        // add last value before start
        let subQuery;
        let subWhere;
        subQuery = ` SELECT TOP 1 ts, val${options.index === null ? `, ${table}.id as id` : ''}${options.ack ? `, ack` : ''}${
            options.from ? `, ${db}.dbo.sources.name as 'from'` : ''
        }${options.q ? `, q` : ''} FROM ${db}.dbo.${table}`;
        if (options.from) {
            subQuery += ` LEFT OUTER JOIN ${db}.dbo.sources ON ${db}.dbo.sources.id=${db}.dbo.${table}._from`;
        }
        subWhere = '';
        if (options.index !== null) {
            subWhere += ` ${db}.dbo.${table}.id=${options.index}`;
        }
        if (options.ignoreNull) {
            //subWhere += (subWhere ? ` AND` : '') + ` val <> NULL`;
        }
        subWhere += `${subWhere ? ` AND` : ''} ${db}.dbo.${table}.ts < ${options.start}`;
        if (subWhere) {
            subQuery += ` WHERE ${subWhere}`;
        }
        subQuery += ` ORDER BY ${db}.dbo.${table}.ts DESC`;
        where += ` UNION ALL SELECT * FROM (${subQuery}) a`;

        // add next value after end
        subQuery = ` SELECT TOP 1 ts, val${options.index === null ? `, ${table}.id as id` : ''}${options.ack ? `, ack` : ''}${
            options.from ? `, ${db}.dbo.sources.name as 'from'` : ''
        }${options.q ? `, q` : ''} FROM ${db}.dbo.${table}`;
        if (options.from) {
            subQuery += ` LEFT OUTER JOIN ${db}.dbo.sources ON ${db}.dbo.sources.id=${db}.dbo.${table}._from`;
        }
        subWhere = '';
        if (options.index !== null) {
            subWhere += ` ${db}.dbo.${table}.id=${options.index}`;
        }
        if (options.ignoreNull) {
            //subWhere += (subWhere ? ` AND` : '') + ` val <> NULL`;
        }
        subWhere += `${subWhere ? ` AND` : ''} ${db}.dbo.${table}.ts >= ${options.end}`;
        if (subWhere) {
            subQuery += ` WHERE ${subWhere}`;
        }
        subQuery += ` ORDER BY ${db}.dbo.${table}.ts ASC`;
        where += ` UNION ALL SELECT * FROM (${subQuery}) b`;
    }

    if (where) {
        query += ` WHERE ${where}`;
    }

    query += ` ORDER BY ts`;
    if (
        (!options.start && options.count) ||
        (options.aggregate === 'none' && options.count && options.returnNewestEntries)
    ) {
        query += ` DESC`;
    } else {
        query += ` ASC`;
    }
    query += `;`;

    return query;
}

export function deleteFromTable(dbName: string, table: TableName, index: number, start?: number, end?: number): string {
    const db = quoteMsIdentifier(dbName);
    let query = `DELETE FROM ${db}.dbo.${table} WHERE`;
    query += ` id=${index}`;
    if (start && end) {
        query += ` AND ${db}.dbo.${table}.ts>=${start} AND ${db}.dbo.${table}.ts<=${end}`;
    } else if (start) {
        query += ` AND ${db}.dbo.${table}.ts=${start}`;
    }

    query += ';';

    return query;
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
export function deleteDatapoint(dbName: string, index: number): string {
    const db = quoteMsIdentifier(dbName);
    return `DELETE FROM ${db}.dbo.datapoints WHERE id=${index};`;
}

export function update(
    dbName: string,
    index: number,
    originalState: { val: number | string | boolean | null | undefined; ts: number; q?: number; ack?: boolean },
    from: number | undefined,
    table: TableName,
): string {
    const db = quoteMsIdentifier(dbName);
    // copy, so the caller's state object is not changed (see insert())
    const state = { ...originalState };
    if (!state || state.val === null || state.val === undefined) {
        state.val = 'NULL';
    } else if (table === 'ts_bool') {
        state.val = boolOrNull(state.val, true);
    } else if (table === 'ts_string') {
        state.val = quoteNString(state.val);
    } else if (table === 'ts_number' || table === 'ts_counter') {
        state.val = numberOrNull(state.val);
    }

    let query = `UPDATE ${db}.dbo.${table} SET `;
    const vals: string[] = [];
    if (state.val !== undefined) {
        vals.push(`val=${state.val}`);
    }
    if (state.q !== undefined) {
        vals.push(`q=${state.q}`);
    }
    if (from !== undefined) {
        vals.push(`_from=${from}`);
    }
    if (state.ack !== undefined) {
        vals.push(`ack=${state.ack ? 1 : 0}`);
    }
    query += vals.join(', ');
    query += ` WHERE id=${index} AND ts=${state.ts};`;

    return query;
}

function rawEntriesWhere(
    dbName: string,
    table: TableName,
    index: number,
    options: { start?: number; end?: number },
): string {
    const db = quoteMsIdentifier(dbName);
    let where = `${db}.dbo.${table}.id=${index}`;
    if (options.start) {
        where += ` AND ${db}.dbo.${table}.ts>=${options.start}`;
    }
    if (options.end) {
        where += ` AND ${db}.dbo.${table}.ts<=${options.end}`;
    }
    return where;
}

export function getRawEntries(dbName: string, table: TableName, index: number, options: RawEntriesOptions): string {
    const db = quoteMsIdentifier(dbName);
    return (
        `SELECT ${db}.dbo.${table}.ts, ${db}.dbo.${table}.val, ${db}.dbo.${table}.ack, ${db}.dbo.${table}.q, ${db}.dbo.sources.name AS 'from' FROM ${db}.dbo.${table}` +
        ` LEFT OUTER JOIN ${db}.dbo.sources ON ${db}.dbo.sources.id=${db}.dbo.${table}._from` +
        ` WHERE ${rawEntriesWhere(dbName, table, index, options)}` +
        ` ORDER BY ${db}.dbo.${table}.ts ${options.sort === 'asc' ? 'ASC' : 'DESC'}` +
        ` OFFSET ${options.offset} ROWS FETCH NEXT ${options.limit} ROWS ONLY;`
    );
}

export function getRawEntriesCount(
    dbName: string,
    table: TableName,
    index: number,
    options: { start?: number; end?: number },
): string {
    const db = quoteMsIdentifier(dbName);
    return `SELECT COUNT(*) AS total FROM ${db}.dbo.${table} WHERE ${rawEntriesWhere(dbName, table, index, options)};`;
}
