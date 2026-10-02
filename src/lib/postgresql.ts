import type { RawEntriesOptions, TableName } from '../types';

export function init(_dbName: string, _doNotCreateDatabase?: boolean): string[] {
    return [
        'CREATE TABLE sources    (id SERIAL NOT NULL PRIMARY KEY, name TEXT);',
        'CREATE TABLE datapoints (id SERIAL NOT NULL PRIMARY KEY, name TEXT, type INTEGER);',
        'CREATE TABLE ts_number  (id INTEGER NOT NULL, ts BIGINT, val REAL,    ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));',
        'CREATE TABLE ts_string  (id INTEGER NOT NULL, ts BIGINT, val TEXT,    ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));',
        'CREATE TABLE ts_bool    (id INTEGER NOT NULL, ts BIGINT, val BOOLEAN, ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));',
        'CREATE TABLE ts_counter (id INTEGER NOT NULL, ts BIGINT, val REAL);',
    ];
}

export function destroy(_dbName: string): string[] {
    return [
        'DROP TABLE ts_counter;',
        'DROP TABLE ts_number;',
        'DROP TABLE ts_string;',
        'DROP TABLE ts_bool;',
        'DROP TABLE sources;',
        'DROP TABLE datapoints;',
    ];
}

export function getFirstTs(_dbName: string, table: TableName): string {
    return `SELECT id, MIN(ts) AS ts FROM ${table} GROUP BY id;`;
}

/**
 * Count the rows and the covered time range per datapoint index.
 *
 * One query per table instead of one per datapoint: a database that has collected data for years
 * holds thousands of datapoints, and `GROUP BY id` lets the engine do the work in a single pass.
 *
 * @param _dbName unused, PostgreSQL and SQLite connect to the target database directly name of the database
 * @param table the time series table to summarize
 */
export function getIdCounts(_dbName: string, table: TableName): string {
    return `SELECT id, COUNT(*) AS cnt, MIN(ts) AS first_ts, MAX(ts) AS last_ts FROM ${table} GROUP BY id;`;
}

/**
 * Average bytes per row and total bytes of one time series table.
 *
 * There is no portable way to ask for the size of the rows belonging to a single datapoint, so the
 * statistics multiply this average by the row count. The result is an estimate and has to be
 * presented as one.
 *
 * @param _dbName unused, PostgreSQL and SQLite connect to the target database directly name of the database
 * @param table the time series table to measure
 */
export function getTableSize(_dbName: string, table: TableName): string {
    // pg_total_relation_size covers table plus indexes and TOAST; reltuples is the planner's
    // row estimate, which is enough to derive an average width.
    return `SELECT CASE WHEN c.reltuples > 0 THEN (pg_total_relation_size(c.oid) / c.reltuples)::bigint ELSE 0 END AS avg_row_length, pg_total_relation_size(c.oid) AS total_bytes FROM pg_class c WHERE c.relname='${table}';`;
}

export function insert(
    _dbName: string,
    index: number,
    values: {
        table: TableName;
        state: { val: any; ts: number; ack?: boolean; q?: number };
        from?: number;
    }[],
): string[] {
    const insertValues: { [table: string]: string[] } = {};
    values.forEach(value => {
        // state, from, table
        insertValues[value.table] = insertValues[value.table] || [];

        if (!value.state || value.state.val === null || value.state.val === undefined) {
            value.state.val = 'NULL';
        } else if (value.table === 'ts_string') {
            value.state.val = `'${value.state.val.toString().replace(/'/g, '')}'`;
        } else if (value.table === 'ts_number') {
            if (isNaN(value.state.val)) {
                value.state.val = 'NULL';
            }
        }

        if (value.table === 'ts_counter') {
            insertValues[value.table].push(`(${index}, ${value.state.ts}, ${value.state.val})`);
        } else {
            insertValues[value.table].push(
                `(${index}, ${value.state.ts}, ${value.state.val}, ${!!value.state.ack}, ${value.from || 0}, ${value.state.q || 0})`,
            );
        }
    });

    const query: string[] = [];
    for (const table in insertValues) {
        if (table === 'ts_counter') {
            // no ON CONFLICT here: ts_counter has no primary key in PostgreSQL (unlike SQLite),
            // so there is no uniqueness conflict to suppress
            while (insertValues[table].length) {
                query.push(
                    `INSERT INTO ts_counter (id, ts, val) VALUES ${insertValues[table].splice(0, 500).join(',')};`,
                );
            }
        } else {
            while (insertValues[table].length) {
                // ts_number/ts_string/ts_bool have PRIMARY KEY(id, ts). Importing history writes rows
                // that may already exist, and a single duplicate would otherwise abort the whole batch.
                // DO NOTHING only applies to uniqueness conflicts, so conversion errors still surface.
                query.push(
                    `INSERT INTO ${table} (id, ts, val, ack, _from, q) VALUES ${insertValues[table].splice(0, 500).join(',')} ON CONFLICT DO NOTHING;`,
                );
            }
        }
    }

    return query;
}

export function retention(_dbName: string, index: number, table: TableName, retention: number): string {
    const d = new Date();
    d.setSeconds(-retention);
    let query = `DELETE FROM ${table} WHERE`;
    query += ` id=${index}`;
    query += ` AND ts < ${d.getTime()}`;
    query += ';';

    return query;
}

export function getIdSelect(_dbName: string, name?: string): string {
    if (!name) {
        return 'SELECT id, type, name FROM datapoints;';
    }
    return `SELECT id, type, name FROM datapoints WHERE name='${name}';`;
}

export function getIdInsert(_dbName: string, name: string, type: 0 | 1 | 2): string {
    return `INSERT INTO datapoints (name, type) VALUES('${name}', ${type});`;
}

export function getIdUpdate(_dbName: string, id: number, type: 0 | 1 | 2): string {
    return `UPDATE datapoints SET type = ${type} WHERE id = ${id};`;
}

export function getFromSelect(_dbName: string, name?: string): string {
    if (name) {
        return `SELECT id FROM sources WHERE name='${name}';`;
    }
    return 'SELECT id, name FROM sources;';
}

export function getFromInsert(dbName: string, values: string): string {
    return `INSERT INTO sources (name) VALUES('${values}');`;
}

export function getCounterDiff(
    _dbName: string,
    options: {
        index: number;
        start: number;
        end: number;
    },
): string {
    // Take first real value after start
    const subQueryStart = `SELECT ts, val FROM ts_number  WHERE id=${options.index} AND ts>=${options.start} AND ts<${options.end} AND val IS NOT NULL ORDER BY ts ASC LIMIT 1`;
    // Take last real value before the end
    const subQueryEnd = `SELECT ts, val FROM ts_number  WHERE id=${options.index} AND ts>=${options.start} AND ts<${options.end} AND val IS NOT NULL ORDER BY ts DESC LIMIT 1`;
    // Take last value before start
    const subQueryFirst = `SELECT ts, val FROM ts_number  WHERE id=${options.index} AND ts< ${options.start} ORDER BY ts DESC LIMIT 1`;
    // Take next value after end
    const subQueryLast = `SELECT ts, val FROM ts_number  WHERE id=${options.index} AND ts>= ${options.end} ORDER BY ts ASC  LIMIT 1`;
    // get values from counters where counter changed from up to down (e.g. counter changed).
    // No ORDER BY here: the outer ORDER BY sorts the combined result anyway.
    const subQueryCounterChanges = `SELECT ts, val FROM ts_counter WHERE id=${options.index} AND ts>${options.start} AND ts<${options.end} AND val IS NOT NULL`;

    // The ORDER BY belongs in the OUTER query, not inside the derived table: sendResponseCounter
    // consumes the rows positionally, and a derived table's ordering is not guaranteed to survive
    // the SELECT DISTINCT above it - PostgreSQL may hash-aggregate instead of sort/unique.
    return (
        `SELECT DISTINCT a.ts, a.val FROM ((${subQueryFirst})\n` +
        `UNION ALL (${subQueryStart})\n` +
        `UNION ALL (${subQueryEnd})\n` +
        `UNION ALL (${subQueryLast})\n` +
        `UNION ALL (${subQueryCounterChanges})) a ORDER BY a.ts;`
    );
}

export function getHistory(
    _dbName: string,
    table: string,
    options: ioBroker.GetHistoryOptions & { index: number | null },
): string {
    let query = `SELECT ts, val${options.index === null ? `, ${table}.id as id` : ''}${options.ack ? ', ack' : ''}${
        options.from ? ', sources.name as from' : ''
    }${options.q ? ', q' : ''} FROM ${table}`;

    if (options.from) {
        query += ` INNER JOIN sources ON sources.id=${table}._from`;
    }

    let where = '';

    if (options.index !== null) {
        where += ` ${table}.id=${options.index}`;
    }
    if (options.end) {
        where += `${where ? ' AND' : ''} ${table}.ts < ${options.end}`;
    }
    if (options.start) {
        where += `${where ? ' AND' : ''} ${table}.ts >= ${options.start}`;

        //add last value before start
        let subQuery;
        let subWhere;
        subQuery = ` SELECT ts, val${options.index === null ? `, ${table}.id as id` : ''}${options.ack ? ', ack' : ''}${
            options.from ? ', sources.name as from' : ''
        }${options.q ? ', q' : ''} FROM ${table}`;
        if (options.from) {
            subQuery += ` INNER JOIN sources ON sources.id=${table}._from`;
        }
        subWhere = '';
        if (options.index !== null) {
            subWhere += ` ${table}.id=${options.index}`;
        }
        if (options.ignoreNull) {
            //subWhere += (subWhere ? " AND" : '') + " val <> NULL";
        }
        subWhere += `${subWhere ? ' AND' : ''} ${table}.ts < ${options.start}`;
        if (subWhere) {
            subQuery += ` WHERE ${subWhere}`;
        }
        subQuery += ` ORDER BY ${table}.ts DESC LIMIT 1`;
        where += ` UNION ALL (${subQuery})`;

        //add next value after end
        subQuery = ` SELECT ts, val${options.index === null ? `, ${table}.id as id` : ''}${options.ack ? ', ack' : ''}${
            options.from ? ', sources.name as from' : ''
        }${options.q ? ', q' : ''} FROM ${table}`;
        if (options.from) {
            subQuery += ` INNER JOIN sources ON sources.id=${table}._from`;
        }
        subWhere = '';
        if (options.index !== null) {
            subWhere += ` ${table}.id=${options.index}`;
        }
        if (options.ignoreNull) {
            //subWhere += (subWhere ? " AND" : '') + " val <> NULL";
        }
        subWhere += `${subWhere ? ' AND' : ''} ${table}.ts >= ${options.end}`;
        if (subWhere) {
            subQuery += ` WHERE ${subWhere}`;
        }
        subQuery += ` ORDER BY ${table}.ts ASC LIMIT 1`;
        where += ` UNION ALL(${subQuery})`;
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
        query += ` LIMIT ${options.count + 2}`;
    }

    query += ';';
    return query;
}

export function deleteFromTable(
    _dbName: string,
    table: TableName,
    index: number,
    start?: number,
    end?: number,
): string {
    let query = `DELETE FROM ${table} WHERE`;
    query += ` id=${index}`;

    if (start && end) {
        query += ` AND ts>=${start} AND ts <= ${end}`;
    } else if (start) {
        query += ` AND ts=${start}`;
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
 * @param _dbName unused, PostgreSQL and SQLite connect to the target database directly name of the database
 * @param index the integer key of the datapoint
 */
export function deleteDatapoint(_dbName: string, index: number): string {
    return `DELETE FROM datapoints WHERE id=${index};`;
}

export function update(
    _dbName: string,
    index: number,
    state: { val: number | string | boolean | null | undefined; ts: number; q?: number; ack?: boolean },
    from: number,
    table: 'ts_bool' | 'ts_number' | 'ts_string' | 'ts_counter',
): string {
    if (!state || state.val === null || state.val === undefined) {
        state.val = 'NULL';
    } else if (table === 'ts_string') {
        state.val = `'${state.val.toString().replace(/'/g, '')}'`;
    }

    let query = `UPDATE ${table} SET `;
    const vals = [];
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
        vals.push(`ack=${!!state.ack}`);
    }
    query += vals.join(', ');
    query += ' WHERE ';
    query += ` id=${index}`;
    query += ` AND ts=${state.ts}`;
    query += ';';

    return query;
}

function rawEntriesWhere(table: TableName, index: number, options: { start?: number; end?: number }): string {
    let where = `${table}.id=${index}`;
    if (options.start) {
        where += ` AND ${table}.ts>=${options.start}`;
    }
    if (options.end) {
        where += ` AND ${table}.ts<=${options.end}`;
    }
    return where;
}

export function getRawEntries(_dbName: string, table: TableName, index: number, options: RawEntriesOptions): string {
    return (
        `SELECT ${table}.ts, ${table}.val, ${table}.ack, ${table}.q, sources.name AS "from" FROM ${table}` +
        ` LEFT JOIN sources ON sources.id=${table}._from` +
        ` WHERE ${rawEntriesWhere(table, index, options)}` +
        ` ORDER BY ${table}.ts ${options.sort === 'asc' ? 'ASC' : 'DESC'}` +
        ` LIMIT ${options.limit} OFFSET ${options.offset};`
    );
}

export function getRawEntriesCount(
    _dbName: string,
    table: TableName,
    index: number,
    options: { start?: number; end?: number },
): string {
    return `SELECT COUNT(*) AS total FROM ${table} WHERE ${rawEntriesWhere(table, index, options)};`;
}
