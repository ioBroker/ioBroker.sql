"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.init = init;
exports.destroy = destroy;
exports.getFirstTs = getFirstTs;
exports.getIdCounts = getIdCounts;
exports.getTableSize = getTableSize;
exports.insert = insert;
exports.retention = retention;
exports.getIdSelect = getIdSelect;
exports.getIdInsert = getIdInsert;
exports.getIdUpdate = getIdUpdate;
exports.getFromSelect = getFromSelect;
exports.getFromInsert = getFromInsert;
exports.getCounterDiff = getCounterDiff;
exports.getHistory = getHistory;
exports.deleteFromTable = deleteFromTable;
exports.deleteDatapoint = deleteDatapoint;
exports.update = update;
exports.getRawEntries = getRawEntries;
exports.getRawEntriesCount = getRawEntriesCount;
function init(_dbName) {
    return [
        'CREATE TABLE sources    (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, name TEXT);',
        'CREATE TABLE datapoints (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, name TEXT, type INTEGER);',
        'CREATE TABLE ts_number  (id INTEGER, ts INTEGER, val REAL,    ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));',
        'CREATE TABLE ts_string  (id INTEGER, ts INTEGER, val TEXT,    ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));',
        'CREATE TABLE ts_bool    (id INTEGER, ts INTEGER, val BOOLEAN, ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));',
        'CREATE TABLE ts_counter (id INTEGER, ts INTEGER, val REAL, PRIMARY KEY(id, ts));',
    ];
}
function destroy(_dbName) {
    return [
        'DROP TABLE ts_counter;',
        'DROP TABLE ts_number;',
        'DROP TABLE ts_string;',
        'DROP TABLE ts_bool;',
        'DROP TABLE sources;',
        'DROP TABLE datapoints;',
    ];
}
function getFirstTs(_dbName, table) {
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
function getIdCounts(_dbName, table) {
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
function getTableSize(_dbName, table) {
    // dbstat is a virtual table that is not compiled into every sqlite3 build, so the caller has
    // to treat a failure here as "size unknown" rather than as an error.
    return `SELECT CASE WHEN SUM(ncell) > 0 THEN SUM(payload) / SUM(ncell) ELSE 0 END AS avg_row_length, SUM(pgsize) AS total_bytes FROM dbstat WHERE name='${table}';`;
}
function insert(_dbName, index, values) {
    const insertValues = {};
    values.forEach(value => {
        // state, from, db
        insertValues[value.table] ||= [];
        if (!value.state || value.state.val === null || value.state.val === undefined) {
            value.state.val = 'NULL';
        }
        else if (value.table === 'ts_bool') {
            value.state.val = value.state.val ? 1 : 0;
        }
        else if (value.table === 'ts_string') {
            value.state.val = `'${value.state.val.toString().replace(/'/g, '')}'`;
        }
        else if (value.table === 'ts_number') {
            if (isNaN(value.state.val)) {
                value.state.val = 'NULL';
            }
        }
        if (value.table === 'ts_counter') {
            insertValues[value.table].push(`(${index}, ${value.state.ts}, ${value.state.val})`);
        }
        else {
            insertValues[value.table].push(`(${index}, ${value.state.ts}, ${value.state.val}, ${value.state.ack ? 1 : 0}, ${value.from || 0}, ${value.state.q || 0})`);
        }
    });
    const query = [];
    for (const table in insertValues) {
        if (table === 'ts_counter') {
            // ts_counter has PRIMARY KEY(id, ts) in SQLite, unlike MySQL and PostgreSQL
            while (insertValues[table].length) {
                query.push(`INSERT INTO ts_counter (id, ts, val) VALUES ${insertValues[table].splice(0, 500).join(',')} ON CONFLICT DO NOTHING;`);
            }
        }
        else {
            while (insertValues[table].length) {
                // ts_number/ts_string/ts_bool have PRIMARY KEY(id, ts). Importing history writes rows
                // that may already exist, and a single duplicate would otherwise abort the whole batch.
                // DO NOTHING only applies to uniqueness conflicts, so INSERT OR IGNORE is deliberately
                // not used here - it would also swallow NOT NULL and CHECK violations.
                query.push(`INSERT INTO ${table} (id, ts, val, ack, _from, q) VALUES ${insertValues[table].splice(0, 500).join(',')} ON CONFLICT DO NOTHING;`);
            }
        }
    }
    return query;
}
function retention(_dbName, index, table, retention) {
    const d = new Date();
    d.setSeconds(-retention);
    let query = `DELETE FROM ${table} WHERE`;
    query += ` id=${index}`;
    query += ` AND ts < ${d.getTime()}`;
    query += ';';
    return query;
}
function getIdSelect(_dbName, name) {
    if (!name) {
        return 'SELECT id, type, name FROM datapoints;';
    }
    return `SELECT id, type, name FROM datapoints WHERE name='${name}';`;
}
function getIdInsert(_dbName, name, type) {
    return `INSERT INTO datapoints (name, type) VALUES('${name}', ${type});`;
}
function getIdUpdate(_dbName, id, type) {
    return `UPDATE datapoints SET type = ${type} WHERE id = ${id};`;
}
function getFromSelect(_dbName, name) {
    if (!name) {
        return 'SELECT id, name FROM sources;';
    }
    return `SELECT id FROM sources WHERE name='${name}';`;
}
function getFromInsert(_dbName, values) {
    return `INSERT INTO sources (name) VALUES('${values}');`;
}
function getCounterDiff(_dbName, options) {
    // Take first real value after start
    const subQueryStart = `SELECT ts, val FROM ts_number  WHERE id=${options.index} AND ts>=${options.start} AND ts<${options.end} AND val IS NOT NULL ORDER BY ts ASC LIMIT 1`;
    // Take last real value before end
    const subQueryEnd = `SELECT ts, val FROM ts_number  WHERE id=${options.index} AND ts>=${options.start} AND ts<${options.end} AND val IS NOT NULL ORDER BY ts DESC LIMIT 1`;
    // Take last value before start
    const subQueryFirst = `SELECT ts, val FROM ts_number  WHERE id=${options.index} AND ts< ${options.start} ORDER BY ts DESC LIMIT 1`;
    // Take next value after end
    const subQueryLast = `SELECT ts, val FROM ts_number  WHERE id=${options.index} AND ts>= ${options.end} ORDER BY ts ASC  LIMIT 1`;
    // get values from counters where counter changed from up to down (e.g. counter changed).
    // No ORDER BY here: SQLite forbids ORDER BY on compound-select members, and the outer
    // ORDER BY sorts the combined result anyway.
    const subQueryCounterChanges = `SELECT ts, val FROM ts_counter WHERE id=${options.index} AND ts>${options.start} AND ts<${options.end} AND val IS NOT NULL`;
    // SQLite forbids ORDER BY/LIMIT directly on parenthesized compound-select members, so every
    // TOP-1-style subquery is wrapped as a FROM-subquery (where ORDER BY + LIMIT are legal). The
    // outer ORDER BY is required: sendResponseCounter consumes the rows positionally.
    return (`SELECT DISTINCT a.ts, a.val FROM (SELECT ts, val FROM (${subQueryFirst})\n` +
        `UNION ALL SELECT ts, val FROM (${subQueryStart})\n` +
        `UNION ALL SELECT ts, val FROM (${subQueryEnd})\n` +
        `UNION ALL SELECT ts, val FROM (${subQueryLast})\n` +
        `UNION ALL ${subQueryCounterChanges}\n` +
        `) a ORDER BY a.ts;`);
}
function getHistory(_dbName, table, options) {
    let query = `SELECT ts, val${options.index === null ? `, ${table}.id as id` : ''}${options.ack ? ', ack' : ''}${options.from ? `, sources.name as 'from'` : ''}${options.q ? ', q' : ''} FROM ${table}`;
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
        // add last value before start
        let subQuery;
        let subWhere;
        subQuery = ` SELECT ts, val${options.index === null ? `, ${table}.id as id` : ''}${options.ack ? ', ack' : ''}${options.from ? `, sources.name as 'from'` : ''}${options.q ? ', q' : ''} FROM ${table}`;
        if (options.from) {
            subQuery += ` INNER JOIN sources ON sources.id=${table}._from`;
        }
        subWhere = '';
        if (options.index !== null) {
            subWhere += ` ${table}.id=${options.index}`;
        }
        if (options.ignoreNull) {
            // subWhere += (subWhere ? " AND" : '') + " val <> NULL";
        }
        subWhere += `${subWhere ? ' AND' : ''} ${table}.ts < ${options.start}`;
        if (subWhere) {
            subQuery += ` WHERE ${subWhere}`;
        }
        subQuery += ` ORDER BY ${table}.ts DESC LIMIT 1`;
        where += ` UNION ALL SELECT * from (${subQuery})`;
        // add next value after end
        subQuery = ` SELECT ts, val${options.index === null ? `, ${table}.id as id` : ''}${options.ack ? ', ack' : ''}${options.from ? `, sources.name as 'from'` : ''}${options.q ? ', q' : ''} FROM ${table}`;
        if (options.from) {
            subQuery += ` INNER JOIN sources ON sources.id=${table}._from`;
        }
        subWhere = '';
        if (options.index !== null) {
            subWhere += ` ${table}.id=${options.index}`;
        }
        if (options.ignoreNull) {
            // subWhere += (subWhere ? " AND" : '') + " val <> NULL";
        }
        subWhere += `${subWhere ? ' AND' : ''} ${table}.ts >= ${options.end}`;
        if (subWhere) {
            subQuery += ` WHERE ${subWhere}`;
        }
        subQuery += ` ORDER BY ${table}.ts ASC LIMIT 1`;
        where += ` UNION ALL SELECT * from (${subQuery}) `;
    }
    if (where) {
        query += ` WHERE ${where}`;
    }
    query += ' ORDER BY ts';
    if ((!options.start && options.count) ||
        (options.aggregate === 'none' && options.count && options.returnNewestEntries)) {
        query += ' DESC';
    }
    else {
        query += ' ASC';
    }
    if ((!options.start && options.count) || (options.aggregate === 'none' && options.count)) {
        query += ` LIMIT ${options.count + 2}`;
    }
    query += ';';
    return query;
}
function deleteFromTable(_dbName, table, index, start, end) {
    let query = `DELETE FROM ${table} WHERE`;
    query += ` id=${index}`;
    if (start && end) {
        query += ` AND ts>=${start} AND ts <= ${end}`;
    }
    else if (start) {
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
function deleteDatapoint(_dbName, index) {
    return `DELETE FROM datapoints WHERE id=${index};`;
}
function update(_dbName, index, state, from, table) {
    if (!state || state.val === null || state.val === undefined) {
        state.val = 'NULL';
    }
    else if (table === 'ts_string') {
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
        vals.push(`ack=${state.ack ? 1 : 0}`);
    }
    query += vals.join(', ');
    query += ' WHERE ';
    query += ` id=${index}`;
    query += ` AND ts=${state.ts}`;
    query += ';';
    return query;
}
function rawEntriesWhere(table, index, options) {
    let where = `${table}.id=${index}`;
    if (options.start) {
        where += ` AND ${table}.ts>=${options.start}`;
    }
    if (options.end) {
        where += ` AND ${table}.ts<=${options.end}`;
    }
    return where;
}
function getRawEntries(_dbName, table, index, options) {
    return (`SELECT ${table}.ts, ${table}.val, ${table}.ack, ${table}.q, sources.name AS "from" FROM ${table}` +
        ` LEFT JOIN sources ON sources.id=${table}._from` +
        ` WHERE ${rawEntriesWhere(table, index, options)}` +
        ` ORDER BY ${table}.ts ${options.sort === 'asc' ? 'ASC' : 'DESC'}` +
        ` LIMIT ${options.limit} OFFSET ${options.offset};`);
}
function getRawEntriesCount(_dbName, table, index, options) {
    return `SELECT COUNT(*) AS total FROM ${table} WHERE ${rawEntriesWhere(table, index, options)};`;
}
//# sourceMappingURL=sqlite.js.map