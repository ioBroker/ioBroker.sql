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
function init(dbName, doNotCreateDatabase) {
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
function destroy(dbName) {
    return [
        `DROP TABLE \`${dbName}\`.ts_counter;`,
        `DROP TABLE \`${dbName}\`.ts_number;`,
        `DROP TABLE \`${dbName}\`.ts_string;`,
        `DROP TABLE \`${dbName}\`.ts_bool;`,
        `DROP TABLE \`${dbName}\`.sources;`,
        `DROP TABLE \`${dbName}\`.datapoints;`,
        `DROP DATABASE \`${dbName}\`;`,
    ];
}
function getFirstTs(dbName, table) {
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
function getIdCounts(dbName, table) {
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
function getTableSize(dbName, table) {
    // information_schema knows the real storage footprint, so the estimate does not have to
    // guess at column widths. AVG_ROW_LENGTH is 0 for an empty table.
    return `SELECT AVG_ROW_LENGTH AS avg_row_length, DATA_LENGTH + INDEX_LENGTH AS total_bytes FROM information_schema.TABLES WHERE TABLE_SCHEMA='${dbName}' AND TABLE_NAME='${table}';`;
}
function insert(dbName, index, values) {
    const insertValues = {};
    values.forEach(value => {
        // state, from, db
        insertValues[value.table] = insertValues[value.table] || [];
        if (!value.state || value.state.val === null || value.state.val === undefined) {
            value.state.val = 'NULL';
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
            // no ON DUPLICATE KEY here: ts_counter has no primary key in MySQL (unlike SQLite),
            // so there is no uniqueness conflict to suppress
            while (insertValues[table].length) {
                query.push(`INSERT INTO \`${dbName}\`.ts_counter (id, ts, val) VALUES ${insertValues[table].splice(0, 500).join(',')};`);
            }
        }
        else {
            while (insertValues[table].length) {
                // ts_number/ts_string/ts_bool have PRIMARY KEY(id, ts). Importing history writes rows
                // that may already exist, and a single duplicate would otherwise abort the whole batch.
                // "id=id" is a no-op assignment, so this suppresses the duplicate key error and nothing
                // else - INSERT IGNORE would also hide truncation and conversion errors.
                query.push(`INSERT INTO \`${dbName}\`.${table} (id, ts, val, ack, _from, q) VALUES ${insertValues[table].splice(0, 500).join(',')} ON DUPLICATE KEY UPDATE id=id;`);
            }
        }
    }
    return query;
}
function retention(dbName, index, table, retention) {
    const d = new Date();
    d.setSeconds(-retention);
    let query = `DELETE FROM \`${dbName}\`.${table} WHERE`;
    query += ` id=${index}`;
    query += ` AND ts < ${d.getTime()}`;
    query += ';';
    return query;
}
function getIdSelect(dbName, name) {
    if (!name) {
        return `SELECT id, type, name FROM \`${dbName}\`.datapoints;`;
    }
    return `SELECT id, type, name FROM \`${dbName}\`.datapoints WHERE name='${name}';`;
}
function getIdInsert(dbName, name, type) {
    return `INSERT INTO \`${dbName}\`.datapoints (name, type) VALUES('${name}', ${type});`;
}
function getIdUpdate(dbName, id, type) {
    return `UPDATE \`${dbName}\`.datapoints SET type=${type} WHERE id=${id};`;
}
function getFromSelect(dbName, name) {
    if (name) {
        return `SELECT id FROM \`${dbName}\`.sources WHERE name='${name}';`;
    }
    return `SELECT id, name FROM \`${dbName}\`.sources;`;
}
function getFromInsert(dbName, values) {
    return `INSERT INTO \`${dbName}\`.sources (name) VALUES('${values}');`;
}
function getCounterDiff(dbName, options) {
    // Take first real value after start
    const subQueryStart = `SELECT ts, val FROM \`${dbName}\`.ts_number  WHERE id=${options.index} AND ts>=${options.start} AND ts<${options.end} AND val IS NOT NULL ORDER BY ts ASC LIMIT 1`;
    // Take last real value before the end
    const subQueryEnd = `SELECT ts, val FROM \`${dbName}\`.ts_number  WHERE id=${options.index} AND ts>=${options.start} AND ts<${options.end} AND val IS NOT NULL ORDER BY ts DESC LIMIT 1`;
    // Take last value before start
    const subQueryFirst = `SELECT ts, val FROM \`${dbName}\`.ts_number  WHERE id=${options.index} AND ts< ${options.start} ORDER BY ts DESC LIMIT 1`;
    // Take next value after end
    const subQueryLast = `SELECT ts, val FROM \`${dbName}\`.ts_number  WHERE id=${options.index} AND ts>= ${options.end} ORDER BY ts ASC LIMIT 1`;
    // get values from counters where counter changed from up to down (e.g. counter changed).
    // No ORDER BY here: MySQL 8 ignores it in a union member without LIMIT, and the outer
    // ORDER BY sorts the combined result anyway.
    const subQueryCounterChanges = `SELECT ts, val FROM \`${dbName}\`.ts_counter WHERE id=${options.index} AND ts>${options.start} AND ts<${options.end} AND val IS NOT NULL`;
    // The ORDER BY belongs in the OUTER query, not inside the derived table: sendResponseCounter
    // consumes the rows positionally, and a derived table's ordering is not guaranteed to survive
    // the SELECT DISTINCT above it.
    return (`SELECT DISTINCT a.ts, a.val FROM ((${subQueryFirst})\n` +
        `UNION ALL (${subQueryStart})\n` +
        `UNION ALL (${subQueryEnd})\n` +
        `UNION ALL (${subQueryLast})\n` +
        `UNION ALL (${subQueryCounterChanges})) a ORDER BY a.ts;`);
}
function getHistory(dbName, table, options) {
    let query = `SELECT ts, val${options.index === null ? `, ${table}.id as id` : ''}${options.ack ? ', ack' : ''}${options.from ? `, \`${dbName}\`.sources.name as 'from'` : ''}${options.q ? ', q' : ''} FROM \`${dbName}\`.${table}`;
    if (options.from) {
        query += ` INNER JOIN \`${dbName}\`.sources ON \`${dbName}\`.sources.id=\`${dbName}\`.${table}._from`;
    }
    let where = '';
    if (options.index !== null) {
        where += ` \`${dbName}\`.${table}.id=${options.index}`;
    }
    if (options.end) {
        where += `${where ? ' AND' : ''} \`${dbName}\`.${table}.ts < ${options.end}`;
    }
    if (options.start) {
        where += `${where ? ' AND' : ''} \`${dbName}\`.${table}.ts >= ${options.start}`;
        let subQuery;
        let subWhere;
        subQuery = ` SELECT ts, val${options.index === null ? `, ${table}.id as id` : ''}${options.ack ? ', ack' : ''}${options.from ? `, \`${dbName}\`.sources.name as 'from'` : ''}${options.q ? ', q' : ''} FROM \`${dbName}\`.${table}`;
        if (options.from) {
            subQuery += ` INNER JOIN \`${dbName}\`.sources ON \`${dbName}\`.sources.id=\`${dbName}\`.${table}._from`;
        }
        subWhere = '';
        if (options.index !== null) {
            subWhere += ` \`${dbName}\`.${table}.id=${options.index}`;
        }
        if (options.ignoreNull) {
            // subWhere += (subWhere ? " AND" : "") + " val <> NULL";
        }
        subWhere += `${subWhere ? ' AND' : ''} \`${dbName}\`.${table}.ts < ${options.start}`;
        if (subWhere) {
            subQuery += ` WHERE ${subWhere}`;
        }
        subQuery += ` ORDER BY \`${dbName}\`.${table}.ts DESC LIMIT 1`;
        where += ` UNION ALL (${subQuery})`;
        // add next value after end
        subQuery = ` SELECT ts, val${options.index === null ? `, ${table}.id as id` : ''}${options.ack ? ', ack' : ''}${options.from ? `, \`${dbName}\`.sources.name as 'from'` : ''}${options.q ? ', q' : ''} FROM \`${dbName}\`.${table}`;
        if (options.from) {
            subQuery += ` INNER JOIN \`${dbName}\`.sources ON \`${dbName}\`.sources.id=\`${dbName}\`.${table}._from`;
        }
        subWhere = '';
        if (options.index !== null) {
            subWhere += ` \`${dbName}\`.${table}.id=${options.index}`;
        }
        if (options.ignoreNull) {
            // subWhere += (subWhere ? " AND" : "") + " val <> NULL";
        }
        subWhere += `${subWhere ? ' AND' : ''} \`${dbName}\`.${table}.ts >= ${options.end}`;
        if (subWhere) {
            subQuery += ` WHERE ${subWhere}`;
        }
        subQuery += ` ORDER BY \`${dbName}\`.${table}.ts ASC LIMIT 1`;
        where += ` UNION ALL (${subQuery})`;
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
function deleteFromTable(dbName, table, index, start, end) {
    let query = `DELETE FROM \`${dbName}\`.${table} WHERE`;
    query += ` id=${index}`;
    if (start && end) {
        query += ` AND ts>=${start} AND ts<=${end}`;
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
 * @param dbName name of the database
 * @param index the integer key of the datapoint
 */
function deleteDatapoint(dbName, index) {
    return `DELETE FROM \`${dbName}\`.datapoints WHERE id=${index};`;
}
function update(dbName, index, state, from, table) {
    if (!state || state.val === null || state.val === undefined) {
        state.val = 'NULL';
    }
    else if (table === 'ts_string') {
        state.val = `'${state.val.toString().replace(/'/g, '')}'`;
    }
    let query = `UPDATE \`${dbName}\`.${table} SET `;
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
function rawEntriesWhere(dbName, table, index, options) {
    let where = `\`${dbName}\`.${table}.id=${index}`;
    if (options.start) {
        where += ` AND \`${dbName}\`.${table}.ts>=${options.start}`;
    }
    if (options.end) {
        where += ` AND \`${dbName}\`.${table}.ts<=${options.end}`;
    }
    return where;
}
function getRawEntries(dbName, table, index, options) {
    return (`SELECT \`${dbName}\`.${table}.ts, \`${dbName}\`.${table}.val, \`${dbName}\`.${table}.ack, \`${dbName}\`.${table}.q, \`${dbName}\`.sources.name AS 'from' FROM \`${dbName}\`.${table}` +
        ` LEFT JOIN \`${dbName}\`.sources ON \`${dbName}\`.sources.id=\`${dbName}\`.${table}._from` +
        ` WHERE ${rawEntriesWhere(dbName, table, index, options)}` +
        ` ORDER BY \`${dbName}\`.${table}.ts ${options.sort === 'asc' ? 'ASC' : 'DESC'}` +
        ` LIMIT ${options.limit} OFFSET ${options.offset};`);
}
function getRawEntriesCount(dbName, table, index, options) {
    return `SELECT COUNT(*) AS total FROM \`${dbName}\`.${table} WHERE ${rawEntriesWhere(dbName, table, index, options)};`;
}
//# sourceMappingURL=mysql.js.map