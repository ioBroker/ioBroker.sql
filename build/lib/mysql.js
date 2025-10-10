"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.init = init;
exports.destroy = destroy;
exports.getFirstTs = getFirstTs;
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
exports.update = update;
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
            while (insertValues[table].length) {
                query.push(`INSERT INTO \`${dbName}\`.ts_counter (id, ts, val) VALUES ${insertValues[table].splice(0, 500).join(',')};`);
            }
        }
        else {
            while (insertValues[table].length) {
                query.push(`INSERT INTO \`${dbName}\`.${table} (id, ts, val, ack, _from, q) VALUES ${insertValues[table].splice(0, 500).join(',')};`);
            }
        }
    }
    return query.join(' ');
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
    // get values from counters where counter changed from up to down (e.g. counter changed)
    const subQueryCounterChanges = `SELECT ts, val FROM \`${dbName}\`.ts_counter WHERE id=${options.index} AND ts>${options.start} AND ts<${options.end} AND val IS NOT NULL ORDER BY ts ASC`;
    return (`SELECT DISTINCT(a.ts), a.val from ((${subQueryFirst})\n` +
        `UNION ALL \n(${subQueryStart})\n` +
        `UNION ALL \n(${subQueryEnd})\n` +
        `UNION ALL \n(${subQueryLast})\n` +
        `UNION ALL \n(${subQueryCounterChanges})\n` +
        `ORDER BY ts) a;`);
}
function getHistory(dbName, table, options) {
    let query = `SELECT ts, val${options.index !== null ? `, ${table}.id as id` : ''}${options.ack ? ', ack' : ''}${options.from ? `, \`${dbName}\`.sources.name as 'from'` : ''}${options.q ? ', q' : ''} FROM \`${dbName}\`.${table}`;
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
        subQuery = ` SELECT ts, val${options.index !== null ? `, ${table}.id as id` : ''}${options.ack ? ', ack' : ''}${options.from ? `, \`${dbName}\`.sources.name as 'from'` : ''}${options.q ? ', q' : ''} FROM \`${dbName}\`.${table}`;
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
        subQuery = ` SELECT ts, val${options.index !== null ? `, ${table}.id as id` : ''}${options.ack ? ', ack' : ''}${options.from ? `, \`${dbName}\`.sources.name as 'from'` : ''}${options.q ? ', q' : ''} FROM \`${dbName}\`.${table}`;
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
//# sourceMappingURL=mysql.js.map