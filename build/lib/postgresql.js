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
function init(_dbName, _doNotCreateDatabase) {
    return [
        'CREATE TABLE sources    (id SERIAL NOT NULL PRIMARY KEY, name TEXT);',
        'CREATE TABLE datapoints (id SERIAL NOT NULL PRIMARY KEY, name TEXT, type INTEGER);',
        'CREATE TABLE ts_number  (id INTEGER NOT NULL, ts BIGINT, val REAL,    ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));',
        'CREATE TABLE ts_string  (id INTEGER NOT NULL, ts BIGINT, val TEXT,    ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));',
        'CREATE TABLE ts_bool    (id INTEGER NOT NULL, ts BIGINT, val BOOLEAN, ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));',
        'CREATE TABLE ts_counter (id INTEGER NOT NULL, ts BIGINT, val REAL);',
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
function insert(_dbName, index, values) {
    const insertValues = {};
    values.forEach(value => {
        // state, from, table
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
            insertValues[value.table].push(`(${index}, ${value.state.ts}, ${value.state.val}, ${!!value.state.ack}, ${value.from || 0}, ${value.state.q || 0})`);
        }
    });
    const query = [];
    for (const table in insertValues) {
        if (table === 'ts_counter') {
            while (insertValues[table].length) {
                query.push(`INSERT INTO ts_counter (id, ts, val) VALUES ${insertValues[table].splice(0, 500).join(',')};`);
            }
        }
        else {
            while (insertValues[table].length) {
                query.push(`INSERT INTO ${table} (id, ts, val, ack, _from, q) VALUES ${insertValues[table].splice(0, 500).join(',')};`);
            }
        }
    }
    return query.join(' ');
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
    if (name) {
        return `SELECT id FROM sources WHERE name='${name}';`;
    }
    return 'SELECT id, name FROM sources;';
}
function getFromInsert(dbName, values) {
    return `INSERT INTO sources (name) VALUES('${values}');`;
}
function getCounterDiff(dbName, options) {
    // Take first real value after start
    const subQueryStart = `SELECT ts, val FROM \`${dbName}\`.ts_number  WHERE id=${options.index} AND ts>=${options.start} AND ts<${options.end} AND val IS NOT NULL ORDER BY ts ASC LIMIT 1`;
    // Take last real value before the end
    const subQueryEnd = `SELECT ts, val FROM \`${dbName}\`.ts_number  WHERE id=${options.index} AND ts>=${options.start} AND ts<${options.end} AND val IS NOT NULL ORDER BY ts DESC LIMIT 1`;
    // Take last value before start
    const subQueryFirst = `SELECT ts, val FROM \`${dbName}\`.ts_number  WHERE id=${options.index} AND ts< ${options.start} ORDER BY ts DESC LIMIT 1`;
    // Take next value after end
    const subQueryLast = `SELECT ts, val FROM \`${dbName}\`.ts_number  WHERE id=${options.index} AND ts>= ${options.end} ORDER BY ts ASC  LIMIT 1`;
    // get values from counters where counter changed from up to down (e.g. counter changed)
    const subQueryCounterChanges = `SELECT ts, val FROM \`${dbName}\`.ts_counter WHERE id=${options.index} AND ts>${options.start} AND ts<${options.end} AND val IS NOT NULL ORDER BY ts ASC`;
    return (`SELECT DISTINCT(a.ts), a.val from ((${subQueryFirst})\n` +
        `UNION ALL \n(${subQueryStart})\n` +
        `UNION ALL \n(${subQueryEnd})\n` +
        `UNION ALL \n(${subQueryLast})\n` +
        `UNION ALL \n(${subQueryCounterChanges})\n` +
        `ORDER BY ts) a;`);
}
function getHistory(_dbName, table, options) {
    let query = `SELECT ts, val${options.index !== null ? `, ${table}.id as id` : ''}${options.ack ? ', ack' : ''}${options.from ? ', sources.name as from' : ''}${options.q ? ', q' : ''} FROM ${table}`;
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
        subQuery = ` SELECT ts, val${options.index !== null ? `, ${table}.id as id` : ''}${options.ack ? ', ack' : ''}${options.from ? ', sources.name as from' : ''}${options.q ? ', q' : ''} FROM ${table}`;
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
        subQuery = ` SELECT ts, val${options.index !== null ? `, ${table}.id as id` : ''}${options.ack ? ', ack' : ''}${options.from ? ', sources.name as from' : ''}${options.q ? ', q' : ''} FROM ${table}`;
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
        vals.push(`ack=${!!state.ack}`);
    }
    query += vals.join(', ');
    query += ' WHERE ';
    query += ` id=${index}`;
    query += ` AND ts=${state.ts}`;
    query += ';';
    return query;
}
//# sourceMappingURL=postgresql.js.map