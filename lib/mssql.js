exports.init = function (dbname, doNotCreateDatabase) {
    const commands = [
        `CREATE TABLE ${dbname}.dbo.sources     (id INTEGER NOT NULL PRIMARY KEY IDENTITY(1,1), name varchar(255));`,
        `CREATE TABLE ${dbname}.dbo.datapoints  (id INTEGER NOT NULL PRIMARY KEY IDENTITY(1,1), name varchar(255), type INTEGER);`,
        `CREATE TABLE ${dbname}.dbo.ts_number   (id INTEGER, ts BIGINT, val REAL, ack BIT, _from INTEGER, q INTEGER);`,
        `CREATE INDEX i_id on ${dbname}.dbo.ts_number (id, ts);`,
        `CREATE TABLE ${dbname}.dbo.ts_string   (id INTEGER, ts BIGINT, val TEXT, ack BIT, _from INTEGER, q INTEGER);`,
        `CREATE INDEX i_id on ${dbname}.dbo.ts_string (id, ts);`,
        `CREATE TABLE ${dbname}.dbo.ts_bool     (id INTEGER, ts BIGINT, val BIT,  ack BIT, _from INTEGER, q INTEGER);`,
        `CREATE INDEX i_id on ${dbname}.dbo.ts_bool (id, ts);`,
        `CREATE TABLE ${dbname}.dbo.ts_counter  (id INTEGER, ts BIGINT, val REAL);`,
        `CREATE INDEX i_id on ${dbname}.dbo.ts_counter (id, ts);`,
    ];
    !doNotCreateDatabase && commands.unshift(`CREATE DATABASE ${dbname};`);

    return commands;
};

exports.destroy = function (dbname) {
    return [
        `DROP TABLE ${dbname}.dbo.ts_counter;`,
        `DROP TABLE ${dbname}.dbo.ts_number;`,
        `DROP TABLE ${dbname}.dbo.ts_string;`,
        `DROP TABLE ${dbname}.dbo.ts_bool;`,
        `DROP TABLE ${dbname}.dbo.sources;`,
        `DROP TABLE ${dbname}.dbo.datapoints;`,
        `DROP DATABASE ${dbname};`,
        `DBCC FREEPROCCACHE;`
    ];
};

exports.getFirstTs = function (dbname, db) {
    return `SELECT id, MIN(ts) AS ts FROM ${dbname}.dbo.${db} GROUP BY id;`;
};

exports.insert = function (dbname, index, values) {
    const insertValues = {}
    values.forEach(value => {
        // state, from, db
        insertValues[value.db] = insertValues[value.db] || [];

        if (!value.state || value.state.val === null || value.state.val === undefined) {
            value.state.val = "NULL";
        } else if (value.db === "ts_string") {
            value.state.val = "'" + value.state.val.toString().replace(/'/g, '') + "'";
        }

        if (value.db === 'ts_counter') {
            insertValues[value.db].push(`(${index}, ${value.state.ts}, ${value.state.val})`);
        } else {
            insertValues[value.db].push(`(${index}, ${value.state.ts}, ${value.state.val}, ${(value.state.ack ? 1 : 0)}, ${value.from || 0}, ${value.state.q || 0})`);
        }
    });

    let query = '';
    for (let db in insertValues) {
        if (db === 'ts_counter') {
            while (insertValues[db].length) {
                query += `INSERT INTO ${dbname}.dbo.ts_counter (id, ts, val) VALUES ${insertValues[db].splice(0, 500).join(',')};`;
            }
        } else {
            while (insertValues[db].length) {
                query += `INSERT INTO ${dbname}.dbo.${db} (id, ts, val, ack, _from, q) VALUES ${insertValues[db].splice(0, 500).join(',')};`;
            }
        }
    }

    return query;
}

exports.retention = function (dbname, index, db, retention) {
    const d = new Date();
    d.setSeconds(-retention);
    let query = `DELETE FROM ${dbname}.dbo.${db} WHERE`;
    query += ` id=` + index;
    query += ` AND ts < ` + d.getTime();
    query += `;`;
    return query;
};

exports.getIdSelect = function (dbname, name) {
    if (!name) {
        return `SELECT id, type, name FROM ${dbname}.dbo.datapoints;`;
    } else {
        return `SELECT id, type, name FROM ${dbname}.dbo.datapoints WHERE name='${name}';`;
    }
};

exports.getIdInsert = function (dbname, name, type) {
    return  `INSERT INTO ${dbname}.dbo.datapoints (name, type) VALUES('${name}', ${type});`;
};

exports.getIdUpdate = function (dbname, id, type) {
    return  `UPDATE ${dbname}.dbo.datapoints SET type=${type} WHERE id=${id};`;
};

exports.getFromSelect = function (dbname, from) {
    if (from) {
        return `SELECT id FROM ${dbname}.dbo.sources WHERE name='${from}';`;
    } else {
        return `SELECT id, name FROM ${dbname}.dbo.sources;`;

    }
};

exports.getFromInsert = function (dbname, from) {
    return `INSERT INTO ${dbname}.dbo.sources (name) VALUES('${from}');`;
};

exports.getCounterDiff = function (dbname, options) {
    // Take first real value after start
    const subQueryStart =          `SELECT TOP 1 val, ts FROM ${dbname}.dbo.ts_number  WHERE ${dbname}.dbo.ts_number.id=${options.id} AND ${dbname}.dbo.ts_number.ts>=${options.start} AND ${dbname}.dbo.ts_number.ts<${options.end} AND ${dbname}.dbo.ts_number.val IS NOT NULL ORDER BY ${dbname}.dbo.ts_number.ts ASC`;
    // Take last real value before end
    const subQueryEnd   =          `SELECT TOP 1 val, ts FROM ${dbname}.dbo.ts_number  WHERE ${dbname}.dbo.ts_number.id=${options.id} AND ${dbname}.dbo.ts_number.ts>=${options.start} AND ${dbname}.dbo.ts_number.ts<${options.end} AND ${dbname}.dbo.ts_number.val IS NOT NULL ORDER BY ${dbname}.dbo.ts_number.ts DESC`;
    // Take last value before start
    const subQueryFirst =          `SELECT TOP 1 val, ts FROM ${dbname}.dbo.ts_number  WHERE ${dbname}.dbo.ts_number.id=${options.id} AND ${dbname}.dbo.ts_number.ts< ${options.start} ORDER BY ${dbname}.dbo.ts_number.ts DESC`;
    // Take next value after end
    const subQueryLast =           `SELECT TOP 1 val, ts FROM ${dbname}.dbo.ts_number  WHERE ${dbname}.dbo.ts_number.id=${options.id} AND ${dbname}.dbo.ts_number.ts>=${options.end}   ORDER BY ${dbname}.dbo.ts_number.ts ASC`;
    // get values from counters where counter values changed
    const subQueryCounterChanges = `SELECT       val, ts FROM ${dbname}.dbo.ts_counter WHERE ${dbname}.dbo.ts_number.id=${options.id} AND ${dbname}.dbo.ts_number.ts>${options.start} AND ${dbname}.dbo.ts_number.ts<${options.end} AND ${dbname}.dbo.ts_number.val IS NOT NULL ORDER BY ${dbname}.dbo.ts_number.ts ASC`;

    return `${subQueryFirst} ` +
        `UNION ALL (${subQueryStart}) a ` +
        `UNION ALL (${subQueryEnd}) b ` +
        `UNION ALL (${subQueryLast}) c` +
        `UNION ALL (${subQueryCounterChanges}) d`;
};

exports.getHistory = function (dbname, db, options) {
    let query = `SELECT * FROM (SELECT `;
    if ((!options.start && options.count) || (options.aggregate === 'none' && options.count)) {
        query += ` TOP ` + options.count;
    }
    query += ` ts, val` +
        (!options.id  ? (`, ${db}.id as id`) : '') +
        (options.ack  ? `, ack` : '') +
        (options.from ? (`, ${dbname}.dbo.sources.name as 'from'`) : '') +
        (options.q    ? `, q` : '') + ` FROM ${dbname}.dbo.` + db;

    if (options.from) {
        query += ` INNER JOIN ${dbname}.dbo.sources ON ${dbname}.dbo.sources.id=${dbname}.dbo.${db}._from`;
    }

    let where = '';

    if (options.id) {
        where += ` ${dbname}.dbo.${db}.id=` + options.id;
    }
    if (options.end) {
        where += (where ? ` AND` : '') + ` ${dbname}.dbo.${db}.ts < ` + options.end;
    }
    if (options.start) {
        where += (where ? ` AND` : '') + ` ${dbname}.dbo.${db}.ts >= ` + options.start;
    }
    if ((!options.start && options.count) || (options.aggregate === 'none' && options.count)) {
        where += ` ORDER BY ts`;

        if ((!options.start && options.count) || (options.aggregate === 'none' && options.count && options.returnNewestEntries) ) {
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
        subQuery = ` SELECT TOP 1 ts, val` +
            (!options.id  ? (`, ${db}.id as id`) : '') +
            (options.ack  ? `, ack` : '') +
            (options.from ? (`, ${dbname}.dbo.sources.name as 'from'`) : '') +
            (options.q    ? `, q` : '') + ` FROM ${dbname}.dbo.` + db;
        if (options.from) {
            subQuery += ` INNER JOIN ${dbname}.dbo.sources ON ${dbname}.dbo.sources.id=${dbname}.dbo.${db}._from`;
        }
        subWhere = '';
        if (options.id) {
            subWhere += ` ${dbname}.dbo.${db}.id=` + options.id;
        }
        if (options.ignoreNull) {
            //subWhere += (subWhere ? ` AND` : '') + ` val <> NULL`;
        }
        subWhere += `${subWhere ? ` AND` : ''} ${dbname}.dbo.${db}.ts < ` + options.start;
        if (subWhere) subQuery += ` WHERE ` + subWhere;
        subQuery += ` ORDER BY ${dbname}.dbo.${db}.ts DESC`;
        where += ` UNION SELECT * FROM (${subQuery}) a`;

        // add next value after end
        subQuery = ` SELECT TOP 1 ts, val` +
            (!options.id  ? (`, ${db}.id as id`) : '') +
            (options.ack  ? `, ack` : '') +
            (options.from ? (`, ${dbname}.dbo.sources.name as 'from'`) : '') +
            (options.q    ? `, q` : '') + ` FROM ${dbname}.dbo.${db}`;
        if (options.from) {
            subQuery += ` INNER JOIN ${dbname}.dbo.sources ON ${dbname}.dbo.sources.id=${dbname}.dbo.${db}._from`;
        }
        subWhere = '';
        if (options.id) {
            subWhere += ` ${dbname}.dbo.${db}.id=${options.id}`;
        }
        if (options.ignoreNull) {
            //subWhere += (subWhere ? ` AND` : '') + ` val <> NULL`;
        }
        subWhere += (subWhere ? ` AND` : '') + ` ${dbname}.dbo.${db}.ts >= ${options.end}`;
        if (subWhere) subQuery += ` WHERE ${subWhere}`;
        subQuery += ` ORDER BY ${dbname}.dbo.${db}.ts ASC`;
        where += ` UNION SELECT * FROM (${subQuery}) b`;
    }

    if (where) query += ` WHERE ` + where;

    query += ` ORDER BY ts`;
    if ((!options.start && options.count) || (options.aggregate === 'none' && options.count && options.returnNewestEntries) ) {
        query += ` DESC`;
    } else {
        query += ` ASC`;
    }
    query += `;`;

    return query;
};

exports.delete = function (dbname, db, index, start, end) {
    let query = `DELETE FROM ${dbname}.dbo.${db} WHERE`;
    query += " id=" + index;
    if (start && end) {
        query += ` AND ${dbname}.dbo.${db}.ts>=${start} AND ${dbname}.dbo.${db}.ts<=${end}`;
    } else if (start) {
        query += ` AND ${dbname}.dbo.${db}.ts=${start}`;
    }

    query += ";";

    return query;
};

exports.update = function (dbname, index, state, from, db) {
    if (!state || state.val === null || state.val === undefined) {
        state.val = 'NULL';
    } else if (db === 'ts_bool') {
        state.val = state.val ? 1 : 0;
    }else if (db === 'ts_string') {
        state.val = `'${state.val.toString().replace(/'/g, '')}'`;
    }

    let query = `UPDATE ${dbname}.dbo.${db} SET `;
    let vals = [];
    if (state.val !== undefined) {
        vals.push("val=" + state.val);
    }
    if (state.q !== undefined) {
        vals.push("q=" + state.q);
    }
    if (from !== undefined) {
        vals.push("_from=" + from);
    }
    if (state.ack !== undefined) {
        vals.push("ack=" + (state.ack ? 1 : 0));
    }
    query += vals.join(', ');
    query += " WHERE ";
    query += " id=" + index;
    query += " AND ts=" + state.ts;
    query += ";";

    return query;
};
