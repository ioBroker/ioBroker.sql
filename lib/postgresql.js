exports.init = function (dbname) {
    return [
        "CREATE TABLE sources    (id SERIAL NOT NULL PRIMARY KEY, name TEXT);",
        "CREATE TABLE datapoints (id SERIAL NOT NULL PRIMARY KEY, name TEXT, type INTEGER);",
        "CREATE TABLE ts_number  (id INTEGER NOT NULL, ts BIGINT, val REAL,    ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));",
        "CREATE TABLE ts_string  (id INTEGER NOT NULL, ts BIGINT, val TEXT,    ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));",
        "CREATE TABLE ts_bool    (id INTEGER NOT NULL, ts BIGINT, val BOOLEAN, ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));",
        "CREATE TABLE ts_counter (id INTEGER NOT NULL, ts BIGINT, val REAL);"
    ];
};

exports.destroy = function (dbname) {
    return [
        "DROP TABLE ts_counter;",
        "DROP TABLE ts_number;",
        "DROP TABLE ts_string;",
        "DROP TABLE ts_bool;",
        "DROP TABLE sources;",
        "DROP TABLE datapoints;"
    ];
};

exports.getFirstTs = function (dbname, db) {
    return "SELECT id, MIN(ts) AS ts FROM " + db + " GROUP BY id;";
};

exports.insert = function (dbname, index, state, from, db) {
    if (!state || state.val === null || state.val === undefined) {
        state.val = 'NULL';
    } else if (db === "ts_string") {
        state.val = "'" + state.val.toString().replace(/'/g, '') + "'";
    }

    if (db === 'ts_counter') {
        return "INSERT INTO ts_counter (id, ts, val) VALUES(" + index + ", " + state.ts + ", " + state.val + ");";
    } else {
        return "INSERT INTO " + db + " (id, ts, val, ack, _from, q) VALUES(" + index + ", " + state.ts + ", " + state.val + ", " + (!!state.ack) + ", " + (from || 0) + ", " + (state.q || 0) + ");";
    }
};

exports.retention = function (dbname, index, db, retention) {
    const d = new Date();
    d.setSeconds(-retention);
    let query = "DELETE FROM " + db + " WHERE";
    query += " id=" + index;
    query += " AND ts < " + d.getTime();
    query += ";";

    return query;
};

exports.getIdSelect = function (dbname, name) {
    if (!name) {
        return "SELECT id, type, name FROM datapoints;";
    } else {
        return "SELECT id, type, name FROM datapoints WHERE name='" + name + "';";
    }
};

exports.getIdInsert = function (dbname, name, type) {
    return  "INSERT INTO datapoints (name, type) VALUES('" + name + "', " + type + ");";
};

exports.getIdUpdate = function (dbname, id, type) {
    return  "UPDATE datapoints SET type = " + type + " WHERE id = " + id + ";";
};

exports.getFromSelect = function (dbname, from) {
    if (from) {
        return "SELECT id FROM sources WHERE name='" + from + "';";
    }
    else {
        return "SELECT id, name FROM sources;";
    }
};

exports.getFromInsert = function (dbname, from) {
    return "INSERT INTO sources (name) VALUES('" + from + "');";
};

exports.getCounterDiff = function (dbname, options) {
    // Take first real value after start
    const subQueryStart          = "SELECT ts, val FROM `" + dbname + "`.ts_number  WHERE id=" + options.id + " AND ts>="  + options.start + " AND ts<" + options.end + " AND val IS NOT NULL ORDER BY ts ASC LIMIT 1";
    // Take last real value before end
    const subQueryEnd            = "SELECT ts, val FROM `" + dbname + "`.ts_number  WHERE id=" + options.id + " AND ts>="  + options.start + " AND ts<" + options.end + " AND val IS NOT NULL ORDER BY ts DESC LIMIT 1";
    // Take last value before start
    const subQueryFirst          = "SELECT ts, val FROM `" + dbname + "`.ts_number  WHERE id=" + options.id + " AND ts< "  + options.start + " ORDER BY ts DESC LIMIT 1";
    // Take next value after end
    const subQueryLast           = "SELECT ts, val FROM `" + dbname + "`.ts_number  WHERE id=" + options.id + " AND ts>= " + options.end   + " ORDER BY ts ASC  LIMIT 1";
    // get values from counters where counter changed from up to down (e.g. counter changed)
    const subQueryCounterChanges = "SELECT ts, val FROM `" + dbname + "`.ts_counter WHERE id=" + options.id + " AND ts>"  + options.start + " AND ts<" + options.end + " AND val IS NOT NULL ORDER BY ts ASC";

    return "SELECT DISTINCT(a.ts), a.val from ((" + subQueryFirst + ")\n" +
        "UNION ALL \n(" + subQueryStart + ")\n" +
        "UNION ALL \n(" + subQueryEnd + ")\n" +
        "UNION ALL \n(" + subQueryLast + ")\n" +
        "UNION ALL \n(" + subQueryCounterChanges + ")\n" +
        "ORDER BY ts) a;";
};

exports.getHistory = function (dbname, db, options) {
    let query = "SELECT ts, val" +
        (!options.id  ? (", " + db + ".id as id") : "") +
        (options.ack  ? ", ack" : "") +
        (options.from ? (", sources.name as from") : "") +
        (options.q    ? ", q" : "") + " FROM " + db;

    if (options.from) {
        query += " INNER JOIN sources ON sources.id=" + db + "._from";
    }

    let where = "";

    if (options.id) {
        where += " " + db + ".id=" + options.id;
    }
    if (options.end) {
        where += (where ? " AND" : "") + " " + db + ".ts < " + options.end;
    }
    if (options.start) {
        where += (where ? " AND" : "") + " " + db + ".ts >= " + options.start;

        //add last value before start
        let subQuery;
        let subWhere;
        subQuery = " SELECT ts, val" +
            (!options.id  ? (", " + db + ".id as id") : "") +
            (options.ack  ? ", ack" : "") +
            (options.from ? (", sources.name as from") : "") +
            (options.q    ? ", q" : "") + " FROM " + db;
        if (options.from) {
            subQuery += " INNER JOIN sources ON sources.id=" + db + "._from";
        }
        subWhere = "";
        if (options.id) {
            subWhere += " " + db + ".id=" + options.id;
        }
        if (options.ignoreNull) {
            //subWhere += (subWhere ? " AND" : "") + " val <> NULL";
        }
        subWhere += (subWhere ? " AND" : "") + " " + db + ".ts < " + options.start;
        if (subWhere) subQuery += " WHERE " + subWhere;
        subQuery += " ORDER BY " + db + ".ts DESC LIMIT 1";
        where += " UNION (" + subQuery + ")";

        //add next value after end
        subQuery = " SELECT ts, val" +
            (!options.id  ? (", " + db + ".id as id") : "") +
            (options.ack  ? ", ack" : "") +
            (options.from ? (", sources.name as from") : "") +
            (options.q    ? ", q" : "") + " FROM " + db;
        if (options.from) {
            subQuery += " INNER JOIN sources ON sources.id=" + db + "._from";
        }
        subWhere = "";
        if (options.id) {
            subWhere += " " + db + ".id=" + options.id;
        }
        if (options.ignoreNull) {
            //subWhere += (subWhere ? " AND" : "") + " val <> NULL";
        }
        subWhere += (subWhere ? " AND" : "") + " " + db + ".ts >= " + options.end;
        if (subWhere) subQuery += " WHERE " + subWhere;
        subQuery += " ORDER BY " + db + ".ts ASC LIMIT 1";
        where += " UNION (" + subQuery + ")";
    }

    if (where) {
        query += " WHERE " + where;
    }

    query += " ORDER BY ts";

    if ((!options.start && options.count) || (options.aggregate === 'none' && options.count && options.returnNewestEntries) ) {
        query += " DESC";
    } else {
        query += " ASC";
    }

    if ((!options.start && options.count) || (options.aggregate === 'none' && options.count)) {
        query += ` LIMIT ` + (options.count + 2);
    }

    query += ";";
    return query;
};

exports.delete = function (dbname, db, index, start, end) {
    let query = "DELETE FROM " + db + " WHERE";
    query += " id=" + index;

    if (start && end) {
        query += " AND ts>=" + start + " AND ts <= " + end;
    } else if (start) {
        query += " AND ts=" + start;
    }

    query += ";";

    return query;
};

exports.update = function (dbname, index, state, from, db) {
    if (!state || state.val === null || state.val === undefined) {
        state.val = "NULL";
    } else if (db === "ts_string") {
        state.val = "'" + state.val.toString().replace(/'/g, '') + "'";
    }

    let query = "UPDATE " + db + " SET ";
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
        vals.push("ack=" + (!!state.ack));
    }
    query += vals.join(', ');
    query += " WHERE ";
    query += " id=" + index;
    query += " AND ts=" + state.ts;
    query += ";";

    return query;
};
