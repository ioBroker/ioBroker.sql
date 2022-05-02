exports.init = function (dbname, doNotCreateDatabase) {
    const commands = [
        "CREATE TABLE `" + dbname + "`.sources    (id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT, name TEXT);",
        "CREATE TABLE `" + dbname + "`.datapoints (id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT, name TEXT, type INTEGER);",
        "CREATE TABLE `" + dbname + "`.ts_number  (id INTEGER, ts BIGINT, val REAL,    ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));",
        "CREATE TABLE `" + dbname + "`.ts_string  (id INTEGER, ts BIGINT, val TEXT,    ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));",
        "CREATE TABLE `" + dbname + "`.ts_bool    (id INTEGER, ts BIGINT, val BOOLEAN, ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));",
        "CREATE TABLE `" + dbname + "`.ts_counter (id INTEGER, ts BIGINT, val REAL);"
    ];

    !doNotCreateDatabase && commands.unshift("CREATE DATABASE `" + dbname + "` DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;");

    return commands;
};

exports.destroy = function (dbname) {
    return [
        "DROP TABLE `" + dbname + "`.ts_counter;",
        "DROP TABLE `" + dbname + "`.ts_number;",
        "DROP TABLE `" + dbname + "`.ts_string;",
        "DROP TABLE `" + dbname + "`.ts_bool;",
        "DROP TABLE `" + dbname + "`.sources;",
        "DROP TABLE `" + dbname + "`.datapoints;",
        "DROP DATABASE `" + dbname + "`;"
    ];
};

exports.getFirstTs = function (dbname, db) {
    return "SELECT id, MIN(ts) AS ts FROM `" + dbname + "`." + db + " GROUP BY id;";
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
                query += "INSERT INTO `" + dbname + "`.ts_counter (id, ts, val) VALUES " + insertValues[db].splice(0, 500).join(',') + ";";
            }
        } else {
            while (insertValues[db].length) {
                query += "INSERT INTO `" + dbname + "`." + db + " (id, ts, val, ack, _from, q) VALUES " + insertValues[db].splice(0, 500).join(',') + ";";
            }
        }
    }

    return query;
}

exports.retention = function (dbname, index, db, retention) {
    const  d = new Date();
    d.setSeconds(-retention);
    let query = "DELETE FROM `" + dbname + "`." + db + " WHERE";
    query += " id=" + index;
    query += " AND ts < " + d.getTime();
    query += ";";

    return query;
};

exports.getIdSelect = function (dbname, name) {
    if (!name) {
        return "SELECT id, type, name FROM `" + dbname + "`.datapoints;";
    } else {
        return "SELECT id, type, name FROM `" + dbname + "`.datapoints WHERE name='" + name + "';";
    }
};

exports.getIdInsert = function (dbname, name, type) {
    return  "INSERT INTO `" + dbname + "`.datapoints (name, type) VALUES('" + name + "', " + type + ");";
};

exports.getIdUpdate = function (dbname, id, type) {
    return  "UPDATE `" + dbname + "`.datapoints SET type=" + type + " WHERE id=" + id + ";";
};

exports.getFromSelect = function (dbname, from) {
    if (from) {
        return "SELECT id FROM `" + dbname + "`.sources WHERE name='" + from + "';";
    } else {
        return "SELECT id, name FROM `" + dbname + "`.sources;";
    }
};

exports.getFromInsert = function (dbname, from) {
    return "INSERT INTO `" + dbname + "`.sources (name) VALUES('" + from + "');";
};

exports.getCounterDiff = function (dbname, options) {
    // Take first real value after start
    const subQueryStart          = "SELECT ts, val FROM `" + dbname + "`.ts_number  WHERE id=" + options.id + " AND ts>="  + options.start + " AND ts<" + options.end + " AND val IS NOT NULL ORDER BY ts ASC  LIMIT 1";
    // Take last real value before end
    const subQueryEnd            = "SELECT ts, val FROM `" + dbname + "`.ts_number  WHERE id=" + options.id + " AND ts>="  + options.start + " AND ts<" + options.end + " AND val IS NOT NULL ORDER BY ts DESC LIMIT 1";
    // Take last value before start
    const subQueryFirst          = "SELECT ts, val FROM `" + dbname + "`.ts_number  WHERE id=" + options.id + " AND ts< "  + options.start + " ORDER BY ts DESC                                        LIMIT 1";
    // Take next value after end
    const subQueryLast           = "SELECT ts, val FROM `" + dbname + "`.ts_number  WHERE id=" + options.id + " AND ts>= " + options.end   + " ORDER BY ts ASC                                         LIMIT 1";
    // get values from counters where counter changed from up to down (e.g. counter changed)
    const subQueryCounterChanges = "SELECT ts, val FROM `" + dbname + "`.ts_counter WHERE id=" + options.id + " AND ts>"   + options.start + " AND ts<" + options.end + " AND val IS NOT NULL ORDER BY ts ASC";

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
        (options.from ? (", `" + dbname + "`.sources.name as 'from'") : "") +
        (options.q    ? ", q" : "") + " FROM `" + dbname + "`." + db;

    if (options.from) {
        query += " INNER JOIN `" + dbname + "`.sources ON `" + dbname + "`.sources.id=`" + dbname + "`." + db + "._from";
    }

    let  where = "";

    if (options.id) {
        where += " `" + dbname + "`." + db + ".id=" + options.id;
    }
    if (options.end) {
        where += (where ? " AND" : "") + " `" + dbname + "`." + db + ".ts < " + options.end;
    }
    if (options.start) {
        where += (where ? " AND" : "") + " `" + dbname + "`." + db + ".ts >= " + options.start;

        let  subQuery;
        let  subWhere;
        subQuery = " SELECT ts, val" +
            (!options.id  ? (", " + db + ".id as id") : "") +
            (options.ack  ? ", ack" : "") +
            (options.from ? (", `" + dbname + "`.sources.name as 'from'") : "") +
            (options.q    ? ", q" : "") + " FROM `" + dbname + "`." + db;
        if (options.from) {
            subQuery += " INNER JOIN `" + dbname + "`.sources ON `" + dbname + "`.sources.id=`" + dbname + "`." + db + "._from";
        }
        subWhere = "";
        if (options.id) {
            subWhere += " `" + dbname + "`." + db + ".id=" + options.id;
        }
        if (options.ignoreNull) {
            //subWhere += (subWhere ? " AND" : "") + " val <> NULL";
        }
        subWhere += (subWhere ? " AND" : "") + " `" + dbname + "`." + db + ".ts < " + options.start;
        if (subWhere) subQuery += " WHERE " + subWhere;
        subQuery += " ORDER BY `" + dbname + "`." + db + ".ts DESC LIMIT 1";
        where += " UNION (" + subQuery + ")";

        //add next value after end
        subQuery = " SELECT ts, val" +
            (!options.id  ? (", " + db + ".id as id") : "") +
            (options.ack  ? ", ack" : "") +
            (options.from ? (", `" + dbname + "`.sources.name as 'from'") : "") +
            (options.q    ? ", q" : "") + " FROM `" + dbname + "`." + db;
        if (options.from) {
            subQuery += " INNER JOIN `" + dbname + "`.sources ON `" + dbname + "`.sources.id=`" + dbname + "`." + db + "._from";
        }
        subWhere = "";
        if (options.id) {
            subWhere += " `" + dbname + "`." + db + ".id=" + options.id;
        }
        if (options.ignoreNull) {
            //subWhere += (subWhere ? " AND" : "") + " val <> NULL";
        }
        subWhere += (subWhere ? " AND" : "") + " `" + dbname + "`." + db + ".ts >= " + options.end;
        if (subWhere) {
            subQuery += " WHERE " + subWhere;
        }
        subQuery += " ORDER BY `" + dbname + "`." + db + ".ts ASC LIMIT 1";
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
    let query = "DELETE FROM `" + dbname + "`." + db + " WHERE";
    query += " id=" + index;

    if (start && end) {
        query += " AND ts>=" + start + " AND ts<=" + end;
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

    let query = "UPDATE `" + dbname + "`." + db + " SET ";
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
