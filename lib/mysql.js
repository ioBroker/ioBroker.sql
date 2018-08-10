exports.init = function (dbname) {
    return [
        "CREATE DATABASE `" + dbname + "` DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;",
        "CREATE TABLE `" + dbname + "`.sources    (id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT, name TEXT);",
        "CREATE TABLE `" + dbname + "`.datapoints (id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT, name TEXT, type INTEGER);",
        "CREATE TABLE `" + dbname + "`.ts_number  (id INTEGER, ts BIGINT, val REAL,    ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));",
        "CREATE TABLE `" + dbname + "`.ts_string  (id INTEGER, ts BIGINT, val TEXT,    ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));",
        "CREATE TABLE `" + dbname + "`.ts_bool    (id INTEGER, ts BIGINT, val BOOLEAN, ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));"
    ];
};

exports.destroy = function (dbname) {
    return [
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

exports.insert = function (dbname, index, state, from, db) {
    if (state.val === null) state.val = 'NULL';
        else if (db === "ts_string") state.val = "'" + state.val.toString().replace(/'/g, '') + "'";
    return "INSERT INTO `" + dbname + "`." + db + " (id, ts, val, ack, _from, q) VALUES(" + index + ", " + state.ts + ", " + state.val + ", " + (state.ack ? 1 : 0) + ", " + (from || 0) + ", " + (state.q || 0) + ");";
};

exports.retention = function (dbname, index, db, retention) {
    var d = new Date();
    d.setSeconds(-retention);
    var query = "DELETE FROM `" + dbname + "`." + db + " WHERE";
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
    return  "UPDATE `" + dbname + "`.datapoints SET type = " + type + " WHERE id = " + id + ";";
};

exports.getFromSelect = function (dbname, from) {
    if (from) {
        return "SELECT id FROM `" + dbname + "`.sources WHERE name='" + from + "';";
    }
    else {
        return "SELECT id, name FROM `" + dbname + "`.sources;";
    }
};

exports.getFromInsert = function (dbname, from) {
    return "INSERT INTO `" + dbname + "`.sources (name) VALUES('" + from + "');";
};

exports.getHistory = function (dbname, db, options) {
    var query = "SELECT ts, val" +
        (!options.id  ? (", " + db + ".id as id") : "") +
        (options.ack  ? ", ack" : "") +
        (options.from ? (", `" + dbname + "`.sources.name as 'from'") : "") +
        (options.q    ? ", q" : "") + " FROM `" + dbname + "`." + db;

    if (options.from) {
        query += " INNER JOIN `" + dbname + "`.sources ON `" + dbname + "`.sources.id=`" + dbname + "`." + db + "._from";
    }

    var where = "";

    if (options.id) {
        where += " `" + dbname + "`." + db + ".id=" + options.id;
    }
    if (options.end) {
        where += (where ? " AND" : "") + " `" + dbname + "`." + db + ".ts < " + options.end;
    }
    if (options.start) {
        where += (where ? " AND" : "") + " `" + dbname + "`." + db + ".ts >= " + options.start;

        var subQuery;
        var subWhere;
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
        if (subWhere) subQuery += " WHERE " + subWhere;
        subQuery += " ORDER BY `" + dbname + "`." + db + ".ts ASC LIMIT 1";
        where += " UNION (" + subQuery + ")";

    }

    if (where) query += " WHERE " + where;

    query += " ORDER BY ts";

    if (!options.start && options.count) {
        query += " DESC LIMIT " + options.count;
    } else {
        query += " ASC";
    }

    query += ";";
    return query;
};
