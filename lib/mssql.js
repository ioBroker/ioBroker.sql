exports.init = function (dbname) {
    return [
        "CREATE DATABASE " + dbname + ";",
        "CREATE TABLE " + dbname + ".dbo.sources    (id INTEGER NOT NULL PRIMARY KEY IDENTITY(1,1), name varchar(255));",
        "CREATE TABLE " + dbname + ".dbo.datapoints (id INTEGER NOT NULL PRIMARY KEY IDENTITY(1,1), name varchar(255), type INTEGER);",
        "CREATE TABLE " + dbname + ".dbo.ts_number  (id INTEGER, ts BIGINT, val REAL, ack BIT, _from INTEGER, q INTEGER);",
        "CREATE TABLE " + dbname + ".dbo.ts_string  (id INTEGER, ts BIGINT, val TEXT, ack BIT, _from INTEGER, q INTEGER);",
        "CREATE TABLE " + dbname + ".dbo.ts_bool    (id INTEGER, ts BIGINT, val BIT,  ack BIT, _from INTEGER, q INTEGER);"
    ];
};

exports.destroy = function (dbname) {
    return [
        "DROP TABLE " + dbname + ".dbo.ts_number;",
        "DROP TABLE " + dbname + ".dbo.ts_string;",
        "DROP TABLE " + dbname + ".dbo.ts_bool;",
        "DROP TABLE " + dbname + ".dbo.sources;",
        "DROP TABLE " + dbname + ".dbo.datapoints;",
        "DROP DATABASE " + dbname + ";",
        "DBCC FREEPROCCACHE;"
    ];
};

exports.getFirstTs = function (dbname, db) {
    return "SELECT id, MIN(ts) AS ts FROM " + dbname + ".dbo." + db + " GROUP BY id;";
};

exports.insert = function (dbname, index, state, from, db) {
    if (db === "ts_bool")   state.val = state.val ? 1 : 0;
    if (db === "ts_string") state.val = "'" + state.val.toString().replace(/'/g, '') + "'";
    return "INSERT INTO " + dbname + ".dbo." + db + " (id, ts, val, ack, _from, q) VALUES(" + index + ", " + state.ts + ", " + state.val + ", " + (state.ack ? 1 : 0) + ", " + (from || 0) + ", " + state.q + ");";
};

exports.retention = function (dbname, index, db, retention) {
    var d = new Date();
    d.setSeconds(-retention);
    var query = "DELETE FROM " + dbname + ".dbo." + db + " WHERE";
    query += " id=" + index;
    query += " AND ts < " + d.getTime();
    query += ";";
    return query;
};

exports.getIdSelect = function (dbname, name) {
    if (!name) {
        return "SELECT id, type, name FROM " + dbname + ".dbo.datapoints;";
    } else {
        return "SELECT id, type, name FROM " + dbname + ".dbo.datapoints WHERE name='" + name + "';";
    }
};

exports.getIdInsert = function (dbname, name, type) {
    return  "INSERT INTO " + dbname + ".dbo.datapoints (name, type) VALUES('" + name + "', " + type + ");";
};

exports.getFromSelect = function (dbname, from) {
    return "SELECT id FROM " + dbname + ".dbo.sources WHERE name='" + from + "';";
};

exports.getFromInsert = function (dbname, from) {
    return "INSERT INTO " + dbname + ".dbo.sources (name) VALUES('" + from + "');";
};

exports.getHistory = function (dbname, db, options) {
    var query = "SELECT ";
    if (!options.start && options.count) {
        query += " TOP " + options.count;
    }
    query += " ts, val" +
        (!options.id  ? (", " + db + ".id as id") : "") +
        (options.ack  ? ", ack" : "") +
        (options.from ? (", " + dbname + ".dbo.sources.name as 'from'") : "") +
        (options.q    ? ", q" : "") + " FROM " + dbname + ".dbo." + db;

    if (options.from) {
        query += " INNER JOIN " + dbname + ".dbo.sources ON " + dbname + ".dbo.sources.id=" + dbname + ".dbo." + db + "._from";
    }

    var where = "";

    if (options.id) {
        where += " " + dbname + ".dbo." + db + ".id=" + options.id;
    }
    if (options.end) {
        where += (where ? " AND" : "") + " " + dbname + ".dbo." + db + ".ts < " + options.end;
    }
    if (options.start) {
        where += (where ? " AND" : "") + " " + dbname + ".dbo." + db + ".ts >= " + options.start;

        // add last value before start
        var subQuery;
        var subWhere;
        subQuery = " SELECT TOP 1 ts, val" +
            (!options.id  ? (", " + db + ".id as id") : "") +
            (options.ack  ? ", ack" : "") +
            (options.from ? (", " + dbname + ".dbo.sources.name as 'from'") : "") +
            (options.q    ? ", q" : "") + " FROM " + dbname + ".dbo." + db;
        if (options.from) {
            subQuery += " INNER JOIN " + dbname + ".dbo.sources ON " + dbname + ".dbo.sources.id=" + dbname + ".dbo." + db + "._from";
        }
        subWhere = "";
        if (options.id) {
            subWhere += " " + dbname + ".dbo." + db + ".id=" + options.id;
        }
        if (options.ignoreNull) {
            //subWhere += (subWhere ? " AND" : "") + " val <> NULL";
        }
        subWhere += (subWhere ? " AND" : "") + " " + dbname + ".dbo." + db + ".ts < " + options.start;
        if (subWhere) subQuery += " WHERE " + subWhere;
        subQuery += " ORDER BY " + dbname + ".dbo." + db + ".ts DESC";
        where += " UNION SELECT * FROM (" + subQuery + ") a";

        // add next value after end
        subQuery = " SELECT TOP 1 ts, val" +
            (!options.id  ? (", " + db + ".id as id") : "") +
            (options.ack  ? ", ack" : "") +
            (options.from ? (", " + dbname + ".dbo.sources.name as 'from'") : "") +
            (options.q    ? ", q" : "") + " FROM " + dbname + ".dbo." + db;
        if (options.from) {
            subQuery += " INNER JOIN " + dbname + ".dbo.sources ON " + dbname + ".dbo.sources.id=" + dbname + ".dbo." + db + "._from";
        }
        subWhere = "";
        if (options.id) {
            subWhere += " " + dbname + ".dbo." + db + ".id=" + options.id;
        }
        if (options.ignoreNull) {
            //subWhere += (subWhere ? " AND" : "") + " val <> NULL";
        }
        subWhere += (subWhere ? " AND" : "") + " " + dbname + ".dbo." + db + ".ts >= " + options.end;
        if (subWhere) subQuery += " WHERE " + subWhere;
        subQuery += " ORDER BY " + dbname + ".dbo." + db + ".ts ASC";
        where += " UNION SELECT * FROM (" + subQuery + ") b";
    }

    if (where) query += " WHERE " + where;

    query += " ORDER BY ts";
    if (!options.start && options.count) {
        query += " DESC";
    } else {
        query += " ASC";
    }
    query += ";";
    return query;
};
