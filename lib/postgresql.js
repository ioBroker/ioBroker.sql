exports.init = function (dbname) {
    return [
        "CREATE TABLE sources    (id INTEGER NOT NULL PRIMARY KEY SERIAL, name TEXT);",
        "CREATE TABLE datapoints (id INTEGER NOT NULL PRIMARY KEY SERIAL, name TEXT, type INTEGER);",
        "CREATE TABLE ts_number  (id INTEGER NOT NULL, ts BIGINT, val REAL,    ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));",
        "CREATE TABLE ts_string  (id INTEGER NOT NULL, ts BIGINT, val TEXT,    ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));",
        "CREATE TABLE ts_bool    (id INTEGER NOT NULL, ts BIGINT, val BOOLEAN, ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));"
    ];
};

exports.destroy = function (dbname) {
    return [
        "DROP TABLE ts_number;",
        "DROP TABLE ts_string;",
        "DROP TABLE ts_bool;",
        "DROP TABLE sources;",
        "DROP TABLE datapoints;"
    ];
};

exports.insert = function (dbname, index, state, from, db) {
    if (db === "ts_string") state.val = "'" + state.val + "'";
    return "INSERT INTO " + db + " (id, ts, val, ack, _from, q) VALUES(" + index + ", " + state.ts + ", " + state.val + ", " + (!!state.ack) + ", " + (from || 0)  + ", " + state.q + ");";
};

exports.retention = function (dbname, index, db, retention) {
    var d = new Date();
    d.setSeconds(-retention);
    var query = "DELETE FROM " + db + " WHERE";
    query += " id=" + index;
    query += " AND ts < " + d.getTime();
    query += ";";

    return query;
};

exports.getIdSelect = function (dbname, name) {
    return "SELECT id, type FROM datapoints WHERE name='" + name + "';";
};

exports.getIdInsert = function (dbname, name, type) {
    return  "INSERT INTO datapoints (name, type) VALUES('" + name + "', " + type + ");";
};

exports.getFromSelect = function (dbname, from) {
    return "SELECT id FROM sources WHERE name='" + from + "';";
};

exports.getFromInsert = function (dbname, from) {
    return "INSERT INTO sources (name) VALUES('" + from + "');";
};
exports.getHistory = function (dbname, db, options) {
    var query = "SELECT ts, val" +
        (!options.id  ? (", " + db + ".id as id") : "") +
        (options.ack  ? ", ack" : "") +
        (options.from ? (", sources.name as from") : "") +
        (options.q    ? ", q" : "") + " FROM " + db;

    if (options.from) {
        query += " INNER JOIN sources ON sources.id=" + db + "._from";
    }

    var where = "";

    if (options.id) {
        where += " " + db + ".id=" + options.id;
    }
    if (options.end) {
        where += (where ? " AND" : "") + " " + db + ".ts < " + options.end;
    }
    if (options.start) {
        where += (where ? " AND" : "") + " " + db + ".ts >= " + options.start;
                
        //add last value before start
        var subQuery, subWhere;
        subQuery = " SELECT as ts, val" +
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
        subWhere += (subWhere ? " AND" : "") + " " + db + ".ts <= " + options.start;
        if (subWhere) subQuery += " WHERE " + subWhere;
        subQuery += " ORDER BY " + db + ".ts DESC LIMIT 1";
        where += " UNION (" + subQuery + ")";
        
        //add next value after end
        subQuery = null;
        subWhere= null;
        subQuery = " SELECT as ts, val" +
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
        subWhere += (subWhere ? " AND" : "") + " " + db + ".ts >= " + options.end;
        if (subWhere) subQuery += " WHERE " + subWhere;
        subQuery += " ORDER BY " + db + ".ts DESC LIMIT 1";
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
