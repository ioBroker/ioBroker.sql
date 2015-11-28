exports.init = function () {
    return [
        "CREATE DATABASE iobroker;",
        "CREATE TABLE iobroker.sources    (id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT, name TEXT);",
        "CREATE TABLE iobroker.datapoints (id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT, name TEXT,type INTEGER);",
        "CREATE TABLE iobroker.ts_number  (id INTEGER, ts BIGINT, val REAL,    ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));",
        "CREATE TABLE iobroker.ts_string  (id INTEGER, ts BIGINT, val TEXT,    ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));",
        "CREATE TABLE iobroker.ts_bool    (id INTEGER, ts BIGINT, val BOOLEAN, ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));"
    ];
};

exports.destroy = function () {
    return [
        "DROP TABLE iobroker.ts_number;",
        "DROP TABLE iobroker.ts_string;",
        "DROP TABLE iobroker.ts_bool;",
        "DROP TABLE iobroker.sources;",
        "DROP TABLE iobroker.datapoints;",
        "DROP DATABASE iobroker;"
    ];
};

exports.insert = function (index, state, from, db) {
    return "INSERT INTO iobroker." + db + " (id, ts, val, ack, _from, q) VALUES(" + index + ", " + state.ts + ", " + state.val + ", " + (state.ack ? 1 : 0) + ", " + (from || 0) + ", " + state.q + ");";
};

exports.retention = function (index, db, retention) {
    var d = new Date();
    d.setSeconds(-retention);
    var query = "DELETE FROM iobroker." + db + " WHERE";
    query += " id=" + index;
    query += " AND ts < " + Math.round(d.getTime() / 1000);
    query += ";";
    return query;
};

exports.getIdSelect = function (name) {
    return "SELECT id, type FROM iobroker.datapoints WHERE name='" + name + "';";
};

exports.getIdInsert = function (name, type) {
    return  "INSERT INTO iobroker.datapoints (name, type) VALUES('" + name + "', " + type + ");";
};

exports.getFromSelect = function (from) {
    return "SELECT id FROM iobroker.sources WHERE name='" + from + "';";
};

exports.getFromInsert = function (from) {
    return "INSERT INTO iobroker.sources (name) VALUES('" + from + "');";
};

exports.getHistory = function (db, options) {
    var query = "SELECT ts, val" +
        (!options.id  ? (", " + db + ".id as id") : "") +
        (options.ack  ? ", ack" : "") +
        (options.from ? (", iobroker.sources.name as 'from'") : "") +
        (options.q    ? ", q" : "") + " FROM iobroker." + db;

    if (options.from) {
        query += " INNER JOIN iobroker.sources ON iobroker.sources.id=iobroker." + db + "._from";
    }

    var where = "";

    if (options.id) {
        where += " iobroker." + db + ".id=" + options.id;
    }
    if (options.end) {
        where += (where ? " AND" : "") + " iobroker." + db + ".ts < " + options.end;
    }
    if (options.start) {
        where += (where ? " AND" : "") + " iobroker." + db + ".ts >= " + options.start;
    }

    if (where) query += " WHERE " + where;


    query += " ORDER BY iobroker." + db + ".ts";

    if (!options.start && options.count) {
        query += " DESC LIMIT " + options.count;
    } else {
        query += " ASC";
    }

    query += ";";
    return query;
};