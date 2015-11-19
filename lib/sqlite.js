exports.init = function () {
    return [
        "CREATE DATABASE iobroker;",
        "CREATE TABLE iobroker.sources (id INTEGER,name TEXT);",
        "CREATE TABLE iobroker.datapoints (id INTEGER NOT NULL PRIMARY KEY,name TEXT,type INTEGER);",
        "CREATE TABLE iobroker.ts_number (id INTEGER, ts INTEGER, val REAL, ack boolean, _from INTEGER, ms INTEGER, q INTEGER);",
        "CREATE TABLE iobroker.ts_string (id INTEGER, ts INTEGER, val TEXT, ack boolean, _from INTEGER, ms INTEGER, q INTEGER);"
    ];
};

exports.insert = function (index, state, from, db) {
    return "INSERT INTO iobroker." + db + "(id, ts, val, ack, _from, q, ms) VALUES(" + index + ", " + state.ts + ", " + state.val + ", " + (state.ack ? 1 : 0) + ", " + (from || 0) + ", " + state.q + ", " + (state.ms || 0) + ");";
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
exports.getIdMax = function () {
    return "SELECT MAX(id) FROM iobroker.datapoints;";
};
exports.getIdInsert = function (index, name, type) {
    return  "INSERT INTO iobroker.datapoints VALUES(" + index + ", '" + name + "', " + type + ");";
};

exports.getFromSelect = function (from) {
    return "SELECT id FROM iobroker.sources WHERE name='" + from + "';";
};
exports.getFromMax = function () {
    return "SELECT MAX(id) FROM iobroker.sources;";
};
exports.getFromInsert = function (index, from) {
    return "INSERT INTO iobroker.sources VALUES(" + index + ", '" + from + "');";
};

exports.getHistory = function (db, options) {
    var query = "SELECT ts, val" +
        (!options.id  ? (", " + db + ".id as id") : "") +
        (options.ack  ? ", ack" : "") +
        (options.ms   ? ", ms" : "") +
        (options.from ? (", iobroker.sources.name as 'from'") : "") +
        (options.q    ? ", q" : "") + " FROM " + db;

    if (options.from) {
        query += " INNER JOIN iobroker.sources ON iobroker.sources.id=" + db + "._from";
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
    }

    query += ";";
    return query;
};