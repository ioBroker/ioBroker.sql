exports.init = function () {
    return [
        "CREATE DATABASE iobroker;",
        "CREATE TABLE iobroker.dbo.sources (id INTEGER, name varchar(255));",
        "CREATE TABLE iobroker.dbo.datapoints (id INTEGER NOT NULL PRIMARY KEY, name varchar(255),type INTEGER);",
        "CREATE TABLE iobroker.dbo.ts_number (id INTEGER, ts BIGINT, val REAL, ack BIT, _from INTEGER, q INTEGER);",
        "CREATE TABLE iobroker.dbo.ts_string (id INTEGER, ts BIGINT, val TEXT, ack BIT, _from INTEGER, q INTEGER);"
    ];
};

exports.insert = function (index, state, from, db) {
    return "INSERT INTO iobroker.dbo." + db + "(id, ts, val, ack, _from, q) VALUES(" + index + ", " + state.ts + ", " + state.val + ", " + (state.ack ? 1 : 0) + ", " + (from || 0) + ", " + state.q + ");";
};

exports.retention = function (index, db, retention) {
    var d = new Date();
    d.setSeconds(-retention);
    var query = "DELETE FROM iobroker.dbo." + db + " WHERE";
    query += " id=" + index;
    query += " AND ts < " + Math.round(d.getTime() / 1000);
    query += ";";
    return query;
};

exports.getIdSelect = function (name) {
    return "SELECT id, type FROM iobroker.dbo.datapoints WHERE name='" + name + "';";
};
exports.getIdMax = function () {
    return "SELECT MAX(id) FROM iobroker.dbo.datapoints;";
};
exports.getIdInsert = function (index, name, type) {
    return  "INSERT INTO iobroker.dbo.datapoints VALUES(" + index + ", '" + name + "', " + type + ");";
};

exports.getFromSelect = function (from) {
    return "SELECT id FROM iobroker.dbo.sources WHERE name='" + from + "';";
};
exports.getFromMax = function () {
    return "SELECT MAX(id) FROM iobroker.dbo.sources;";
};
exports.getFromInsert = function (index, from) {
    return "INSERT INTO iobroker.dbo.sources VALUES(" + index + ", '" + from + "');";
};

exports.getHistory = function (db, options) {
    var query = "SELECT ";
    if (!options.start && options.count) {
        query += " TOP " + options.count;
    }
    query += " ts, val" +
        (!options.id  ? (", " + db + ".id as id") : "") +
        (options.ack  ? ", ack" : "") +
        (options.from ? (", iobroker.dbo.sources.name as 'from'") : "") +
        (options.q    ? ", q" : "") + " FROM iobroker.dbo." + db;

    if (options.from) {
        query += " INNER JOIN iobroker.dbo.sources ON iobroker.dbo.sources.id=iobroker.dbo." + db + "._from";
    }

    var where = "";

    if (options.id) {
        where += " iobroker.dbo." + db + ".id=" + options.id;
    }
    if (options.end) {
        where += (where ? " AND" : "") + " iobroker.dbo." + db + ".ts < " + options.end;
    }
    if (options.start) {
        where += (where ? " AND" : "") + " iobroker.dbo." + db + ".ts >= " + options.start;
    }

    if (where) query += " WHERE " + where;


    query += " ORDER BY iobroker.dbo." + db + ".ts";
    if (!options.start && options.count) {
        query += " DESC";
    } else {
        query += " ASC";
    }
    query += ";";
    return query;
};