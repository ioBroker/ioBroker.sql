exports.init = function () {
    return [
        "CREATE TABLE sources (id INTEGER,name TEXT);",
        "CREATE TABLE datapoints (id INTEGER NOT NULL PRIMARY KEY,name TEXT,type INTEGER);",
        "CREATE TABLE ts_number (id INTEGER, ts INTEGER, val REAL, ack boolean, _from INTEGER, ms INTEGER, q INTEGER);",
        "CREATE TABLE ts_string (id INTEGER, ts INTEGER, val TEXT, ack boolean, _from INTEGER, ms INTEGER, q INTEGER);"
    ];
};

exports.insert = function (index, state, from, db) {
    return "INSERT INTO " + db + "(id, ts, val, ack, _from, q, ms) VALUES(" + index + ", " + state.ts + ", " + state.val + ", " + (state.ack ? 1 : 0) + ", " + (from || 0) + ", " + state.q + ", " + (state.ms || 0) + ");";
};

exports.retention = function (index, db, retention) {
    var d = new Date();
    d.setSeconds(-retention);
    var query = "DELETE FROM " + db + " WHERE";
    query += " id=" + index;
    query += " AND ts < " + Math.round(d.getTime() / 1000);
    query += ";";
    return query;
};

exports.getIdSelect = function (name) {
    return "SELECT id, type FROM datapoints WHERE name='" + name + "';";
};
exports.getIdMax = function () {
    return "SELECT MAX(id) FROM datapoints;";
};
exports.getIdInsert = function (index, name, type) {
    return  "INSERT INTO datapoints VALUES(" + index + ", '" + name + "', " + type + ");";
};

exports.getFromSelect = function (from) {
    return "SELECT id FROM sources WHERE name='" + from + "';";
};
exports.getFromMax = function () {
    return "SELECT MAX(id) FROM sources;";
};
exports.getFromInsert = function (index, from) {
    return "INSERT INTO sources VALUES(" + index + ", '" + from + "');";
};

exports.getHistory = function (db, options) {
    var query = "SELECT ts, val" +
        (!options.id  ? (", " + db + ".id as id") : "") +
        (options.ack  ? ", ack" : "") +
        (options.ms   ? ", ms" : "") +
        (options.from ? (", sources.name as 'from'") : "") +
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
    }

    if (where) query += " WHERE " + where;


    query += " ORDER BY " + db + ".ts";

    if (!options.start && options.count) {
        query += " DESC LIMIT " + options.count;
    }

    query += ";";
    return query;
};