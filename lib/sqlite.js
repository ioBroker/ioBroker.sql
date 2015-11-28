exports.init = function () {
    return [
        "CREATE TABLE sources    (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, name TEXT);",
        "CREATE TABLE datapoints (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,name TEXT,type INTEGER);",
        "CREATE TABLE ts_number  (id INTEGER, ts INTEGER, val REAL,    ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));",
        "CREATE TABLE ts_string  (id INTEGER, ts INTEGER, val TEXT,    ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));",
        "CREATE TABLE ts_bool    (id INTEGER, ts INTEGER, val BOOLEAN, ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));"
    ];
};

exports.destroy = function () {
    return [
        "DROP TABLE ts_number;",
        "DROP TABLE ts_string;",
        "DROP TABLE ts_bool;",
        "DROP TABLE sources;",
        "DROP TABLE datapoints;"
    ];
};
exports.insert = function (index, state, from, db) {
    if (db === 'ts_bool') state.val = state.val ? 1 : 0;
    return "INSERT INTO " + db + " (id, ts, val, ack, _from, q) VALUES(" + index + ", " + state.ts + ", " + state.val + ", " + (state.ack ? 1 : 0) + ", " + (from || 0) + ", " + state.q + ");";
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

exports.getIdInsert = function (name, type) {
    return  "INSERT INTO datapoints (name, type) VALUES('" + name + "', " + type + ");";
};

exports.getFromSelect = function (from) {
    return "SELECT id FROM sources WHERE name='" + from + "';";
};

exports.getFromInsert = function (from) {
    return "INSERT INTO sources (name) VALUES('" + from + "');";
};

exports.getHistory = function (db, options) {
    var query = "SELECT ts, val" +
        (!options.id  ? (", " + db + ".id as id") : "") +
        (options.ack  ? ", ack" : "") +
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
    } else {
        query += " ASC";
    }

    query += ";";
    return query;
};