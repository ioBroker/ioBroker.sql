/* jshint -W097 */// jshint strict:false
/*jslint node: true */
"use strict";

var utils   = require(__dirname + '/lib/utils'); // Get common adapter utils
var SQL     = require('sql-client');
var fs      = require('fs');
var commons = require(__dirname + '/lib/aggregate');

var clients = {
    postgresql: {name: 'PostgreSQLClient'},
    mysql:      {name: 'MySQLClient'},
    sqlite:     {name: 'SQLite3Client'}
};

var types   = {
    'number':  0,
    'boolean': 0,
    'string':  1
};

var dbNames = [
    'ts_number',
    'ts_string'
];

var sqlDPs  = {};
var from    = {};
var clientPool;
var subscribeAll = false;

var adapter = utils.adapter('sql');
adapter.on('objectChange', function (id, obj) {
    if (obj && obj.common && obj.common.history && obj.common.history[adapter.namespace]) {

        if (!sqlDPs[id] && !subscribeAll) {
            // unsubscribe
            for (var id in sqlDPs) {
                adapter.unsubscribeForeignStates(id);
            }
            subscribeAll = true;
            adapter.subscribeForeignStates('*');
        }
        sqlDPs[id] = obj.common.history;
        adapter.log.info('enabled logging of ' + id);
    } else {
        if (sqlDPs[id]) {
            adapter.log.info('disabled logging of ' + id);
            delete sqlDPs[id];
        }
    }
});

adapter.on('stateChange', function (id, state) {
    pushHistory(id, state);
});

adapter.on('unload', function (callback) {
    finish(callback);
});

adapter.on('ready', function () {
    main();
});

adapter.on('message', function (msg) {
    processMessage(msg);
});

process.on('SIGINT', function () {
    // close connection to DB
    finish();
});

function connect() {
    if (!clientPool) {
        var params = {
            host:       adapter.config.host + (adapter.config.port ? ':' + adapter.config.port : ''),
            user:       adapter.config.user,
            password:   adapter.config.password,
            max_idle:   2
        };

        clientPool = new SQL[clients[adapter.config.dbtype].name + 'Pool'](params);
        return clientPool.open(function (err) {
            if (err) {
                adapter.log.error(err);
                setTimeout(function () {
                    connect();
                }, 30000);
            } else {
                setTimeout(function () {
                    connect();
                }, 0);
            }
        });
    }

    readAndRunScripts(function () {
        adapter.log.info('Connected to ' + adapter.config.dbtype);
    });
}

function testConnection(msg) {
    var params = {
        host:       msg.message.config.host + (msg.message.config.port ? ':' + msg.message.config.port : ''),
        user:       msg.message.config.user,
        password:   msg.message.config.password
    };
    var timeout;
    try {
        var client = new SQL[clients[msg.message.config.dbtype].name](params);
        timeout = setTimeout(function () {
            timeout = null;
            adapter.sendTo(msg.from, msg.command, {error: 'connect timeout'}, msg.callback);
        }, 5000);

        client.connect(function (err) {
            if (err) {
                if (timeout) {
                    clearTimeout(timeout);
                    timeout = null;
                }
                return adapter.sendTo(msg.from, msg.command, {error: err.toString()}, msg.callback);
            }
            client.execute("SELECT 2 + 3 AS x", function (err, rows, fields) {
                client.disconnect();
                if (timeout) {
                    clearTimeout(timeout);
                    timeout = null;
                    return adapter.sendTo(msg.from, msg.command, {error: err ? err.toString() : null}, msg.callback);
                }
            });
        });
    } catch (ex) {
        if (timeout) {
            clearTimeout(timeout);
            timeout = null;
        }
        if (ex.toString() == 'TypeError: undefined is not a function') {
            return adapter.sendTo(msg.from, msg.command, {error: 'Node.js DB driver could not be installed.'}, msg.callback);
        } else {
            return adapter.sendTo(msg.from, msg.command, {error: ex.toString()}, msg.callback);
        }
    }
}

// one script
function oneScript(fileName, cb) {
    var file = fs.readFileSync(__dirname + '/sql/' + fileName).toString('utf-8');
    file = file.replace(/\r\n/g, '').replace(/\n/g, '').replace(/\t/g, ' ');

    if (file[0] == "﻿") file = file.substring(1);

    /*client.execute( "SELECT ? + 3 AS x", [ 4 ], function (err,rows,fields) {
     console.log("The answer is",rows[0].x);
     callback();
     }):*/
    clientPool.borrow(function (err, client) {
        client.execute(file, function(err, rows, fields) {
            if (err) {
                if (err.errno == 1007 || err.errno == 1050) { // if database exists or table exists
                    // do nothing

                } else
                if (err.code == '42P07') {
                    var match = script.match(/CREATE\s+TABLE\s+(\w*)\s+\(/);
                    if (match) {
                        adapter.log.debug('OK. Table "' + match[1] + '" yet exists');
                    } else {
                        adapter.log.error(file);
                        adapter.log.error(err);
                    }
                } else {
                    adapter.log.error(file);
                    adapter.log.error(err);
                }
            }
            if (cb) cb(err);
            clientPool.return(client);
        });
    });
}

// all scripts
function allScripts(files, cb) {
    if (files && files.length) {
        oneScript(files.shift(), function (err) {
            allScripts(files, cb);
        });
    } else {
        cb();
    }
}

function readAndRunScripts(cb) {
    var files = fs.readdirSync(__dirname + '/sql');
    files.sort();

    // remove scripts starting with "_"
    for (var f = files.length - 1; f >= 0; f--) {
        if (files[f][0] == '_') files.splice(f, 1);
    }

    allScripts(files, function () {
        cb && cb();
    });
}

function finish(callback) {
    if (clientPool) clientPool.close();
    if (callback)   callback();
}

function processMessage(msg) {
    if (msg.command == 'getHistory') {
        getHistory(msg);
    } else if (msg.command == 'test') {
        testConnection(msg);
    }
}

function main() {
    if (!clients[adapter.config.dbtype]) {
        adapter.log.error('Unknown DB type: ' + adapter.config.dbtype);
        adapter.stop();
    }
    // read all history settings
    adapter.objects.getObjectView('history', 'state', {}, function (err, doc) {
        var count = 0;
        if (doc && doc.rows) {
            for (var i = 0, l = doc.rows.length; i < l; i++) {
                if (doc.rows[i].value) {
                    var id = doc.rows[i].id;
                    sqlDPs[id] = doc.rows[i].value;

                    if (!sqlDPs[id][adapter.namespace]) {
                        delete sqlDPs[id];
                    } else {
                        count++;
                        adapter.log.info('enabled logging of ' + id);
                        sqlDPs[id][adapter.namespace].retention   = parseInt(sqlDPs[id][adapter.namespace].retention || adapter.config.retention, 10) || 0;
                        sqlDPs[id][adapter.namespace].debounce    = parseInt(sqlDPs[id][adapter.namespace].debounce  || adapter.config.debounce,  10) || 1000;
                        sqlDPs[id][adapter.namespace].changesOnly = sqlDPs[id][adapter.namespace].changesOnly === 'true' || sqlDPs[id][adapter.namespace].changesOnly === true;

                        // add one day if retention is too small
                        if (sqlDPs[id][adapter.namespace].retention <= 604800) {
                            sqlDPs[id][adapter.namespace].retention += 86400;
                        }
                    }
                }
            }
        }
        if (count < 20) {
            for (var id in sqlDPs) {
                adapter.subscribeForeignStates(id);
            }
        } else {
            subscribeAll = true;
            adapter.subscribeForeignStates('*');
        }
    });

    adapter.subscribeForeignObjects('*');

    if (adapter.config.host) {
        connect();
    }
}

function pushHistory(id, state) {
    // Push into redis
    if (sqlDPs[id]) {
        var settings = sqlDPs[id][adapter.namespace];

        if (!settings || !state) return;
        
        if (sqlDPs[id].state && settings.changesOnly && (state.ts !== state.lc)) return;

        sqlDPs[id].state = state;

        // Do not store values ofter than 1 second
        if (!sqlDPs[id].timeout) {

            sqlDPs[id].timeout = setTimeout(function (_id) {
                if (!sqlDPs[_id] || !sqlDPs[_id].state) return;
                var _settings = sqlDPs[_id][adapter.namespace];
                // if it was not deleted in this time
                if (_settings) {
                    sqlDPs[_id].timeout = null;

                    if (typeof sqlDPs[_id].state.val === 'string') {
                        var f = parseFloat(sqlDPs[_id].state.val);
                        if (f.toString() == sqlDPs[_id].state.val) {
                            sqlDPs[_id].state.val = f;
                        } else if (sqlDPs[_id].state.val === 'true') {
                            sqlDPs[_id].state.val = true;
                        } else if (sqlDPs[_id].state.val === 'false') {
                            sqlDPs[_id].state.val = false;
                        }
                    }
                    pushValueIntoDB(_id, sqlDPs[_id].state);
                }
            }, settings.debounce, id);
        }
    }
}

function checkRetention(id) {
    if (sqlDPs[id][adapter.namespace].retention) {
        var d = new Date();
        var dt = d.getTime();
        // check every 6 hours
        if (!sqlDPs[id].lastCheck || dt - sqlDPs[id].lastCheck >= 21600000/* 6 hours */) {
            sqlDPs[id].lastCheck = dt;
            // calculate date
            d.setSeconds(-(sqlDPs[id][adapter.namespace].retention));
            var query = "DELETE FROM iobroker." + dbNames[sqlDPs[id].type] + " WHERE";
            query += " id=" + sqlDPs[id].index;
            query += " AND ts < " + Math.round(d.getTime() / 1000);
            clientPool.borrow(function (err, client) {
                if (err) {
                    adapter.log.error(err);
                    return;
                }
                client.execute(query, function (err, rows, fields) {
                    if (err) {
                        adapter.log.error('Cannot delete ' + query + ': ' + err);
                    }
                    clientPool.return(client);
                });
            });
        }
    }
}

function pushValueIntoDB(id, state) {
    if (!clientPool) {
        adapter.log.warn('No connection to DB');
        return;
    }
    var type = types[typeof state.val];
    if (type === undefined) {
        adapter.log.warn('Cannot store values of type "' + typeof state.val + '"');
        return;
    }
    // get id if state
    if (sqlDPs[id].index === undefined) {
        // read or create in DB
        return getId(id, type, function (err) {
            if (err) {
                adapter.log.warn('Cannot get index of "' + id + '": ' + err);
            } else {
                pushValueIntoDB(id, state);
            }
        });
    }

    // get from
    if (state.from && !from[state.from]) {
        // read or create in DB
        return getFrom(state.from, function (err) {
            if (err) {
                adapter.log.warn('Cannot get "from" for "' + state.from + '": ' + err);
            } else {
                pushValueIntoDB(id, state);
            }
        });
    }
    var query = "INSERT INTO iobroker." + dbNames[type] + "(id, ts, val, ack, _from, q, ms) VALUES(" + sqlDPs[id].index + ", " + state.ts + ", " + state.val + ", " + (state.ack ? 1 : 0) + ", " + (from[state.from] || 0) + ", " + state.q + ", " + (state.ms || 0) + ");";

    adapter.log.debug(query);

    clientPool.borrow(function (err, client) {
        if (err) {
            adapter.log.error(err);
            return;
        }
        client.execute(query, function (err, rows, fields) {
            if (err) {
                adapter.log.error('Cannot insert ' + query + ': ' + err);
            }
            clientPool.return(client);
        });
    });
    checkRetention(id);
}

function getId(id, type, cb) {
    var query = "SELECT id, type FROM iobroker.datapoints WHERE name='" + id + "';";

    clientPool.borrow(function (err, client) {
        if (err) {
            if (cb) cb(err);
            return;
        }
        client.execute(query, function (err, rows, fields) {
            if (err) {
                adapter.log.error('Cannot select ' + query + ': ' + err);
                if (cb) cb(err);
                clientPool.return(client);
                return;
            }
            if (!rows.length) {
                if (type !== null) {
                    // insert
                    query = "SELECT MAX(id) FROM iobroker.datapoints;";
                    client.execute(query, function (err, rows, fields) {
                        if (err) {
                            adapter.log.error('Cannot select ' + query + ': ' + err);
                            if (cb) cb(err);
                            clientPool.return(client);
                            return;
                        }
                        var max = parseInt(rows[0] ? rows[0]['MAX(id)'] : 0) || 0;
                        query = "INSERT INTO iobroker.datapoints VALUES(" + (max + 1) + ", '" + id + "', " + type + ");";
                        client.execute(query, function (err, rows, fields) {
                            if (err) {
                                adapter.log.error('Cannot insert ' + query + ': ' + err);
                                if (cb) cb(err);
                                clientPool.return(client);
                                return;
                            }
                            sqlDPs[id].index = (max + 1);
                            sqlDPs[id].type  = type;

                            if (cb) cb();
                            clientPool.return(client);
                        });
                    });
                } else {
                    if (cb) cb('id not found');
                    clientPool.return(client);
                }
            } else {
                sqlDPs[id].index = rows[0].id;
                sqlDPs[id].type  = rows[0].type;

                if (cb) cb();
                clientPool.return(client);
            }
        });
    });
}

function getFrom(_from, cb) {
    var query = "SELECT id FROM iobroker.sources WHERE name='" + _from + "';";

    clientPool.borrow(function (err, client) {
        if (err) {
            if (cb) cb(err);
            return;
        }
        client.execute(query, function (err, rows, fields) {
            if (err) {
                adapter.log.error('Cannot select ' + query + ': ' + err);
                if (cb) cb(err);
                clientPool.return(client);
                return;
            }
            if (!rows.length) {
                // insert
                query = "SELECT MAX(id) FROM iobroker.sources;";
                client.execute(query, function (err, rows, fields) {
                    if (err) {
                        adapter.log.error('Cannot select ' + query + ': ' + err);
                        if (cb) cb(err);
                        clientPool.return(client);
                        return;
                    }
                    var max = parseInt(rows[0] ? rows[0]['MAX(id)'] : 0) || 0;
                    query = "INSERT INTO iobroker.sources VALUES(" + (max + 1) + ", '" + _from + "');";
                    client.execute(query, function (err, rows, fields) {
                        if (err) {
                            adapter.log.error('Cannot insert ' + query + ': ' + err);
                            if (cb) cb(err);
                            clientPool.return(client);
                            return;
                        }
                        from[_from] = (max + 1);

                        if (cb) cb();
                        clientPool.return(client);
                    });
                });
            } else {
                from[_from] = rows[0].id;

                if (cb) cb();
                clientPool.return(client);
            }
        });
    });
}

function sortByTs(a, b) {
    var aTs = a.ts;
    var bTs = b.ts;
    return ((aTs < bTs) ? -1 : ((aTs > bTs) ? 1 : 0));
}

function getDataFromDB(db, options, callback) {
    var query = "SELECT ts, val" +
        (!options.id  ? ", iobroker." + db + ".id as id" : "") +
        (options.ack  ? ", ack" : "") +
        (options.from ? ", iobroker.sources.name as 'from'" : "") +
        (options.q    ? ", q" : "") + " FROM iobroker." + db;

    if (options.from) {
        query += " INNER JOIN iobroker.sources ON iobroker.sources.id=iobroker." + db + "._from";
    }

    var where = "";

    if (options.id) {
        where += " iobroker." + db + ".id=" + sqlDPs[options.id].index;
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

    adapter.log.debug(query);

    clientPool.borrow(function (err, client) {
        if (err) {
            if (callback) callback(err);
            return;
        }
        client.execute(query, function (err, rows, fields) {
            // because descending
            if (!err && rows && !options.start && options.count) {
                rows.sort(sortByTs);
            }
            clientPool.return(client);
            if (callback) callback(err, rows);
        });
    });
}
function getHistory(msg) {
    var startTime = new Date().getTime();
    var options = {
        id:         msg.message.id == '*' ? null : msg.message.id,
        start:      msg.message.options.start,
        end:        msg.message.options.end || Math.round((new Date()).getTime() / 1000) + 5000,
        step:       parseInt(msg.message.options.step) || null,
        count:      parseInt(msg.message.options.count) || 500,
        ignoreNull: msg.message.options.ignoreNull,
        aggregate:  msg.message.options.aggregate || 'average', // One of: max, min, average, total
        limit:      msg.message.options.limit || adapter.config.limit || 2000,
        from:       msg.message.options.from  || false,
        q:          msg.message.options.q     || false,
        ack:        msg.message.options.ack   || false,
        ms:         msg.message.options.ms    || false
    };

    if (options.start > options.end){
        var _end = options.end;
        options.end   = options.start;
        options.start =_end;
    }

    if (!options.start && !options.count) {
        options.start = Math.round((new Date()).getTime() / 1000) - 5030; // - 1 year
    }

    if (options.id && sqlDPs[options.id].index === undefined) {
        // read or create in DB
        return getId(options.id, null, function (err) {
            if (err) {
                adapter.log.warn('Cannot get index of "' + options.id + '": ' + err);
                commons.sendResponse(adapter, msg, options, [], startTime);
            } else {
                getHistory(msg);
            }
        });
    }

    // if specific id requested
    if (options.id) {
        getDataFromDB(dbNames[sqlDPs[options.id].type], options, function (err, data) {
            commons.sendResponse(adapter, msg, options, (err ? err.toString() : null) || data, startTime);
        });
    } else {
        // if all IDs requested
        var rows = [];
        var count = 0;
        for (var db = 0; db < dbNames.length; db++) {
            count++;
            getDataFromDB(dbNames[db], options, function (err, data) {
                if (data) rows = rows.concat(data);
                if (!--count) {
                    rows.sort(sortByTs);
                    commons.sendResponse(adapter, msg, options, rows, startTime);
                }
            });
        }
    }
}

