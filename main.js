/* jshint -W097 */// jshint strict:false
/*jslint node: true */
"use strict";

var utils    = require(__dirname + '/lib/utils'); // Get common adapter utils
var SQL      = require('sql-client');
var commons  = require(__dirname + '/lib/aggregate');
var SQLFuncs = null;
var fs       = require('fs');

var clients = {
    postgresql: {name: 'PostgreSQLClient'},
    mysql:      {name: 'MySQLClient'},
    sqlite:     {name: 'SQLite3Client'},
    mssql:      {name: 'MSSQLClient'}
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

var _client = false;
function connect() {
    if (!clientPool) {
        var params = {
            server:     adapter.config.host + (adapter.config.port ? ':' + adapter.config.port : ''),
            host:       adapter.config.host + (adapter.config.port ? ':' + adapter.config.port : ''),
            user:       adapter.config.user,
            password:   adapter.config.password,
            max_idle:   2
        };
        if (adapter.config.encrypt) {
            params.options = {
                encrypt: true // Use this if you're on Windows Azure
            };
        }

        if (adapter.config.dbtype === 'postgres') {
            params.database = 'postgres';
        }

        if (adapter.config.dbtype === 'sqlite') {
            params = getSqlLiteDir(adapter.config.fileName);
        }
        else
        // special solution for postgres. Connect first to Db "postgres", create new DB "iobroker" and then connect to "iobroker" DB.
        if (_client !== true && adapter.config.dbtype === 'postgresql') {
            if (adapter.config.dbtype == 'postgresql') {
                params.database = 'postgres';
            }
            // connect first to DB postgres and create iobroker DB
            _client = new SQL[clients[adapter.config.dbtype].name](params);
            return _client.connect(function (err) {
                if (err) {
                    adapter.log.error(err);
                    setTimeout(function () {
                        connect();
                    }, 30000);
                    return;
                }
                _client.execute('CREATE DATABASE iobroker;', function (err, rows, fields) {
                    _client.disconnect();
                    if (err && err.code !== '42P04') { // if error not about yet exists
                        _client = false;
                        adapter.log.error(err);
                        setTimeout(function () {
                            connect();
                        }, 30000);
                    } else {
                        _client = true;
                        setTimeout(function () {
                            connect();
                        }, 100);
                    }
                });
            });
        }

        if (adapter.config.dbtype == "postgresql") {
            params.database = "iobroker";
        }

        try {
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
        } catch (ex) {
            if (ex.toString() == 'TypeError: undefined is not a function') {
                adapter.log.error('Node.js DB driver for "' + adapter.config.dbtype + '" could not be installed.');
            } else {
                adapter.log.error(ex.toString());
            }
            return setTimeout(function () {
                connect();
            }, 30000);
        }
    }

    allScripts(SQLFuncs.init(), function (err) {
        if (err) {
            adapter.log.error(err);
            return setTimeout(function () {
                connect();
            }, 30000);
        } else {
            adapter.log.info('Connected to ' + adapter.config.dbtype);
        }
    });
}

// Find sqlite data directory
function getSqlLiteDir(fileName) {
    fileName = fileName || 'sqlite.db';
    fileName = fileName.replace(/\\/g, '/');
    if (fileName[0] == '/' || fileName.match(/^\w:\//)) {
        return fileName;
    }
    else {
        // normally /opt/iobroker/node_modules/iobroker.js-controller
        // but can be /example/ioBroker.js-controller
        var tools = require(utils.controllerDir + '/lib/tools');
        var config = tools.getConfigFileName().replace(/\\/g, '/');
        var parts = config.split('/');
        parts.pop();
        config = parts.join('/') + '/sqlite';
        // create sqlite directory
        if (!fs.existsSync(config)) {
            fs.mkdirSync(config);
        }

        return config + '/' + fileName;
    }
}

function testConnection(msg) {
    msg.message.config.port = parseInt(msg.message.config.port, 10) || 0;
    var params = {
        server:     msg.message.config.host + (msg.message.config.port ? ':' + msg.message.config.port : ''),
        host:       msg.message.config.host + (msg.message.config.port ? ':' + msg.message.config.port : ''),
        user:       msg.message.config.user,
        password:   msg.message.config.password
    };

    if (msg.message.config.dbtype === 'postgresql' && !SQL.PostgreSQLClient) {
        var postgres = require(__dirname + '/lib/postgresql-client');
        for (var attr in postgres) {
            if (!SQL[attr]) SQL[attr] = postgres[attr];
        }
    } else
    if (msg.message.config.dbtype === 'mssql' && !SQL.MSSQLClient) {
        var mssql = require(__dirname + '/lib/mssql-client');
        for (var attr in mssql) {
            if (!SQL[attr]) SQL[attr] = mssql[attr];
        }
    }

    if (msg.message.config.dbtype === 'postgresql') {
        params.database = 'postgres';
    } else if (msg.message.config.dbtype === 'sqlite') {
        params = getSqlLiteDir(msg.message.config.fileName);
    }
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
function oneScript(script, cb) {
    try {
        clientPool.borrow(function (err, client) {
            if (err || !client) {
                clientPool.close();
                clientPool = null;
                adapter.log.error(err);
                if (cb) cb(err);
                return;
            }
            adapter.log.debug(script);
            client.execute(script, function(err, rows, fields) {
                adapter.log.debug('Response: ' + JSON.stringify(err));
                if (err) {
                    // Database 'iobroker' already exists. Choose a different database name.
                    if (err.number === 1801 ||
                        //There is already an object named 'sources' in the database.
                        err.number === 2714) {
                        // do nothing
                        err = null;
                    } else
                    if (err.message && err.message.match(/^SQLITE_ERROR: table [\w_]+ already exists$/)) {
                        // do nothing
                        err = null;
                    } else
                    if (err.errno == 1007 || err.errno == 1050) { // if database exists or table exists
                        // do nothing
                        err = null;
                    }  else
                    if (err.code == '42P04') {// if database exists or table exists
                        // do nothing
                        err = null;
                    }
                    else if (err.code == '42P07') {
                        var match = script.match(/CREATE\s+TABLE\s+(\w*)\s+\(/);
                        if (match) {
                            adapter.log.debug('OK. Table "' + match[1] + '" yet exists');
                            err = null;
                        } else {
                            adapter.log.error(script);
                            adapter.log.error(err);
                        }
                    } else {
                        adapter.log.error(script);
                        adapter.log.error(err);
                    }
                }
                if (cb) cb(err);
                clientPool.return(client);
            });
        });
    } catch(ex) {
        adapter.log.error(ex);
    }

}

// all scripts
function allScripts(scripts, index, cb) {
    if (typeof index === 'function') {
        cb = index;
        index = 0;
    }
    index = index || 0;

    if (scripts && index < scripts.length) {
        oneScript(scripts[index], function (err) {
            if (err) {
                cb && cb(err);
            } else {
                allScripts(scripts, index + 1, cb);
            }
        });
    } else {
        cb && cb();
    }
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
    adapter.config.port = parseInt(adapter.config.port, 10) || 0;
    if (adapter.config.round !== null) adapter.config.round = Math.pow(10, parseInt(adapter.config.round, 10));
    if (adapter.config.dbtype === 'postgresql' && !SQL.PostgreSQLClient) {
        var postgres = require(__dirname + '/lib/postgresql-client');
        for (var attr in postgres) {
            if (!SQL[attr]) SQL[attr] = postgres[attr];
        }
    } else
    if (adapter.config.dbtype === 'mssql' && !SQL.MSSQLClient) {
        var mssql = require(__dirname + '/lib/mssql-client');
        for (var attr in mssql) {
            if (!SQL[attr]) SQL[attr] = mssql[attr];
        }
    }
    SQLFuncs = require(__dirname + '/lib/' + adapter.config.dbtype);

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

    if (adapter.config.dbtype === 'sqlite' || adapter.config.host) {
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
            var query = SQLFuncs.retention(sqlDPs[id].index, dbNames[sqlDPs[id].type], sqlDPs[id][adapter.namespace].retention);
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

    // todo change it after ms are added
    state.ts = parseInt(state.ts, 10) * 1000 + (parseInt(state.ms, 10) || 0);

    var query = SQLFuncs.insert(sqlDPs[id].index, state, from[state.from] || 0, dbNames[type]);
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
    var query = SQLFuncs.getIdSelect(id);

    clientPool.borrow(function (err, client) {
        if (err) {
            if (cb) cb(err);
            return;
        }
        client.execute(query, function (err, rows, fields) {
            if (rows && rows.rows) rows = rows.rows;
            if (err) {
                adapter.log.error('Cannot select ' + query + ': ' + err);
                if (cb) cb(err);
                clientPool.return(client);
                return;
            }
            if (!rows.length) {
                if (type !== null) {
                    // insert
                    query = SQLFuncs.getIdMax(id);
                    client.execute(query, function (err, rows, fields) {
                        if (rows && rows.rows) rows = rows.rows;
                        if (err) {
                            adapter.log.error('Cannot select ' + query + ': ' + err);
                            if (cb) cb(err);
                            clientPool.return(client);
                            return;
                        }
                        var max = 0;
                        if (rows[0]) {
                            if (rows[0]['MAX(id)'] !== undefined) max = rows[0]['MAX(id)'];
                            if (rows[0].max !== undefined) max = rows[0].max;
                        }
                        max = parseInt(max) || 0;

                        query = SQLFuncs.getIdInsert(max + 1, id, type);
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
    var sources    = (adapter.config.dbtype !== 'postgresql' ? "iobroker." : "") + "sources";
    var query = SQLFuncs.getFromSelect(_from);

    clientPool.borrow(function (err, client) {
        if (err) {
            if (cb) cb(err);
            return;
        }
        client.execute(query, function (err, rows, fields) {
            if (rows && rows.rows) rows = rows.rows;
            if (err) {
                adapter.log.error('Cannot select ' + query + ': ' + err);
                if (cb) cb(err);
                clientPool.return(client);
                return;
            }
            if (!rows.length) {
                // insert
                query = SQLFuncs.getFromMax();
                client.execute(query, function (err, rows, fields) {
                    if (rows && rows.rows) rows = rows.rows;
                    if (err) {
                        adapter.log.error('Cannot select ' + query + ': ' + err);
                        if (cb) cb(err);
                        clientPool.return(client);
                        return;
                    }
                    var max = 0;
                    if (rows[0]) {
                        if (rows[0]['MAX(id)'] !== undefined) max = rows[0]['MAX(id)'];
                        if (rows[0].max !== undefined) max = rows[0].max;
                    }
                    max = parseInt(max) || 0;

                    query = SQLFuncs.getFromInsert(max + 1, _from);
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
    if (options.start) options.start *= 1000;
    if (options.end)   options.end   *= 1000;
    if (options.step)  options.step  *= 1000;

    var query = SQLFuncs.getHistory(db, options);
    adapter.log.debug(query);

    clientPool.borrow(function (err, client) {
        if (err) {
            if (callback) callback(err);
            return;
        }
        client.execute(query, function (err, rows, fields) {
            if (rows && rows.rows) rows = rows.rows;
            // because descending
            if (!err && rows && !options.start && options.count) {
                rows.sort(sortByTs);
            }

            for (var c = 0; c < rows.length; c++) {
                // todo change it after ms are added
                if (options.ms) rows[c].ms = rows[c].ts % 1000;
                rows[c].ts = Math.round(rows[c].ts / 1000);

                if (options.ack) rows[c].ack = !!rows[c].ack;
                if (adapter.config.round !== null) rows[c].val = Math.round(rows[c].val * adapter.config.round) / adapter.config.round;
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
    var type = sqlDPs[options.id].type;
    if (options.id) options.id = sqlDPs[options.id].index;

    // if specific id requested
    if (options.id || options.id === 0) {
        getDataFromDB(dbNames[type], options, function (err, data) {
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

process.on('uncaughtException', function(err) {
    adapter.log.warn('Exception: ' + err);
});