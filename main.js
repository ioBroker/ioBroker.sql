/* jshint -W097 */// jshint strict:false
/*jslint node: true */
'use strict';

var utils    = require(__dirname + '/lib/utils'); // Get common adapter utils
var SQL      = require('sql-client');
var commons  = require(__dirname + '/lib/aggregate');
var SQLFuncs = null;
var fs       = require('fs');

var clients = {
    postgresql: {name: 'PostgreSQLClient',  multiRequests: true},
    mysql:      {name: 'MySQLClient',       multiRequests: true},
    sqlite:     {name: 'SQLite3Client',     multiRequests: false},
    mssql:      {name: 'MSSQLClient',       multiRequests: true}
};

var types   = {
    'number':  0,
    'string':  1,
    'boolean': 2,
    'object':  1
};

var dbNames = [
    'ts_number',
    'ts_string',
    'ts_bool'
];

var storageTypes = [
    'Number',
    'String',
    'Boolean'
];

var clientPool;
var sqlDPs        = {};
var from          = {};
var subscribeAll  = false;
var tasks         = [];
var tasksReadType = [];
var multiRequests = true;
var tasksStart    = [];
var finished      = false;
var connected     = null;
var isFromRunning = {};

var adapter = utils.Adapter('sql');
adapter.on('objectChange', function (id, obj) {
    var tmpState;
    var now = new Date().getTime();
    if (obj && obj.common &&
        (
            // todo remove history sometime (2016.08) - Do not forget object selector in io-package.json
            (obj.common.history && obj.common.history[adapter.namespace] && obj.common.history[adapter.namespace].enabled) ||
            (obj.common.custom  && obj.common.custom[adapter.namespace]  && obj.common.custom[adapter.namespace].enabled)
        )
    ) {
        if (!(sqlDPs[id] && sqlDPs[id][adapter.namespace]) && !subscribeAll) {
            // un-subscribe
            for (var _id in sqlDPs) {
                if (sqlDPs.hasOwnProperty(_id) && sqlDPs[_id] && sqlDPs[_id][adapter.namespace]) {
                    adapter.unsubscribeForeignStates(_id);
                }
            }
            subscribeAll = true;
            adapter.subscribeForeignStates('*');
        }
        var writeNull = !sqlDPs[id];
        if (sqlDPs[id] && sqlDPs[id].relogTimeout) {
            clearTimeout(sqlDPs[id].relogTimeout);
        }

        var storedIndex = null;
        var storedType = null;
        if (sqlDPs[id] && sqlDPs[id].index !== undefined) storedIndex = sqlDPs[id].index;
        if (sqlDPs[id] && sqlDPs[id].dbtype !== undefined) storedType = sqlDPs[id].dbtype;
        // todo remove history sometime (2016.08)
        sqlDPs[id] = obj.common.custom || obj.common.history;
        if (storedIndex !== null) sqlDPs[id].index = storedIndex;
        if (storedType !== null) {
            /*if (sqlDPs[id][adapter.namespace].storageType) {
                if (storageTypes.indexOf(sqlDPs[id][adapter.namespace].storageType) === storedType) {
                    sqlDPs[id].dbtype = storedType;
                }
                else {
                    adapter.log.info('Can not reuse DB type because Store-As is different. First stored data will define it. (' + sqlDPs[id][adapter.namespace].storageType + ' -> ' + storageTypes.indexOf(sqlDPs[id][adapter.namespace].storageType) + ' vs. ' + storedType + ')');
                }
            }
            else {
                sqlDPs[id].dbtype = storedType;
            }*/
            sqlDPs[id].dbtype = storedType;
        }
        adapter.log.debug('remembered Index/Type ' + sqlDPs[id].index + ' / ' + sqlDPs[id].dbtype);

        if (sqlDPs[id][adapter.namespace].retention !== undefined && sqlDPs[id][adapter.namespace].retention !== null && sqlDPs[id][adapter.namespace].retention !== '') {
            sqlDPs[id][adapter.namespace].retention = parseInt(sqlDPs[id][adapter.namespace].retention || adapter.config.retention, 10) || 0;
        } else {
            sqlDPs[id][adapter.namespace].retention = adapter.config.retention;
        }
        if (sqlDPs[id][adapter.namespace].debounce !== undefined && sqlDPs[id][adapter.namespace].debounce !== null && sqlDPs[id][adapter.namespace].debounce !== '') {
            sqlDPs[id][adapter.namespace].debounce = parseInt(sqlDPs[id][adapter.namespace].debounce, 10) || 0;
        } else {
            sqlDPs[id][adapter.namespace].debounce = adapter.config.debounce;
        }
        sqlDPs[id][adapter.namespace].changesOnly = sqlDPs[id][adapter.namespace].changesOnly === 'true' || sqlDPs[id][adapter.namespace].changesOnly === true;
        if (sqlDPs[id][adapter.namespace].changesRelogInterval !== undefined && sqlDPs[id][adapter.namespace].changesRelogInterval !== null && sqlDPs[id][adapter.namespace].changesRelogInterval !== '') {
            sqlDPs[id][adapter.namespace].changesRelogInterval = parseInt(sqlDPs[id][adapter.namespace].changesRelogInterval, 10) || 0;
        } else {
            sqlDPs[id][adapter.namespace].changesRelogInterval = adapter.config.changesRelogInterval;
        }
        if (sqlDPs[id][adapter.namespace].changesRelogInterval > 0) {
            sqlDPs[id].relogTimeout = setTimeout(reLogHelper, (sqlDPs[id][adapter.namespace].changesRelogInterval * 500 * Math.random()) + sqlDPs[id][adapter.namespace].changesRelogInterval * 500, id);
        }
        if (sqlDPs[id][adapter.namespace].changesMinDelta !== undefined && sqlDPs[id][adapter.namespace].changesMinDelta !== null && sqlDPs[id][adapter.namespace].changesMinDelta !== '') {
            sqlDPs[id][adapter.namespace].changesMinDelta = parseFloat(sqlDPs[id][adapter.namespace].changesMinDelta.toString().replace(/,/g, '.')) || 0;
        } else {
            sqlDPs[id][adapter.namespace].changesMinDelta = adapter.config.changesMinDelta;
        }
        if (!sqlDPs[id][adapter.namespace].storageType) sqlDPs[id][adapter.namespace].storageType = false;

        // add one day if retention is too small
        if (sqlDPs[id][adapter.namespace].retention && sqlDPs[id][adapter.namespace].retention <= 604800) {
            sqlDPs[id][adapter.namespace].retention += 86400;
        }
        if (writeNull) {
            writeNulls(id);
        }
        adapter.log.info('enabled logging of ' + id);
    }
    else {
        if (sqlDPs[id]) {
            adapter.log.info('disabled logging of ' + id);
            if (sqlDPs[id].relogTimeout) clearTimeout(sqlDPs[id].relogTimeout);
            if (sqlDPs[id].timeout) clearTimeout(sqlDPs[id].timeout);

            if (Object.assign) {
                tmpState = Object.assign({}, sqlDPs[id].state);
            }
            else {
                tmpState = JSON.parse(JSON.stringify(sqlDPs[id].state));
            }
            var state = sqlDPs[id].state ? tmpState : null;

            if (sqlDPs[id].skipped) {
                pushValueIntoDB(id, sqlDPs[id].skipped);
                sqlDPs[id].skipped = null;
            }

            var nullValue = {val: null, ts: now, lc: now, q: 0x40, from: 'system.adapter.' + adapter.namespace};
            if (sqlDPs[id][adapter.namespace]) {
                if (sqlDPs[id][adapter.namespace].changesOnly && state && state.val !== null) {
                    (function (_id, _state, _nullValue) {
                        _state.ts   = now;
                        _state.from = 'system.adapter.' + adapter.namespace;
                        nullValue.ts += 4;
                        nullValue.lc += 4; // because of MS SQL
                        adapter.log.debug('Write 1/2 "' + _state.val + '" _id: ' + _id);
                        pushValueIntoDB(_id, _state, function () {
                            // terminate values with null to indicate adapter stop. timestamp + 1#
                            adapter.log.debug('Write 2/2 "null" _id: ' + _id);
                            pushValueIntoDB(_id, _nullValue, function() {
                                delete sqlDPs[id][adapter.namespace];
                            });
                        });
                    })(id, state, nullValue);
                }
                else {
                    // terminate values with null to indicate adapter stop. timestamp + 1
                    adapter.log.debug('Write 0 NULL _id: ' + id);
                    pushValueIntoDB(id, nullValue, function() {
                        delete sqlDPs[id][adapter.namespace];
                    });
                }
            }
            else {
                delete sqlDPs[id][adapter.namespace];
            }
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
process.on('SIGTERM', function () {
    // close connection to DB
    finish();
});

function setConnected(isConnected) {
    if (connected !== isConnected) {
        connected = isConnected;
        adapter.setState('info.connection', connected, true);
    }
}

var _client = false;
function connect() {
    if (!clientPool) {
        setConnected(false);

        var params = {
            server:     adapter.config.host, // needed for MSSQL
            host:       adapter.config.host, // needed for PostgeSQL , MySQL
            user:       adapter.config.user,
            password:   adapter.config.password,
            max_idle:   (adapter.config.dbtype === 'sqlite') ? 1 : 2
        };
        if (adapter.config.port) {
            params.port = adapter.config.port;
        }
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
            if (adapter.config.dbtype === 'postgresql') {
                params.database = 'postgres';
            }

            if (!adapter.config.dbtype) {
                adapter.log.error('DB Type is not defined!');
                return;
            }
            if (!clients[adapter.config.dbtype] || !clients[adapter.config.dbtype].name) {
                adapter.log.error('Unknown type "' + adapter.config.dbtype + '"');
                return;
            }
            if (!SQL[clients[adapter.config.dbtype].name]) {
                adapter.log.error('SQL package "' + clients[adapter.config.dbtype].name + '" is not installed.');
                return;
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
                _client.execute('CREATE DATABASE ' + adapter.config.dbname + ';', function (err /* , rows, fields */) {
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

        if (adapter.config.dbtype === 'postgresql') {
            params.database = adapter.config.dbname;
        }

        try {
            if (!clients[adapter.config.dbtype].name) {
                adapter.log.error('Unknown SQL type selected:  "' + adapter.config.dbtype + '"');
            } else if (!SQL[clients[adapter.config.dbtype].name + 'Pool']) {
                adapter.log.error('Selected SQL DB was not installed properly:  "' + adapter.config.dbtype + '". SQLite requires build tools on system. See README.md');
            } else {
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
        } catch (ex) {
            if (ex.toString() === 'TypeError: undefined is not a function') {
                adapter.log.error('Node.js DB driver for "' + adapter.config.dbtype + '" could not be installed.');
            } else {
                adapter.log.error(ex.toString());
            }
            setConnected(false);
            return setTimeout(function () {
                connect();
            }, 30000);
        }
    }

    allScripts(SQLFuncs.init(adapter.config.dbname), function (err) {
        if (err) {
            //adapter.log.error(err);
            return setTimeout(function () {
                connect();
            }, 30000);
        } else {
            adapter.log.info('Connected to ' + adapter.config.dbtype);
            setConnected(true);
            // read all DB IDs and all FROM ids
            if (!multiRequests) {
                getAllIds(function () {
                    getAllFroms();
                    processStartValues();
                });
            } else {
                getAllIds(function () {
                    processStartValues();
                });
            }
        }
    });
}

// Find sqlite data directory
function getSqlLiteDir(fileName) {
    fileName = fileName || 'sqlite.db';
    fileName = fileName.replace(/\\/g, '/');
    if (fileName[0] === '/' || fileName.match(/^\w:\//)) {
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
        server:     msg.message.config.host,
        host:       msg.message.config.host,
        user:       msg.message.config.user,
        password:   msg.message.config.password
    };
    if (msg.message.config.port) {
        params.port = msg.message.config.port;
    }

    if (msg.message.config.dbtype === 'postgresql' && !SQL.PostgreSQLClient) {
        var postgres = require(__dirname + '/lib/postgresql-client');
        for (var attr in postgres) {
            if (!SQL[attr]) SQL[attr] = postgres[attr];
        }
    } else
    if (msg.message.config.dbtype === 'mssql' && !SQL.MSSQLClient) {
        var mssql = require(__dirname + '/lib/mssql-client');
        for (var _attr in mssql) {
            if (!SQL[_attr]) SQL[_attr] = mssql[_attr];
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
            client.execute("SELECT 2 + 3 AS x", function (err /* , rows, fields */) {
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
        if (ex.toString() === 'TypeError: undefined is not a function') {
            return adapter.sendTo(msg.from, msg.command, {error: 'Node.js DB driver could not be installed.'}, msg.callback);
        } else {
            return adapter.sendTo(msg.from, msg.command, {error: ex.toString()}, msg.callback);
        }
    }
}

function destroyDB(msg) {
    try {
        allScripts(SQLFuncs.destroy(adapter.config.dbname), function (err) {
            if (err) {
                adapter.log.error(err);
                adapter.sendTo(msg.from, msg.command, {error: err.toString()}, msg.callback);
            } else {
                adapter.sendTo(msg.from, msg.command, {error: null}, msg.callback);
                // restart adapter
                setTimeout(function () {
                    adapter.getForeignObject('system.adapter.' + adapter.namespace, function (err, obj) {
                        if (!err) {
                            adapter.setForeignObject(obj._id, obj);
                        } else {
                            adapter.log.error('Cannot read object "system.adapter.' + adapter.namespace + '": ' + err);
                            adapter.stop();
                        }
                    });
                }, 2000);
            }
        });
    } catch (ex) {
        return adapter.sendTo(msg.from, msg.command, {error: ex.toString()}, msg.callback);
    }
}

function _userQuery(msg, callback) {
    try {
        adapter.log.debug(msg.message);

        clientPool.borrow(function (err, client) {
            if (err) {
                adapter.sendTo(msg.from, msg.command, {error: err.toString()}, msg.callback);
                if (callback) callback();
            } else {
                client.execute(msg.message, function (err, rows /* , fields */) {
                    if (rows && rows.rows) rows = rows.rows;
                    clientPool.return(client);
                    adapter.sendTo(msg.from, msg.command, {error: err ? err.toString() : null, result: rows}, msg.callback);
                    if (callback) callback();
                });
            }
        });
    } catch (err) {
        adapter.sendTo(msg.from, msg.command, {error: err.toString()}, msg.callback);
        if (callback) callback();
    }
}
// execute custom query
function query(msg) {
    if (!multiRequests) {
        if (tasks.length > 100) {
            adapter.log.error('Cannot queue new requests, because more than 100');
            adapter.sendTo(msg.from, msg.command, {error: 'Cannot queue new requests, because more than 100'}, msg.callback);
            return;
        }
        tasks.push({operation: 'userQuery', msg: msg});
        if (tasks.length === 1) processTasks();
    } else {
        _userQuery(msg);
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
            client.execute(script, function(err /* , rows, fields */) {
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
                    if (err.code === '42P04') {// if database exists or table exists
                        // do nothing
                        err = null;
                    }
                    else if (err.code === '42P07') {
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
        if (cb) cb(ex);
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
                if (cb) cb(err);
            } else {
                allScripts(scripts, index + 1, cb);
            }
        });
    } else {
        if (cb) cb();
    }
}

function finish(callback) {
    adapter.unsubscribeForeignStates('*');
    var count = 0;
    if (finished) {
        if (callback) {
            if (finished === true) {
                callback();
            } else {
                finished.push(callback);
            }
        }
        return;
    }
    finished = [callback];
    var now = new Date().getTime();
    for (var id in sqlDPs) {
        if (!sqlDPs.hasOwnProperty(id)) continue;

        if (sqlDPs[id].relogTimeout) {
            clearTimeout(sqlDPs[id].relogTimeout);
            sqlDPs[id].relogTimeout = null;
        }
        if (sqlDPs[id].timeout) {
            clearTimeout(sqlDPs[id].timeout);
            sqlDPs[id].timeout  = null;
        }
        var tmpState;
        if (Object.assign) {
            tmpState = Object.assign({}, sqlDPs[id].state);
        }
        else {
            tmpState = JSON.parse(JSON.stringify(sqlDPs[id].state));
        }
        var state = sqlDPs[id].state ? tmpState : null;

        if (sqlDPs[id].skipped) {
            count++;
            pushValueIntoDB(id, sqlDPs[id].skipped, function () {
                if (!--count) {
                    if (clientPool) {
                        clientPool.close();
                        clientPool = null;
                    }
                    if (typeof finished === 'object') {
                        setTimeout(function (cb) {
                            for (var f = 0; f < cb.length; f++) {
                                cb[f]();
                            }
                        }, 500, finished);
                        finished = true;
                    }
                }
            });
            sqlDPs[id].skipped = null;
        }

        var nullValue = {val: null, ts: now, lc: now, q: 0x40, from: 'system.adapter.' + adapter.namespace};
        if (sqlDPs[id][adapter.namespace]) {
            if (sqlDPs[id][adapter.namespace].changesOnly && state && state.val !== null) {
                count++;
                (function (_id, _state, _nullValue) {
                    _state.ts   = now;
                    _state.from = 'system.adapter.' + adapter.namespace;
                    nullValue.ts += 4;
                    nullValue.lc += 4; // because of MS SQL
                    adapter.log.debug('Write 1/2 "' + _state.val + '" _id: ' + _id);
                    pushValueIntoDB(_id, _state, function () {
                        // terminate values with null to indicate adapter stop. timestamp + 1#
                        adapter.log.debug('Write 2/2 "null" _id: ' + _id);
                        pushValueIntoDB(_id, _nullValue, function () {
                            if (!--count) {
                                if (clientPool) {
                                    clientPool.close();
                                    clientPool = null;
                                }
                                if (typeof finished === 'object') {
                                    setTimeout(function (cb) {
                                        for (var f = 0; f < cb.length; f++) {
                                            cb[f]();
                                        }
                                    }, 500, finished);
                                    finished = true;
                                }
                            }
                        });
                    });
                })(id, state, nullValue);
            } else {
                // terminate values with null to indicate adapter stop. timestamp + 1
                count++;
                adapter.log.debug('Write 0 NULL _id: ' + id);
                pushValueIntoDB(id, nullValue, function () {
                    if (!--count) {
                        if (clientPool) {
                            clientPool.close();
                            clientPool = null;
                        }
                        if (typeof finished === 'object') {
                            setTimeout(function (cb) {
                                for (var f = 0; f < cb.length; f++) {
                                    cb[f]();
                                }
                            }, 500, finished);
                            finished = true;
                        }
                    }
                });
            }
        }
    }

    if (!count && callback) {
        if (clientPool) {
            clientPool.close();
            clientPool = null;
        }
        callback();
    }
}

function processMessage(msg) {
    if (msg.command === 'getHistory') {
        getHistory(msg);
    }
    else if (msg.command === 'test') {
        testConnection(msg);
    }
    else if (msg.command === 'destroy') {
        destroyDB(msg);
    }
    /* else if (msg.command === 'generateDemo') {
        generateDemo(msg);
    } */
    else if (msg.command === 'query') {
        query(msg);
    }
    else if (msg.command === 'storeState') {
        storeState(msg);
    }
    else if (msg.command === 'getDpOverview') {
        getDpOverview(msg);
    }
    else if (msg.command === 'enableHistory') {
        enableHistory(msg);
    }
    else if (msg.command === 'disableHistory') {
        disableHistory(msg);
    }
    else if (msg.command === 'getEnabledDPs') {
        getEnabledDPs(msg);
    } else if (msg.command === 'stopInstance') {
        finish(function () {
            if (msg.callback) {
                adapter.sendTo(msg.from, msg.command, 'stopped', msg.callback);
                setTimeout(function () {
                    process.exit(0);
                }, 200);
            }
        });
    }
}

function fixSelector(callback) {
    // fix _design/custom object
    adapter.getForeignObject('_design/custom', function (err, obj) {
        if (!obj || obj.views.state.map.indexOf('common.history') === -1 || obj.views.state.map.indexOf('common.custom') === -1) {
            obj = {
                _id: '_design/custom',
                language: 'javascript',
                views: {
                    state: {
                        map: 'function(doc) { if (doc.type===\'state\' && (doc.common.custom || doc.common.history)) emit(doc._id, doc.common.custom || doc.common.history) }'
                    }
                }
            };
            adapter.setForeignObject('_design/custom', obj, function (err) {
                if (callback) callback(err);
            });
        } else {
            if (callback) callback(err);
        }
    });
}

function processStartValues() {
    if (tasksStart && tasksStart.length) {
        var task = tasksStart.shift();
        if (sqlDPs[task.id][adapter.namespace].changesOnly) {
            adapter.getForeignState(task.id, function (err, state) {
                var now = task.now || new Date().getTime();
                pushHistory(task.id, {
                    val:  null,
                    ts:   state ? now - 4 : now, // 4 is because of MS SQL
                    ack:  true,
                    q:    0x40,
                    from: 'system.adapter.' + adapter.namespace
                });
                if (state) {
                    state.ts = now;
                    state.from = 'system.adapter.' + adapter.namespace;
                    pushHistory(task.id, state);
                }
                setTimeout(processStartValues, 0);
            });
        } else {
            pushHistory(task.id, {
                val:  null,
                ts:   task.now || new Date().getTime(),
                ack:  true,
                q:    0x40,
                from: 'system.adapter.' + adapter.namespace
            });
            setTimeout(processStartValues, 0);
        }
    }
}

function writeNulls(id, now) {
    if (!id) {
        now = new Date().getTime();
        for (var _id in sqlDPs) {
            if (sqlDPs.hasOwnProperty(_id) && sqlDPs[_id] && sqlDPs[_id][adapter.namespace]) {
                writeNulls(_id, now);
            }
        }
    } else {
        now = now || new Date().getTime();
        tasksStart.push({id: id, now: now});
        if (tasksStart.length === 1 && connected) {
            processStartValues();
        }
        if (sqlDPs[id][adapter.namespace] && sqlDPs[id][adapter.namespace].changesRelogInterval > 0) {
            if (sqlDPs[id].relogTimeout) clearTimeout(sqlDPs[id].relogTimeout);
            sqlDPs[id].relogTimeout = setTimeout(reLogHelper, (sqlDPs[id][adapter.namespace].changesRelogInterval * 500 * Math.random()) + sqlDPs[id][adapter.namespace].changesRelogInterval * 500, id);
        }
    }
}

function main() {
    setConnected(false);

    adapter.config.dbname = adapter.config.dbname || 'iobroker';

    adapter.config.retention = parseInt(adapter.config.retention, 10) || 0;
    adapter.config.debounce  = parseInt(adapter.config.debounce,  10) || 0;
    adapter.config.requestInterval = (adapter.config.requestInterval === undefined || adapter.config.requestInterval === null  || adapter.config.requestInterval === '') ? 0 : parseInt(adapter.config.requestInterval, 10) || 0;

    if (adapter.config.changesRelogInterval !== null && adapter.config.changesRelogInterval !== undefined) {
        adapter.config.changesRelogInterval = parseInt(adapter.config.changesRelogInterval, 10);
    } else {
        adapter.config.changesRelogInterval = 0;
    }

    if (!clients[adapter.config.dbtype]) {
        adapter.log.error('Unknown DB type: ' + adapter.config.dbtype);
        adapter.stop();
    }
    if (adapter.config.multiRequests !== undefined && adapter.config.dbtype !== 'SQLite3Client' && adapter.config.dbtype !== 'sqlite') {
        clients[adapter.config.dbtype].multiRequests = adapter.config.multiRequests;
    }

    if (adapter.config.changesMinDelta !== null && adapter.config.changesMinDelta !== undefined) {
        adapter.config.changesMinDelta = parseFloat(adapter.config.changesMinDelta.toString().replace(/,/g, '.'));
    } else {
        adapter.config.changesMinDelta = 0;
    }

    multiRequests = clients[adapter.config.dbtype].multiRequests;

    adapter.config.port = parseInt(adapter.config.port, 10) || 0;
    if (adapter.config.round !== null && adapter.config.round !== undefined) {
        adapter.config.round = Math.pow(10, parseInt(adapter.config.round, 10));
    } else {
        adapter.config.round = null;
    }
    if (adapter.config.dbtype === 'postgresql' && !SQL.PostgreSQLClient) {
        var postgres = require(__dirname + '/lib/postgresql-client');
        for (var attr in postgres) {
            if (postgres.hasOwnProperty(attr) && !SQL[attr]) {
                SQL[attr] = postgres[attr];
            }
        }
    } else
    if (adapter.config.dbtype === 'mssql' && !SQL.MSSQLClient) {
        var mssql = require(__dirname + '/lib/mssql-client');
        for (var attr_ in mssql) {
            if (mssql.hasOwnProperty(attr_) && !SQL[attr_]) {
                SQL[attr_] = mssql[attr_];
            }
        }
    }
    SQLFuncs = require(__dirname + '/lib/' + adapter.config.dbtype);

    fixSelector(function () {
        // read all custom settings
        adapter.objects.getObjectView('custom', 'state', {}, function (err, doc) {
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
                            if (sqlDPs[id][adapter.namespace].retention !== undefined && sqlDPs[id][adapter.namespace].retention !== null && sqlDPs[id][adapter.namespace].retention !== '') {
                                sqlDPs[id][adapter.namespace].retention = parseInt(sqlDPs[id][adapter.namespace].retention || adapter.config.retention, 10) || 0;
                            } else {
                                sqlDPs[id][adapter.namespace].retention = adapter.config.retention;
                            }

                            if (sqlDPs[id][adapter.namespace].debounce !== undefined && sqlDPs[id][adapter.namespace].debounce !== null && sqlDPs[id][adapter.namespace].debounce !== '') {
                                sqlDPs[id][adapter.namespace].debounce = parseInt(sqlDPs[id][adapter.namespace].debounce, 10) || 0;
                            } else {
                                sqlDPs[id][adapter.namespace].debounce = adapter.config.debounce;
                            }
                            sqlDPs[id][adapter.namespace].changesOnly = sqlDPs[id][adapter.namespace].changesOnly === 'true' || sqlDPs[id][adapter.namespace].changesOnly === true;

                            if (sqlDPs[id][adapter.namespace].changesRelogInterval !== undefined && sqlDPs[id][adapter.namespace].changesRelogInterval !== null && sqlDPs[id][adapter.namespace].changesRelogInterval !== '') {
                                sqlDPs[id][adapter.namespace].changesRelogInterval = parseInt(sqlDPs[id][adapter.namespace].changesRelogInterval, 10) || 0;
                            } else {
                                sqlDPs[id][adapter.namespace].changesRelogInterval = adapter.config.changesRelogInterval;
                            }
                            if (sqlDPs[id][adapter.namespace].changesRelogInterval > 0) {
                                sqlDPs[id].relogTimeout = setTimeout(reLogHelper, (sqlDPs[id][adapter.namespace].changesRelogInterval * 500 * Math.random()) + sqlDPs[id][adapter.namespace].changesRelogInterval * 500, id);
                            }
                            if (sqlDPs[id][adapter.namespace].changesMinDelta !== undefined && sqlDPs[id][adapter.namespace].changesMinDelta !== null && sqlDPs[id][adapter.namespace].changesMinDelta !== '') {
                                sqlDPs[id][adapter.namespace].changesMinDelta = parseFloat(sqlDPs[id][adapter.namespace].changesMinDelta) || 0;
                            } else {
                                sqlDPs[id][adapter.namespace].changesMinDelta = adapter.config.changesMinDelta;
                            }
                            if (!sqlDPs[id][adapter.namespace].storageType) sqlDPs[id][adapter.namespace].storageType = false;

                            // add one day if retention is too small
                            if (sqlDPs[id][adapter.namespace].retention && sqlDPs[id][adapter.namespace].retention <= 604800) {
                                sqlDPs[id][adapter.namespace].retention += 86400;
                            }
                        }
                    }
                }
            }

            writeNulls();

            if (count < 20) {
                for (var _id in sqlDPs) {
                    if (sqlDPs.hasOwnProperty(_id) && sqlDPs[_id] && sqlDPs[_id][adapter.namespace]) {
                        adapter.subscribeForeignStates(_id);
                    }
                }
            } else {
                subscribeAll = true;
                adapter.subscribeForeignStates('*');
            }
        });
    });

    adapter.subscribeForeignObjects('*');

    if (adapter.config.dbtype === 'sqlite' || adapter.config.host) {
        connect();
    }
}

function pushHistory(id, state, timerRelog) {
    if (timerRelog === undefined) timerRelog = false;
    // Push into DB
    if (sqlDPs[id]) {
        var settings = sqlDPs[id][adapter.namespace];

        if (!settings || !state) return;

        if (state.val !== null && typeof state.val === 'string' && settings.storageType !== 'String') {
            var f = parseFloat(state.val);
            if (f == state.val) {
                state.val = f;
            }
        }

        if (sqlDPs[id].state && settings.changesOnly && !timerRelog) {
            if (settings.changesRelogInterval === 0) {
                if (state.ts !== state.lc) {
                    sqlDPs[id].skipped = state; // remember new timestamp
                    adapter.log.debug('value not changed ' + id + ', last-value=' + sqlDPs[id].state.val + ', new-value=' + state.val + ', ts=' + state.ts);
                    return;
                }
            }
            else if (sqlDPs[id].lastLogTime) {
                if ((state.ts !== state.lc) && (Math.abs(sqlDPs[id].lastLogTime - state.ts) < settings.changesRelogInterval * 1000)) {
                    sqlDPs[id].skipped = state; // remember new timestamp
                    adapter.log.debug('value not changed ' + id + ', last-value=' + sqlDPs[id].state.val + ', new-value=' + state.val + ', ts=' + state.ts);
                    return;
                }
                if (state.ts !== state.lc) {
                    adapter.log.debug('value-changed-relog ' + id + ', value=' + state.val + ', lastLogTime=' + sqlDPs[id].lastLogTime + ', ts=' + state.ts);
                }
            }
            if (sqlDPs[id].state.val !== null && (settings.changesMinDelta !== 0) && (typeof state.val === 'number') && (Math.abs(sqlDPs[id].state.val - state.val) < settings.changesMinDelta)) {
                adapter.log.debug('Min-Delta not reached ' + id + ', last-value=' + sqlDPs[id].state.val + ', new-value=' + state.val + ', ts=' + state.ts);
                sqlDPs[id].skipped = state; // remember new timestamp
                return;
            }
            else if (typeof state.val === 'number') {
                adapter.log.debug('Min-Delta reached ' + id + ', last-value=' + sqlDPs[id].state.val + ', new-value=' + state.val + ', ts=' + state.ts);
            }
            else {
                adapter.log.debug('Min-Delta ignored because no number ' + id + ', last-value=' + sqlDPs[id].state.val + ', new-value=' + state.val + ', ts=' + state.ts);
            }
        }

        if (sqlDPs[id].relogTimeout) {
            clearTimeout(sqlDPs[id].relogTimeout);
            sqlDPs[id].relogTimeout = null;
        }
        if (settings.changesRelogInterval > 0) {
            sqlDPs[id].relogTimeout = setTimeout(reLogHelper, settings.changesRelogInterval * 1000, id);
        }

        var ignoreDebonce = false;
        if (timerRelog) {
            state.ts = new Date().getTime();
            adapter.log.debug('timed-relog ' + id + ', value=' + state.val + ', lastLogTime=' + sqlDPs[id].lastLogTime + ', ts=' + state.ts);
            ignoreDebonce = true;
        } else {
            if (settings.changesOnly && sqlDPs[id].skipped) {
                sqlDPs[id].state = sqlDPs[id].skipped;
                pushHelper(id);
            }
            if (sqlDPs[id].state && ((sqlDPs[id].state.val === null && state.val !== null) || (sqlDPs[id].state.val !== null && state.val === null))) {
                ignoreDebonce = true;
            } else if (!sqlDPs[id].state && state.val === null) {
                ignoreDebonce = true;
            }

            // only store state if really changed
            sqlDPs[id].state = state;
        }
        sqlDPs[id].lastLogTime = state.ts;
        sqlDPs[id].skipped = null;

        if (settings.debounce && !ignoreDebonce) {
            // Discard changes in debounce time to store last stable value
            if (sqlDPs[id].timeout) clearTimeout(sqlDPs[id].timeout);
            sqlDPs[id].timeout = setTimeout(pushHelper, settings.debounce, id);
        } else {
            pushHelper(id);
        }
    }
}

function reLogHelper(_id) {
    if (!sqlDPs[_id]) {
        adapter.log.info('non-existing id ' + _id);
        return;
    }
    sqlDPs[_id].relogTimeout = null;
    if (sqlDPs[_id].skipped) {
        sqlDPs[_id].state = sqlDPs[_id].skipped;
        sqlDPs[_id].state.from = 'system.adapter.' + adapter.namespace;
        sqlDPs[_id].skipped = null;
        pushHistory(_id, sqlDPs[_id].state, true);
    }
    else {
        adapter.getForeignState(_id, function (err, state) {
            if (err) {
                adapter.log.info('init timed Relog: can not get State for ' + _id + ' : ' + err);
            }
            else if (!state) {
                adapter.log.info('init timed Relog: disable relog because state not set so far for ' + _id + ': ' + JSON.stringify(state));
            }
            else {
                adapter.log.debug('init timed Relog: getState ' + _id + ':  Value=' + state.val + ', ack=' + state.ack + ', ts=' + state.ts  + ', lc=' + state.lc);
                sqlDPs[_id].state = state;
                pushHistory(_id, sqlDPs[_id].state, true);
            }
        });
    }
}

function pushHelper(_id) {
    if (!sqlDPs[_id] || !sqlDPs[_id].state) return;
    var _settings = sqlDPs[_id][adapter.namespace];
    // if it was not deleted in this time
    if (_settings) {
        sqlDPs[_id].timeout = null;

        if (sqlDPs[_id].state.val !== null) {
            if (typeof sqlDPs[_id].state.val === 'object') {
                sqlDPs[_id].state.val = JSON.stringify(sqlDPs[_id].state.val);
            }

            adapter.log.debug('Datatype ' + _id + ': Currently: ' + typeof sqlDPs[_id].state.val + ', StorageType: ' + _settings.storageType);
            if (typeof sqlDPs[_id].state.val === 'string' && _settings.storageType !== 'String') {
                adapter.log.debug('Do Automatic Datatype conversion for ' + _id);
                var f = parseFloat(sqlDPs[_id].state.val);
                if (f == sqlDPs[_id].state.val) {
                    sqlDPs[_id].state.val = f;
                } else if (sqlDPs[_id].state.val === 'true') {
                    sqlDPs[_id].state.val = true;
                } else if (sqlDPs[_id].state.val === 'false') {
                    sqlDPs[_id].state.val = false;
                }
            }
            if (_settings.storageType === 'String' && typeof sqlDPs[_id].state.val !== 'string') {
                sqlDPs[_id].state.val = sqlDPs[_id].state.val.toString();
            }
            else if (_settings.storageType === 'Number' && typeof sqlDPs[_id].state.val !== 'number') {
                if (typeof sqlDPs[_id].state.val === 'boolean') {
                    sqlDPs[_id].state.val = sqlDPs[_id].state.val?1:0;
                }
                else {
                    adapter.log.info('Do not store value "' + sqlDPs[_id].state.val + '" for ' + _id + ' because no number');
                    return;
                }
            }
            else if (_settings.storageType === 'Boolean' && typeof sqlDPs[_id].state.val !== 'boolean') {
                sqlDPs[_id].state.val = !!sqlDPs[_id].state.val;
            }
        }
        else {
            adapter.log.debug('Datatype ' + _id + ': Currently: null');
        }
        pushValueIntoDB(_id, sqlDPs[_id].state);
    }
}

function getAllIds(cb) {
    var query = SQLFuncs.getIdSelect(adapter.config.dbname);
    adapter.log.debug(query);
    clientPool.borrow(function (err, client) {
        if (err) {
            if (cb) cb(err);
            return;
        }
        client.execute(query, function (err, rows /* , fields */) {
            if (rows && rows.rows) rows = rows.rows;
            if (err) {
                adapter.log.error('Cannot select ' + query + ': ' + err);
                if (cb) cb(err);
                clientPool.return(client);
                return;
            }
            if (rows.length) {
                var id;
                for (var r = 0; r < rows.length; r++) {
                    id = rows[r].name;
                    sqlDPs[id] = sqlDPs[id] || {};
                    sqlDPs[id].index = rows[r].id;
                    sqlDPs[id].dbtype  = rows[r].type;
                }

                if (cb) cb();
                clientPool.return(client);
            }
        });
    });
}

function getAllFroms(cb) {
    var query = SQLFuncs.getFromSelect(adapter.config.dbname);
    adapter.log.debug(query);
    clientPool.borrow(function (err, client) {
        if (err) {
            if (cb) cb(err);
            return;
        }
        client.execute(query, function (err, rows /* , fields */) {
            if (rows && rows.rows) rows = rows.rows;
            if (err) {
                adapter.log.error('Cannot select ' + query + ': ' + err);
                if (cb) cb(err);
                clientPool.return(client);
                return;
            }
            if (rows.length) {
                for (var r = 0; r < rows.length; r++) {
                    from[rows[r].name] = rows[r].id;
                }

                if (cb) cb();
                clientPool.return(client);
            }
        });
    });
}

function _checkRetention(query, cb) {
    adapter.log.debug(query);

    clientPool.borrow(function (err, client) {
        if (err) {
            adapter.log.error(err);
            if (cb) cb();
            return;
        }
        client.execute(query, function (err /* , rows, fields */ ) {
            if (err) adapter.log.error('Cannot delete ' + query + ': ' + err);
            clientPool.return(client);
            if (cb) cb();
        });
    });
}

function checkRetention(id) {
    if (sqlDPs[id] && sqlDPs[id][adapter.namespace] && sqlDPs[id][adapter.namespace].retention) {
        var d = new Date();
        var dt = d.getTime();
        // check every 6 hours
        if (!sqlDPs[id].lastCheck || dt - sqlDPs[id].lastCheck >= 21600000/* 6 hours */) {
            sqlDPs[id].lastCheck = dt;
            var query = SQLFuncs.retention(adapter.config.dbname, sqlDPs[id].index, dbNames[sqlDPs[id].type], sqlDPs[id][adapter.namespace].retention);

            if (!multiRequests) {
                if (tasks.length > 100) {
                    adapter.log.error('Cannot queue new requests, because more than 100');
                    return;
                }
                tasks.push({operation: 'delete', query: query});
                if (tasks.length === 1) processTasks();
            } else {
                _checkRetention(query);
            }
        }
    }
}

function _insertValueIntoDB(query, id, cb) {
    adapter.log.debug(query);

    clientPool.borrow(function (err, client) {
        if (err) {
            adapter.log.error(err);
            if (cb) cb();
            return;
        }
        client.execute(query, function (err /* , rows, fields */) {
            if (err) adapter.log.error('Cannot insert ' + query + ': ' + err);
            clientPool.return(client);
            checkRetention(id);
            if (cb) cb();
        });
    });
}

function processReadTypes() {
    if (tasksReadType && tasksReadType.length) {
        var task = tasksReadType.shift();
        adapter.log.debug('Type set in Def for ' + task.id + ': ' + sqlDPs[task.id][adapter.namespace].storageType);
        if (sqlDPs[task.id][adapter.namespace].storageType) {
            sqlDPs[task.id].type = types[sqlDPs[task.id][adapter.namespace].storageType.toLowerCase()];
            adapter.log.debug('Type (from Def) for ' + task.id + ': ' + sqlDPs[task.id].type);
            processVerifyTypes(task);
        }
        else if (sqlDPs[task.id].dbtype !== undefined) {
            sqlDPs[task.id].type = sqlDPs[task.id].dbtype;
            sqlDPs[task.id][adapter.namespace].storageType = storageTypes[sqlDPs[task.id].type];
            adapter.log.debug('Type (from DB-Type) for ' + task.id + ': ' + sqlDPs[task.id].type);
            processVerifyTypes(task);
        }
        else {
            adapter.getForeignObject(task.id, function (err, obj) {
                if (obj && obj.common && obj.common.type) {
                    sqlDPs[task.id].type = types[obj.common.type.toLowerCase()];
                } else {
                    sqlDPs[task.id].type = 1; // string
                }
                sqlDPs[task.id][adapter.namespace].storageType = storageTypes[sqlDPs[task.id].type];
                adapter.log.debug('Type (from Obj) for ' + task.id + ': ' + sqlDPs[task.id].type);
                processVerifyTypes(task);
            });
        }
    }
}

function processVerifyTypes(task) {
    if (sqlDPs[task.id].dbtype !== undefined && sqlDPs[task.id].type !== sqlDPs[task.id].dbtype) {
        sqlDPs[task.id].dbtype = sqlDPs[task.id].type;

        var query = SQLFuncs.getIdUpdate(adapter.config.dbname, sqlDPs[task.id].index, sqlDPs[task.id].type);
        adapter.log.debug(query);
        clientPool.borrow(function (err, client) {
            if (err) {
                processVerifyTypes(task);
                return;
            }
            client.execute(query, function (err, rows /* , fields */) {
                if (err) {
                    adapter.log.error('error updating history config for ' + task.id + ' to pin datatype: ' + query + ': ' + err);
                }
                else {
                    adapter.log.info('changed history configuration to pin detected datatype for ' + task.id);
                }
                clientPool.return(client);
                processVerifyTypes(task);
            });
        });

        return;
    }

    pushValueIntoDB(task.id, task.state);

    setTimeout(function () {
        processReadTypes();
    }, 50);
}

function pushValueIntoDB(id, state, cb) {
    if (!sqlDPs[id]) return;
    if (!clientPool) {
        adapter.log.warn('No connection to SQL-DB');
        if (cb) cb('No connection to SQL-DB');
        return;
    }
    var type;

    if (sqlDPs[id].type !== undefined) {
        type = sqlDPs[id].type;
    }
    else {
        // read type from DB
        tasksReadType.push({id: id, state: state});
        if (tasksReadType.length === 1) {
            processReadTypes();
        }

        return;
    }

    if (type === undefined) { // Can not happen anymore
        if (state.val === null) {
            adapter.log.warn('Ignore null value for ' + id + ' because no type defined till now.');
            if (cb) cb('Ignore null value for ' + id + ' because no type defined till now.');
            return;
        }
        adapter.log.warn('Cannot store values of type "' + typeof state.val + '" for ' + id);
        if (cb) cb('Cannot store values of type "' + typeof state.val + '" ' + id);
        return;
    }
    var tmpState;
    // get id if state
    if (sqlDPs[id].index === undefined) {
        sqlDPs[id].isRunning = sqlDPs[id].isRunning || [];
        if (Object.assign) {
            tmpState = Object.assign({}, state);
        }
        else {
            tmpState = JSON.parse(JSON.stringify(state));
        }
        sqlDPs[id].isRunning.push({id: id, state: tmpState, cb: cb});

        if (sqlDPs[id].isRunning.length === 1) {
            // read or create in DB
            return getId(id, type, function (err, _id) {
                if (err) {
                    adapter.log.warn('Cannot get index of "' + _id + '": ' + err);
                    if (sqlDPs[_id].isRunning) {
                        for (var t = 0; t < sqlDPs[_id].isRunning.length; t++) {
                            if (sqlDPs[_id].isRunning[t].cb) sqlDPs[_id].isRunning[t].cb('Cannot get index of "' + sqlDPs[_id].isRunning[t].id + '": ' + err);
                        }
                    }
                } else {
                    if (sqlDPs[_id].isRunning) {
                        for (var k = 0; k < sqlDPs[_id].isRunning.length; k++) {
                            pushValueIntoDB(sqlDPs[_id].isRunning[k].id, sqlDPs[_id].isRunning[k].state, sqlDPs[_id].isRunning[k].cb);
                        }
                    }
                }
                sqlDPs[_id].isRunning = null;
            });
        }
        return;
    }

    // get from
    if (state.from && !from[state.from]) {
        isFromRunning[state.from] = isFromRunning[state.from] || [];
        if (Object.assign) {
            tmpState = Object.assign({}, state);
        }
        else {
            tmpState = JSON.parse(JSON.stringify(state));
        }
        isFromRunning[state.from].push({id: id, state: tmpState, cb: cb});

        if (isFromRunning[state.from].length === 1) {
            // read or create in DB
            return getFrom(state.from, function (err, from) {
                if (err) {
                    adapter.log.warn('Cannot get "from" for "' + from + '": ' + err);
                    if (isFromRunning[from]) {
                        for (var t = 0; t < isFromRunning[from].length; t++) {
                            if (isFromRunning[from][t].cb) isFromRunning[from][t].cb('Cannot get "from" for "' + from + '": ' + err);
                        }
                    }
                } else {
                    if (isFromRunning[from]) {
                        for (var k = 0; k < isFromRunning[from].length; k++) {
                            pushValueIntoDB(isFromRunning[from][k].id, isFromRunning[from][k].state, isFromRunning[from][k].cb);
                        }
                    }
                }
                isFromRunning[from] = null;
            });
        }
        return;
    }
    // if greater than 2000.01.01 00:00:00
    if (state.ts > 946681200000) {
        state.ts = parseInt(state.ts, 10);
    } else {
        state.ts = parseInt(state.ts, 10) * 1000 + (parseInt(state.ms, 10) || 0);
    }

    try {
        if (state.val !== null && typeof state.val === 'object') {
            state.val = JSON.stringify(state.val);
        }
    } catch (err) {
        adapter.log.error('Cannot convert the object value "' + id + '"');
        if (cb) cb('Cannot convert the object value "' + id + '"');
        return;
    }

    // increase timestamp if last is the same
    if (sqlDPs[id].ts && state.ts === sqlDPs[id].ts) {
        state.ts++;
    }
    // remember last timestamp
    sqlDPs[id].ts = state.ts;

    var query = SQLFuncs.insert(adapter.config.dbname, sqlDPs[id].index, state, from[state.from] || 0, dbNames[type]);
    if (!multiRequests) {
        if (tasks.length > 100) {
            adapter.log.error('Cannot queue new requests, because more than 100');
            if (cb) cb('Cannot queue new requests, because more than 100');
            return;
        }

        tasks.push({operation: 'insert', query: query, id: id, callback: cb});
        if (tasks.length === 1) {
            processTasks();
        }
    } else {
        _insertValueIntoDB(query, id, cb);
    }
}

var lockTasks = false;
function processTasks() {
    if (lockTasks) {
        adapter.log.debug('Tries to execute task, but last one not finished!');
        return;
    }
    lockTasks = true;
    if (tasks.length) {
        if (tasks[0].operation === 'insert') {
            _insertValueIntoDB(tasks[0].query, tasks[0].id, function () {
                if (tasks[0].callback) tasks[0].callback();
                tasks.shift();
                lockTasks = false;
                if (tasks.length) setTimeout(processTasks, adapter.config.requestInterval);
            });
        }
        else if (tasks[0].operation === 'select') {
            _getDataFromDB(tasks[0].query, tasks[0].options, function (err, rows) {
                if (tasks[0].callback) tasks[0].callback(err, rows);
                tasks.shift();
                lockTasks = false;
                if (tasks.length) setTimeout(processTasks, adapter.config.requestInterval);
            });
        }
        else if (tasks[0].operation === 'userQuery') {
            _userQuery(tasks[0].msg, function () {
                if (tasks[0].callback) tasks[0].callback();
                tasks.shift();
                lockTasks = false;
                if (tasks.length) setTimeout(processTasks, adapter.config.requestInterval);
            });
        }
        else if (tasks[0].operation === 'delete') {
            _checkRetention(tasks[0].query, function () {
                if (tasks[0].callback) tasks[0].callback();
                tasks.shift();
                lockTasks = false;
                if (tasks.length) setTimeout(processTasks, adapter.config.requestInterval);
            });
        } else {
            adapter.log.error('unknown task: ' + tasks[0].operation);
            if (tasks[0].callback) tasks[0].callback();
            tasks.shift();
            lockTasks = false;
            if (tasks.length) setTimeout(processTasks, adapter.config.requestInterval);
        }
    }
}
// my be it is required to cache all the data in memory
function getId(id, type, cb) {
    var query = SQLFuncs.getIdSelect(adapter.config.dbname, id);
    adapter.log.debug(query);

    if (!clientPool) {
        if (cb) cb('No connection', id);
        return;
    }

    clientPool.borrow(function (err, client) {
        if (err) {
            if (cb) cb(err, id);
            return;
        }
        client.execute(query, function (err, rows /* , fields */) {
            if (rows && rows.rows) rows = rows.rows;
            if (err) {
                adapter.log.error('Cannot select ' + query + ': ' + err);
                if (cb) cb(err, id);
                clientPool.return(client);
                return;
            }
            if (!rows.length) {
                if (type !== null) {
                    // insert
                    query = SQLFuncs.getIdInsert(adapter.config.dbname, id, type);
                    adapter.log.debug(query);
                    client.execute(query, function (err /* , rows, fields */) {
                        if (err) {
                            adapter.log.error('Cannot insert ' + query + ': ' + err);
                            if (cb) cb(err, id);
                            clientPool.return(client);
                            return;
                        }
                        query = SQLFuncs.getIdSelect(adapter.config.dbname,id);
                        adapter.log.debug(query);
                        client.execute(query, function (err, rows /* , fields */) {
                            if (rows && rows.rows) {
                                rows = rows.rows;
                            }
                            if (err) {
                                adapter.log.error('Cannot select ' + query + ': ' + err);
                                if (cb) cb(err, id);
                                clientPool.return(client);
                                return;
                            }
                            sqlDPs[id].index = rows[0].id;
                            sqlDPs[id].type  = rows[0].type;

                            if (cb) cb(null, id);
                            clientPool.return(client);
                        });
                    });
                } else {
                    if (cb) cb('id not found', id);
                    clientPool.return(client);
                }
            } else {
                sqlDPs[id].index = rows[0].id;
                sqlDPs[id].type  = rows[0].type;

                if (cb) cb(null, id);
                clientPool.return(client);
            }
        });
    });
}
// my be it is required to cache all the data in memory
function getFrom(_from, cb) {
    // var sources    = (adapter.config.dbtype !== 'postgresql' ? (adapter.config.dbname + '.') : '') + 'sources';
    var query = SQLFuncs.getFromSelect(adapter.config.dbname, _from);
    adapter.log.debug(query);

    if (!clientPool) {
        if (cb) cb('No connection', _from);
        return;
    }

    clientPool.borrow(function (err, client) {
        if (err) {
            if (cb) cb(err, _from);
            return;
        }
        client.execute(query, function (err, rows /* , fields */) {
            if (rows && rows.rows) rows = rows.rows;
            if (err) {
                adapter.log.error('Cannot select ' + query + ': ' + err);
                if (cb) cb(err, _from);
                clientPool.return(client);
                return;
            }
            if (!rows.length) {
                // insert
                query = SQLFuncs.getFromInsert(adapter.config.dbname, _from);
                adapter.log.debug(query);
                client.execute(query, function (err /* , rows, fields */) {
                    if (err) {
                        adapter.log.error('Cannot insert ' + query + ': ' + err);
                        if (cb) cb(err, _from);
                        clientPool.return(client);
                        return;
                    }

                    query = SQLFuncs.getFromSelect(adapter.config.dbname, _from);
                    adapter.log.debug(query);
                    client.execute(query, function (err, rows /* , fields */) {
                        if (rows && rows.rows) rows = rows.rows;
                        if (err) {
                            adapter.log.error('Cannot select ' + query + ': ' + err);
                            if (cb) cb(err, _from);
                            clientPool.return(client);
                            return;
                        }
                        from[_from] = rows[0].id;

                        if (cb) cb(null, _from);
                        clientPool.return(client);
                    });
                });
            } else {
                from[_from] = rows[0].id;

                if (cb) cb(null, _from);
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

function _getDataFromDB(query, options, callback) {
    adapter.log.debug(query);

    clientPool.borrow(function (err, client) {
        if (err) {
            if (callback) callback(err);
            return;
        }
        client.execute(query, function (err, rows /* , fields */) {
            if (rows && rows.rows) rows = rows.rows;
            // because descending
            if (!err && rows && !options.start && options.count) {
                rows.sort(sortByTs);
            }

            if (rows) {
                var isNumber = null;
                for (var c = 0; c < rows.length; c++) {
                    if (isNumber === null && rows[c].val !== null) {
                        isNumber = (parseFloat(rows[c].val) == rows[c].val);
                    }
                    if (typeof rows[c].ts === 'string') {
                        rows[c].ts = parseInt(rows[c].ts, 10);
                    }

                    // if less than 2000.01.01 00:00:00
                    if (rows[c].ts < 946681200000) {
                        rows[c].ts *= 1000;
                    }

                    if (adapter.common.loglevel === 'debug') {
                        rows[c].date = new Date(parseInt(rows[c].ts, 10));
                    }
                    if (options.ack) {
                        rows[c].ack = !!rows[c].ack;
                    }
                    if (isNumber && adapter.config.round && rows[c].val !== null) {
                        rows[c].val = Math.round(rows[c].val * adapter.config.round) / adapter.config.round;
                    }
                    if (sqlDPs[options.index].type === 2) {
                        rows[c].val = !!rows[c].val;
                    }
                }
            }

            clientPool.return(client);
            if (callback) callback(err, rows);
        });
    });
}

function getDataFromDB(db, options, callback) {
    var query = SQLFuncs.getHistory(adapter.config.dbname, db, options);
    adapter.log.debug(query);
    if (!multiRequests) {
        if (tasks.length > 100) {
            adapter.log.error('Cannot queue new requests, because more than 100');
            if (callback) callback('Cannot queue new requests, because more than 100');
            return;
        }
        tasks.push({operation: 'select', query: query, options: options, callback: callback});
        if (tasks.length === 1) processTasks();
    } else {
        _getDataFromDB(query, options, callback);
    }
}

function getHistory(msg) {
    var startTime = new Date().getTime();

    var options = {
        id:         msg.message.id === '*' ? null : msg.message.id,
        start:      msg.message.options.start,
        end:        msg.message.options.end || ((new Date()).getTime() + 5000000),
        step:       parseInt(msg.message.options.step, 10)  || null,
        count:      parseInt(msg.message.options.count, 10) || 500,
        ignoreNull: msg.message.options.ignoreNull,
        aggregate:  msg.message.options.aggregate || 'average', // One of: max, min, average, total
        limit:      msg.message.options.limit || adapter.config.limit || 2000,
        from:       msg.message.options.from  || false,
        q:          msg.message.options.q     || false,
        ack:        msg.message.options.ack   || false,
        ms:         msg.message.options.ms    || false,
        addId:      msg.message.options.addId || false,
        sessionId:  msg.message.options.sessionId
    };

    if (options.ignoreNull === 'true')  options.ignoreNull = true;  // include nulls and replace them with last value
    if (options.ignoreNull === 'false') options.ignoreNull = false; // include nulls
    if (options.ignoreNull === '0')     options.ignoreNull = 0;     // include nulls and replace them with 0
    if (options.ignoreNull !== true && options.ignoreNull !== false && options.ignoreNull !== 0) options.ignoreNull = false;

    if (!sqlDPs[options.id]) {
        commons.sendResponse(adapter, msg, options, [], startTime);
        return;
    }

    if (options.start > options.end) {
        var _end = options.end;
        options.end   = options.start;
        options.start =_end;
    }

    if (!options.start && !options.count) {
        options.start = (new Date()).getTime() - 5030000; // - 1 year
    }

    if (sqlDPs[options.id].type === undefined && sqlDPs[options.id].dbtype !== undefined) {
        if (sqlDPs[options.id][adapter.namespace] && sqlDPs[options.id][adapter.namespace].storageType) {
            if (storageTypes.indexOf(sqlDPs[options.id][adapter.namespace].storageType) === sqlDPs[options.id].dbtype) {
                adapter.log.debug('For getHistory for id ' + options.id + ': Type empty, use dbtype ' + sqlDPs[options.id].dbtype);
                sqlDPs[options.id].type = sqlDPs[options.id].dbtype;
            }
        }
    }
    if (sqlDPs[options.id].type === undefined) {
        adapter.log.warn('For getHistory for id ' + options.id + ': Type empty. Need to write data first. Index = ' + sqlDPs[options.id].index);
        commons.sendResponse(adapter, msg, options, 'Please wait till next data record is logged and reload.', startTime);
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
    if (options.id) {
        options.index = options.id;
        options.id = sqlDPs[options.id].index;
    }

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
/*
function generateDemo(msg) {
    var id      = adapter.name +'.' + adapter.instance + '.Demo.' + (msg.message.id || 'Demo_Data');
    var start   = new Date(msg.message.start).getTime();
    var end     = new Date(msg.message.end).getTime();
    var value   = 1;
    var sin     = 0.1;
    var up      = true;
    var curve   = msg.message.curve;
    var step    = (msg.message.step || 60) * 1000;


    if (end < start) {
        var tmp = end;
        end = start;
        start = tmp;
    }

    end = new Date(end).setHours(24);

    function generate() {
        if (curve === 'sin') {
            if (sin === 6.2) {
                sin = 0;
            } else {
                sin = Math.round((sin + 0.1) * 10) / 10;
            }
            value = Math.round(Math.sin(sin) * 10000) / 100;
        } else if (curve === 'dec') {
            value++;
        } else if (curve === 'inc') {
            value--;
        } else {
            if (up) {
                value++;
            } else {
                value--;
            }
        }
        start += step;

        pushValueIntoDB(id, {
            ts:   new Date(start).getTime(),
            val:  value,
            q:    0,
            ack:  true
        });


        if (start <= end) {
            setTimeout(function () {
                generate();
            }, 15);
        } else {
            adapter.sendTo(msg.from, msg.command, 'finished', msg.callback);
        }
    }
    var obj = {
        type: 'state',
        common: {
            name:       msg.message.id,
            type:       'state',
            enabled:    false,
            custom:     {}
        }
    };
    obj.common.custom[adapter.namespace] = {
        enabled:        true,
        changesOnly:    false,
        debounce:       1000,
        retention:      31536000
    };


    adapter.setObject('demo.' + msg.message.id, obj);

    sqlDPs[id] = {};
    sqlDPs[id][adapter.namespace] = obj.common.custom[adapter.namespace];

    generate();
}
*/
function storeState(msg) {
    if (!msg.message || !msg.message.id || !msg.message.state) {
        adapter.log.error('storeState called with invalid data');
        adapter.sendTo(msg.from, msg.command, {
            error:  'Invalid call: ' + JSON.stringify(msg)
        }, msg.callback);
        return;
    }

    if (Array.isArray(msg.message)) {
        for (var i = 0; i < msg.message.length; i++) {
            pushValueIntoDB(msg.message[i].id, msg.message[i].state);
        }
    } else if (Array.isArray(msg.message.state)) {
        for (var j = 0; j < msg.message.state.length; j++) {
            pushValueIntoDB(msg.message.id, msg.message.state[j]);
        }
    } else {
        pushValueIntoDB(msg.message.id, msg.message.state);
    }

    adapter.sendTo(msg.from, msg.command, {
        success:    true,
        connected:  !!clientPool
    }, msg.callback);
}

function getDpOverview(msg) {
    var result = {};
    var query = SQLFuncs.getIdSelect(adapter.config.dbname);
    adapter.log.info(query);
    clientPool.borrow(function (err, client) {
        if (err) {
            adapter.sendTo(msg.from, msg.command, {
                error:  'Cannot select ' + query + ': ' + err
            }, msg.callback);
            return;
        }
        client.execute(query, function (err, rows /* , fields */) {
            if (rows && rows.rows) rows = rows.rows;
            if (err) {
                adapter.log.error('Cannot select ' + query + ': ' + err);
                adapter.sendTo(msg.from, msg.command, {
                    error:  'Cannot select ' + query + ': ' + err
                }, msg.callback);
                clientPool.return(client);
                return;
            }
            adapter.log.info('Query result ' + JSON.stringify(rows));
            if (rows.length) {
                for (var r = 0; r < rows.length; r++) {
                    if (!result[rows[r].type]) result[rows[r].type] = {};
                    result[rows[r].type][rows[r].id] = {};
                    result[rows[r].type][rows[r].id].name = rows[r].name;
                    switch(dbNames[rows[r].type]) {
                        case 'ts_number':   result[rows[r].type][rows[r].id].type = 'number';
                                            break;
                        case 'ts_string':   result[rows[r].type][rows[r].id].type = 'string';
                                            break;
                        case 'ts_bool':     result[rows[r].type][rows[r].id].type = 'boolean';
                                            break;
                    }
                }

                adapter.log.info('inited result: ' + JSON.stringify(result));
                getFirstTsForIds(client, 0, result, msg);
            }
        });
    });

}

function getFirstTsForIds(dbClient, typeId, resultData, msg) {
    if (typeId < dbNames.length) {
        if (!resultData[typeId]) {
            getFirstTsForIds(dbClient, typeId + 1, resultData, msg);
        } else {
            var query = SQLFuncs.getFirstTs(adapter.config.dbname, dbNames[typeId]);
            adapter.log.info(query);
            dbClient.execute(query, function (err, rows /* , fields */) {
                if (rows && rows.rows) rows = rows.rows;
                if (err) {
                    adapter.log.error('Cannot select ' + query + ': ' + err);
                    adapter.sendTo(msg.from, msg.command, {
                        error:  'Cannot select ' + query + ': ' + err
                    }, msg.callback);
                    clientPool.return(dbClient);
                    return;
                }
                adapter.log.info('Query result ' + JSON.stringify(rows));
                if (rows.length) {
                    for (var r = 0; r < rows.length; r++) {
                        if (resultData[typeId][rows[r].id]) {
                            resultData[typeId][rows[r].id].ts = rows[r].ts;
                        }
                    }
                }
                adapter.log.info('enhanced result (' + typeId + '): ' + JSON.stringify(resultData));
                setTimeout(getFirstTsForIds, 5000, dbClient, typeId + 1, resultData, msg);
            });
        }
    } else {
        clientPool.return(dbClient);
        adapter.log.info('consolidate data ...');
        var result = {};
        for (var ti = 0; ti < dbNames.length; ti++ ) {
            if (resultData[ti]) {
                for (var index in resultData[ti]) {
                    if (!resultData[ti].hasOwnProperty(index)) continue;

                    var id = resultData[ti][index].name;
                    if (!result[id]) {
                        result[id] = {};
                        result[id].type = resultData[ti][index].type;
                        result[id].ts = resultData[ti][index].ts;
                    } else {
                        result[id].type = 'undefined';
                        if (resultData[ti][index].ts < result[id].ts) {
                            result[id].ts = resultData[ti][index].ts;
                        }
                    }
                }
            }
        }
        adapter.log.info('Result: ' + JSON.stringify(result));
        adapter.sendTo(msg.from, msg.command, {
            succes: true,
            result: result
        }, msg.callback);
    }
}

function enableHistory(msg) {
    if (!msg.message || !msg.message.id) {
        adapter.log.error('enableHistory called with invalid data');
        adapter.sendTo(msg.from, msg.command, {
            error:  'Invalid call'
        }, msg.callback);
        return;
    }
    var obj = {};
    obj.common = {};
    obj.common.custom = {};
    if (msg.message.options) {
        obj.common.custom[adapter.namespace] = msg.message.options;
    }
    else {
        obj.common.custom[adapter.namespace] = {};
    }
    obj.common.custom[adapter.namespace].enabled = true;
    adapter.extendForeignObject(msg.message.id, obj, function (err) {
        if (err) {
            adapter.log.error('enableHistory: ' + err);
            adapter.sendTo(msg.from, msg.command, {
                error:  err
            }, msg.callback);
        } else {
            adapter.log.info(JSON.stringify(obj));
            adapter.sendTo(msg.from, msg.command, {
                success:                  true
            }, msg.callback);
        }
    });
}

function disableHistory(msg) {
    if (!msg.message || !msg.message.id) {
        adapter.log.error('disableHistory called with invalid data');
        adapter.sendTo(msg.from, msg.command, {
            error:  'Invalid call'
        }, msg.callback);
        return;
    }
    var obj = {};
    obj.common = {};
    obj.common.custom = {};
    obj.common.custom[adapter.namespace] = {};
    obj.common.custom[adapter.namespace].enabled = false;
    adapter.extendForeignObject(msg.message.id, obj, function (err) {
        if (err) {
            adapter.log.error('disableHistory: ' + err);
            adapter.sendTo(msg.from, msg.command, {
                error:  err
            }, msg.callback);
        } else {
            adapter.log.info(JSON.stringify(obj));
            adapter.sendTo(msg.from, msg.command, {
                success:                  true
            }, msg.callback);
        }
    });
}

function getEnabledDPs(msg) {
    var data = {};
    for (var id in sqlDPs) {
        if (sqlDPs.hasOwnProperty(id) && sqlDPs[id] && sqlDPs[id][adapter.namespace]) {
            data[id] = sqlDPs[id][adapter.namespace];
        }
    }

    adapter.sendTo(msg.from, msg.command, data, msg.callback);
}

process.on('uncaughtException', function(err) {
    adapter.log.warn('Exception: ' + err);
});
