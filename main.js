/* jshint -W097 */
/* jshint strict: false */
/* jslint node: true */
'use strict';

const utils       = require('@iobroker/adapter-core'); // Get common adapter utils
const adapterName = require('./package.json').name.split('.').pop();
const SQL         = require('sql-client');
const commons     = require('./lib/aggregate');
const fs          = require('fs');
const schedule    = require('node-schedule');
let   SQLFuncs    = null;

const clients = {
    postgresql: {name: 'PostgreSQLClient',  multiRequests: true},
    mysql:      {name: 'MySQLClient',       multiRequests: true},
    sqlite:     {name: 'SQLite3Client',     multiRequests: false},
    mssql:      {name: 'MSSQLClient',       multiRequests: true}
};

const types   = {
    'number':  0,
    'string':  1,
    'boolean': 2,
    'object':  1
};

const dbNames = [
    'ts_number',
    'ts_string',
    'ts_bool'
];

const storageTypes = [
    'Number',
    'String',
    'Boolean'
];

const sqlDPs        = {};
const from          = {};
const tasks         = [];
const tasksReadType = [];
const tasksStart    = [];
const isFromRunning = {};
const aliasMap      = {};

let finished        = false;
let connected       = null;
let multiRequests   = true;
let subscribeAll    = false;
let clientPool;
let adapter;

function startAdapter(options) {
    options = options || {};

    Object.assign(options, {name: adapterName});

    adapter = new utils.Adapter(options);

    adapter.on('objectChange', (id, obj) => {
        let tmpState;
        const now = Date.now();
        const formerAliasId = aliasMap[id] ? aliasMap[id] : id;
        if (obj && obj.common && obj.common.custom  && obj.common.custom[adapter.namespace]  && obj.common.custom[adapter.namespace].enabled) {
            const realId = id;
            let checkForRemove = true;
            if (obj.common.custom && obj.common.custom[adapter.namespace] && obj.common.custom[adapter.namespace].aliasId) {
                if (obj.common.custom[adapter.namespace].aliasId !== id) {
                    aliasMap[id] = obj.common.custom[adapter.namespace].aliasId;
                    adapter.log.debug('Registered Alias: ' + id + ' --> ' + aliasMap[id]);
                    id = aliasMap[id];
                    checkForRemove = false;
                } else {
                    adapter.log.warn('Ignoring Alias-ID because identical to ID for ' + id);
                    obj.common.custom[adapter.namespace].aliasId = '';
                }
            }

            if (checkForRemove && aliasMap[id]) {
                adapter.log.debug('Removed Alias: ' + id + ' !-> ' + aliasMap[id]);
                delete aliasMap[id];
            }

            if (!(sqlDPs[formerAliasId] && sqlDPs[formerAliasId][adapter.namespace]) && !subscribeAll) {
                // un-subscribe
                for (const _id in sqlDPs) {
                    if (sqlDPs.hasOwnProperty(_id) && sqlDPs.hasOwnProperty(sqlDPs[_id].realId)) {
                        adapter.unsubscribeForeignStates(sqlDPs[_id].realId);
                    }
                }
                subscribeAll = true;
                adapter.subscribeForeignStates('*');
            }
            const writeNull = !(sqlDPs[id] && sqlDPs[id][adapter.namespace]);
            if (sqlDPs[formerAliasId] && sqlDPs[formerAliasId].relogTimeout) {
                clearTimeout(sqlDPs[formerAliasId].relogTimeout);
            }

            let storedIndex = null;
            let storedType = null;
            if (sqlDPs[id] && sqlDPs[id].index !== undefined) {
                storedIndex = sqlDPs[id].index;
            }

            if (sqlDPs[id] && sqlDPs[id].dbtype !== undefined) {
                storedType = sqlDPs[id].dbtype;
            } else if (sqlDPs[formerAliasId] && sqlDPs[formerAliasId].dbtype !== undefined) {
                storedType = sqlDPs[formerAliasId].dbtype;
            }

            sqlDPs[id] = obj.common.custom;
            if (storedIndex !== null) {
                sqlDPs[id].index = storedIndex;
            }
            if (storedType !== null) {
                sqlDPs[id].dbtype = storedType;
            }

            if (sqlDPs[id].index === undefined) {
                getId(id, sqlDPs[id].dbtype, () => reInit(id, writeNull, realId));
            } else {
                reInit(id, writeNull, realId);
            }
        } else {
            if (aliasMap[id]) {
                adapter.log.debug('Removed Alias: ' + id + ' !-> ' + aliasMap[id]);
                delete aliasMap[id];
            }
            id = formerAliasId;
            if (sqlDPs[id]) {
                adapter.log.info('disabled logging of ' + id);
                sqlDPs[id].relogTimeout && clearTimeout(sqlDPs[id].relogTimeout);
                sqlDPs[id].timeout && clearTimeout(sqlDPs[id].timeout);

                tmpState = Object.assign({}, sqlDPs[id].state);
                const state = sqlDPs[id].state ? tmpState : null;

                if (sqlDPs[id].skipped) {
                    pushValueIntoDB(id, sqlDPs[id].skipped);
                    sqlDPs[id].skipped = null;
                }

                const nullValue = {val: null, ts: now, lc: now, q: 0x40, from: 'system.adapter.' + adapter.namespace};

                if (sqlDPs[id][adapter.namespace] && adapter.config.writeNulls) {
                    if (sqlDPs[id][adapter.namespace].changesOnly && state && state.val !== null) {
                        (function (_id, _state, _nullValue) {
                            _state.ts   = now;
                            _state.from = 'system.adapter.' + adapter.namespace;
                            nullValue.ts += 4;
                            nullValue.lc += 4; // because of MS SQL
                            adapter.log.debug('Write 1/2 "' + _state.val + '" _id: ' + _id);
                            pushValueIntoDB(_id, _state, () => {
                                // terminate values with null to indicate adapter stop. timestamp + 1#
                                adapter.log.debug('Write 2/2 "null" _id: ' + _id);
                                pushValueIntoDB(_id, _nullValue, () => delete sqlDPs[id][adapter.namespace]);
                            });
                        })(id, state, nullValue);
                    }
                    else {
                        // terminate values with null to indicate adapter stop. timestamp + 1
                        adapter.log.debug('Write 0 NULL _id: ' + id);
                        pushValueIntoDB(id, nullValue, () => delete sqlDPs[id][adapter.namespace]);
                    }
                } else {
                    delete sqlDPs[id][adapter.namespace];
                }
            }
        }
    });

    adapter.on('stateChange', (id, state) => {
        if(id === adapter.namespace + '.statistic.update' && state && state.val === true){
            // listener for start refresh statistic manual -> per button
            refreshStatistic();
        }

        id = aliasMap[id] ? aliasMap[id] : id;
        pushHistory(id, state);
    });

    adapter.on('unload', callback => finish(callback));

    adapter.on('ready', () => main());

    adapter.on('message', msg => processMessage(msg));

    return adapter;
}

function reInit(id, writeNull, realId) {
    adapter.log.debug(`remembered Index/Type ${sqlDPs[id].index} / ${sqlDPs[id].dbtype}`);
    sqlDPs[id].realId  = realId;

    if (sqlDPs[id][adapter.namespace].retention || sqlDPs[id][adapter.namespace].retention === 0) {
        sqlDPs[id][adapter.namespace].retention = parseInt(sqlDPs[id][adapter.namespace].retention, 10) || 0;
    } else {
        sqlDPs[id][adapter.namespace].retention = adapter.config.retention;
    }

    if (sqlDPs[id][adapter.namespace].debounce || sqlDPs[id][adapter.namespace].debounce === 0) {
        sqlDPs[id][adapter.namespace].debounce = parseInt(sqlDPs[id][adapter.namespace].debounce, 10) || 0;
    } else {
        sqlDPs[id][adapter.namespace].debounce = adapter.config.debounce;
    }

    sqlDPs[id][adapter.namespace].changesOnly = sqlDPs[id][adapter.namespace].changesOnly === 'true' || sqlDPs[id][adapter.namespace].changesOnly === true;

    if (sqlDPs[id][adapter.namespace].changesRelogInterval || sqlDPs[id][adapter.namespace].changesRelogInterval === 0) {
        sqlDPs[id][adapter.namespace].changesRelogInterval = parseInt(sqlDPs[id][adapter.namespace].changesRelogInterval, 10) || 0;
    } else {
        sqlDPs[id][adapter.namespace].changesRelogInterval = adapter.config.changesRelogInterval;
    }

    if (sqlDPs[id][adapter.namespace].changesRelogInterval > 0) {
        sqlDPs[id].relogTimeout = setTimeout(reLogHelper, (sqlDPs[id][adapter.namespace].changesRelogInterval * 500 * Math.random()) + sqlDPs[id][adapter.namespace].changesRelogInterval * 500, id);
    }

    if (sqlDPs[id][adapter.namespace].changesMinDelta || sqlDPs[id][adapter.namespace].changesMinDelta === 0) {
        sqlDPs[id][adapter.namespace].changesMinDelta = parseFloat(sqlDPs[id][adapter.namespace].changesMinDelta.toString().replace(/,/g, '.')) || 0;
    } else {
        sqlDPs[id][adapter.namespace].changesMinDelta = adapter.config.changesMinDelta;
    }

    if (!sqlDPs[id][adapter.namespace].storageType) {
        sqlDPs[id][adapter.namespace].storageType = false;
    }

    // add one day if retention is too small
    if (sqlDPs[id][adapter.namespace].retention && sqlDPs[id][adapter.namespace].retention <= 604800) {
        sqlDPs[id][adapter.namespace].retention += 86400;
    }

    if (writeNull && adapter.config.writeNulls) {
        writeNulls(id);
    }

    adapter.log.info('enabled logging of ' + id + ', Alias=' + (id !== realId));
}

function setConnected(isConnected) {
    if (connected !== isConnected) {
        connected = isConnected;
        adapter.setState('info.connection', connected, true);
    }
}

let _client = false;
function connect(callback) {
    if (!clientPool) {
        setConnected(false);

        let params = {
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
            return _client.connect(err => {
                if (err) {
                    adapter.log.error(err);
                    setTimeout(() => connect(callback), 30000);
                    return;
                }
                _client.execute('CREATE DATABASE ' + adapter.config.dbname + ';', (err /* , rows, fields */) => {
                    _client.disconnect();
                    if (err && err.code !== '42P04') { // if error not about yet exists
                        _client = false;
                        adapter.log.error(err);
                        setTimeout(() => connect(callback), 30000);
                    } else {
                        _client = true;
                        setTimeout(() => connect(callback), 100);
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
                return clientPool.open(err => {
                    if (err) {
                        adapter.log.error(err);
                        setTimeout(() => connect(callback), 30000);
                    } else {
                        setTimeout(() => connect(callback), 0);
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
            return setTimeout(() => connect(callback), 30000);
        }
    }

    allScripts(SQLFuncs.init(adapter.config.dbname), err => {
        if (err) {
            //adapter.log.error(err);
            return setTimeout(() => connect(callback), 30000);
        } else {
            adapter.log.info('Connected to ' + adapter.config.dbtype);
            // read all DB IDs and all FROM ids
            getAllIds(() =>
                getAllFroms(callback));
        }
    });
}

// Find sqlite data directory
function getSqlLiteDir(fileName) {
    fileName = fileName || 'sqlite.db';
    fileName = fileName.replace(/\\/g, '/');
    if (fileName[0] === '/' || fileName.match(/^\w:\//)) {
        return fileName;
    } else {
        // normally /opt/iobroker/node_modules/iobroker.js-controller
        // but can be /example/ioBroker.js-controller
        const tools = require(utils.controllerDir + '/lib/tools');
        let config = tools.getConfigFileName().replace(/\\/g, '/');
        const parts = config.split('/');
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
    let params = {
        server:     msg.message.config.host,
        host:       msg.message.config.host,
        user:       msg.message.config.user,
        password:   msg.message.config.password
    };
    if (msg.message.config.port) {
        params.port = msg.message.config.port;
    }

    if (msg.message.config.dbtype === 'postgresql' && !SQL.PostgreSQLClient) {
        const postgres = require('./lib/postgresql-client');
        Object.keys(postgres)
            .filter(attr => !SQL[attr])
            .forEach(attr => SQL[attr] = postgres[attr]);
    } else
    if (msg.message.config.dbtype === 'mssql' && !SQL.MSSQLClient) {
        const mssql = require('./lib/mssql-client');
        Object.keys(mssql)
            .filter(attr => !SQL[attr])
            .forEach(attr => SQL[attr] = mssql[attr]);
    }

    if (msg.message.config.dbtype === 'postgresql') {
        params.database = 'postgres';
    } else if (msg.message.config.dbtype === 'sqlite') {
        params = getSqlLiteDir(msg.message.config.fileName);
    }
    let timeout;
    try {
        const client = new SQL[clients[msg.message.config.dbtype].name](params);
        timeout = setTimeout(() => {
            timeout = null;
            adapter.sendTo(msg.from, msg.command, {error: 'connect timeout'}, msg.callback);
        }, 5000);

        client.connect(err => {
            if (err) {
                if (timeout) {
                    clearTimeout(timeout);
                    timeout = null;
                }
                return adapter.sendTo(msg.from, msg.command, {error: err.toString()}, msg.callback);
            }
            client.execute('SELECT 2 + 3 AS x', (err /* , rows, fields */) => {
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
        allScripts(SQLFuncs.destroy(adapter.config.dbname), err => {
            if (err) {
                adapter.log.error(err);
                adapter.sendTo(msg.from, msg.command, {error: err.toString()}, msg.callback);
            } else {
                adapter.sendTo(msg.from, msg.command, {error: null}, msg.callback);
                // restart adapter
                setTimeout(() =>
                    adapter.getForeignObject('system.adapter.' + adapter.namespace, (err, obj) => {
                        if (!err) {
                            adapter.setForeignObject(obj._id, obj);
                        } else {
                            adapter.log.error('Cannot read object "system.adapter.' + adapter.namespace + '": ' + err);
                            adapter.stop();
                        }
                    }), 2000);
            }
        });
    } catch (ex) {
        return adapter.sendTo(msg.from, msg.command, {error: ex.toString()}, msg.callback);
    }
}

function _userQuery(msg, callback) {
    try {
        adapter.log.debug(msg.message);

        clientPool.borrow((err, client) => {
            if (err) {
                adapter.sendTo(msg.from, msg.command, {error: err.toString()}, msg.callback);
                callback && callback();
            } else {
                client.execute(msg.message, (err, rows /* , fields */) => {
                    if (rows && rows.rows) rows = rows.rows;
                    clientPool.return(client);
                    adapter.sendTo(msg.from, msg.command, {error: err ? err.toString() : null, result: rows}, msg.callback);
                    callback && callback();
                });
            }
        });
    } catch (err) {
        adapter.sendTo(msg.from, msg.command, {error: err.toString()}, msg.callback);
        callback && callback();
    }
}
// execute custom query
function query(msg) {
    if (!multiRequests) {
        if (tasks.length > 100) {
            const error = 'Cannot queue new requests, because more than 100';
            adapter.log.error(error);
            adapter.sendTo(msg.from, msg.command, {error}, msg.callback);
        } else {
            tasks.push({operation: 'userQuery', msg});
            tasks.length === 1 && processTasks();
        }
    } else {
        _userQuery(msg);
    }
}

// one script
function oneScript(script, cb) {
    try {
        clientPool.borrow((err, client) => {
            if (err || !client) {
                clientPool.close();
                clientPool = null;
                adapter.log.error(err);
                return cb && cb(err);
            }

            adapter.log.debug(script);

            client.execute(script, (err /* , rows, fields */) => {
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
                        const match = script.match(/CREATE\s+TABLE\s+(\w*)\s+\(/);
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
                cb && cb(err);
                clientPool.return(client);
            });
        });
    } catch (ex) {
        adapter.log.error(ex);
        cb && cb(ex);
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
        oneScript(scripts[index], err => {
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

    function finishId(id) {
        if (sqlDPs[id].relogTimeout) {
            clearTimeout(sqlDPs[id].relogTimeout);
            sqlDPs[id].relogTimeout = null;
        }
        if (sqlDPs[id].timeout) {
            clearTimeout(sqlDPs[id].timeout);
            sqlDPs[id].timeout  = null;
        }
        let tmpState = Object.assign({}, sqlDPs[id].state);
        const state = sqlDPs[id].state ? tmpState : null;

        if (sqlDPs[id].skipped) {
            count++;
            pushValueIntoDB(id, sqlDPs[id].skipped, () => {
                if (!--count) {
                    if (clientPool) {
                        clientPool.close();
                        clientPool = null;
                    }
                    if (typeof finished === 'object') {
                        setTimeout(cb => {
                            for (let f = 0; f < cb.length; f++) {
                                cb[f]();
                            }
                        }, 500, finished);
                        finished = true;
                    }
                }
            });
            sqlDPs[id].skipped = null;
        }

        const nullValue = {val: null, ts: now, lc: now, q: 0x40, from: 'system.adapter.' + adapter.namespace};

        if (sqlDPs[id][adapter.namespace] && adapter.config.writeNulls) {
            if (sqlDPs[id][adapter.namespace].changesOnly && state && state.val !== null) {
                count++;
                (function (_id, _state, _nullValue) {
                    _state.ts   = now;
                    _state.from = 'system.adapter.' + adapter.namespace;
                    nullValue.ts += 4;
                    nullValue.lc += 4; // because of MS SQL
                    adapter.log.debug('Write 1/2 "' + _state.val + '" _id: ' + _id);
                    pushValueIntoDB(_id, _state, () => {
                        // terminate values with null to indicate adapter stop. timestamp + 1#
                        adapter.log.debug('Write 2/2 "null" _id: ' + _id);
                        pushValueIntoDB(_id, _nullValue, () => {
                            if (!--count) {
                                if (clientPool) {
                                    clientPool.close();
                                    clientPool = null;
                                }
                                if (typeof finished === 'object') {
                                    setTimeout(cb => {
                                        for (let f = 0; f < cb.length; f++) {
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
                pushValueIntoDB(id, nullValue, () => {
                    if (!--count) {
                        setTimeout(() => {
                            if (clientPool) {
                                clientPool.close();
                                clientPool = null;
                            }
                            if (typeof finished === 'object') {
                                setTimeout(cb => {
                                    for (let f = 0; f < cb.length; f++) {
                                        cb[f]();
                                    }
                                }, 500, finished);
                                finished = true;
                            }
                        }, 5000);
                    }
                });
            }
        }
    }

    adapter.unsubscribeForeignStates('*');
    let count = 0;
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
    const now = Date.now();
    let dpcount = 0;
    let delay = 0;
    for (const id in sqlDPs) {
        if (!sqlDPs.hasOwnProperty(id)) continue;
        dpcount++;
        delay += (dpcount%50 === 0) ? 1000: 0;
        setTimeout(finishId, delay, id);
    }

    if (!dpcount && callback) {
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
    else if (msg.command === 'getCounter') {
        getCounterDiff(msg);
    }
    else if (msg.command === 'test') {
        testConnection(msg);
    }
    else if (msg.command === 'destroy') {
        destroyDB(msg);
    }
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
        finish(() => {
            if (msg.callback) {
                adapter.sendTo(msg.from, msg.command, 'stopped', msg.callback);
                setTimeout(() => process.exit(0), 200);
            }
        });
    }
}

function fixSelector(callback) {
    // fix _design/custom object
    adapter.getForeignObject('_design/custom', (err, obj) => {
        if (!obj || !obj.views.state.map.includes('common.custom')) {
            obj = {
                _id: '_design/custom',
                language: 'javascript',
                views: {
                    state: {
                        map: 'function(doc) { doc.type === \'state\' && doc.common.custom && emit(doc._id, doc.common.custom) }'
                    }
                }
            };
            adapter.setForeignObject('_design/custom', obj, err => callback && callback(err));
        } else {
            callback && callback(err);
        }
    });
}

function processStartValues(callback) {
    if (tasksStart && tasksStart.length) {
        const task = tasksStart.shift();
        if (sqlDPs[task.id][adapter.namespace].changesOnly) {
            adapter.getForeignState(sqlDPs[task.id].realId, (err, state) => {
                const now = task.now || Date.now();
                pushHistory(task.id, {
                    val:  null,
                    ts:   state ? now - 4 : now, // 4 is because of MS SQL
                    lc:   state ? now - 4 : now, // 4 is because of MS SQL
                    ack:  true,
                    q:    0x40,
                    from: 'system.adapter.' + adapter.namespace
                });

                if (state) {
                    state.ts = now;
                    state.lc = now;
                    state.from = 'system.adapter.' + adapter.namespace;
                    pushHistory(task.id, state);
                }

                setTimeout(processStartValues, 0);
            });
        }
        else {
            pushHistory(task.id, {
                val:  null,
                ts:   task.now || Date.now(),
                lc:   task.now || Date.now(),
                ack:  true,
                q:    0x40,
                from: 'system.adapter.' + adapter.namespace
            });

            setTimeout(processStartValues, 0);
        }
        if (sqlDPs[task.id][adapter.namespace] && sqlDPs[task.id][adapter.namespace].changesRelogInterval > 0) {
            sqlDPs[task.id].relogTimeout && clearTimeout(sqlDPs[task.id].relogTimeout);
            sqlDPs[task.id].relogTimeout = setTimeout(reLogHelper, (sqlDPs[task.id][adapter.namespace].changesRelogInterval * 500 * Math.random()) + sqlDPs[task.id][adapter.namespace].changesRelogInterval * 500, task.id);
        }
    } else {
        callback && callback();
    }
}

function writeNulls(id, now) {
    if (!id) {
        now = Date.now();

        Object.keys(sqlDPs)
            .filter(_id => sqlDPs[_id] && sqlDPs[_id][adapter.namespace])
            .forEach(_id => writeNulls(_id, now));
    } else {
        now = now || Date.now();

        tasksStart.push({id, now});

        if (tasksStart.length === 1 && connected) {
            processStartValues();
        }
    }
}

function pushHistory(id, state, timerRelog) {
    if (timerRelog === undefined) {
        timerRelog = false;
    }

    // Push into DB
    if (sqlDPs[id]) {
        const settings = sqlDPs[id][adapter.namespace];

        if (!settings || !state) {
            return;
        }

        adapter.log.debug(`new value received for ${id}, new-value=${state.val}, ts=${state.ts}, relog=${timerRelog}`);

        if (state.val !== null && typeof state.val === 'string' && settings.storageType !== 'String') {
            const f = parseFloat(state.val);
            // do not use here === or find a better way to validate if string is valid float
            if (f == state.val) {
                state.val = f;
            }
        }

        if (settings.counter && sqlDPs[id].state) {
            if (sqlDPs[id].type !== 'number') {
                adapter.log.error('Counter cannot have type not "number"!');
            }
            // if actual value is less then last seen counter
            else if (state.val < sqlDPs[id].state.val) {
                // store both values
                pushValueIntoDB(id, sqlDPs[id].state, true);
                pushValueIntoDB(id, state, true);
            }
        }

        if (sqlDPs[id].state && settings.changesOnly && !timerRelog) {
            if (settings.changesRelogInterval === 0) {
                if (state.ts !== state.lc) {
                    sqlDPs[id].skipped = state; // remember new timestamp
                    return adapter.log.debug(`value not changed ${id}, last-value=${sqlDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
                }
            } else if (sqlDPs[id].lastLogTime) {
                if ((state.ts !== state.lc) && (Math.abs(sqlDPs[id].lastLogTime - state.ts) < settings.changesRelogInterval * 1000)) {
                    sqlDPs[id].skipped = state; // remember new timestamp
                    return adapter.log.debug(`value not changed relog ${id}, last-value=${sqlDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
                }
                if (state.ts !== state.lc) {
                    adapter.log.debug(`value-changed-relog ${id}, value=${state.val}, lastLogTime=${sqlDPs[id].lastLogTime}, ts=${state.ts}`);
                }
            }

            if (sqlDPs[id].state.val !== null && settings.changesMinDelta !== 0 && typeof state.val === 'number' && Math.abs(sqlDPs[id].state.val - state.val) < settings.changesMinDelta) {
                adapter.log.debug(`Min-Delta not reached ${id}, last-value=${sqlDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
                sqlDPs[id].skipped = state; // remember new timestamp
                return;
            } else if (typeof state.val === 'number') {
                adapter.log.debug(`Min-Delta reached ${id}, last-value=${sqlDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
            } else {
                adapter.log.debug(`Min-Delta ignored because no number ${id}, last-value=${sqlDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
            }
        }

        if (sqlDPs[id].relogTimeout) {
            clearTimeout(sqlDPs[id].relogTimeout);
            sqlDPs[id].relogTimeout = null;
        }
        if (settings.changesRelogInterval > 0) {
            sqlDPs[id].relogTimeout = setTimeout(reLogHelper, settings.changesRelogInterval * 1000, id);
        }

        let ignoreDebonce = false;
        if (timerRelog) {
            state.ts = Date.now();
            adapter.log.debug(`timed-relog ${id}, value=${state.val}, lastLogTime=${sqlDPs[id].lastLogTime}, ts=${state.ts}`);
            ignoreDebonce = true;
        } else {
            if (settings.changesOnly && sqlDPs[id].skipped) {
                sqlDPs[id].state = sqlDPs[id].skipped;
                pushHelper(id);
            }

            if (sqlDPs[id].state && ((sqlDPs[id].state.val === null && state.val !== null) || (sqlDPs[id].state.val !== null && state.val === null))) {
                ignoreDebonce = true;
            }
            else if (!sqlDPs[id].state && state.val === null) {
                ignoreDebonce = true;
            }

            // only store state if really changed
            sqlDPs[id].state = state;
        }
        sqlDPs[id].lastLogTime = state.ts;
        sqlDPs[id].skipped = null;

        if (settings.debounce && !ignoreDebonce) {
            // Discard changes in debounce time to store last stable value
            sqlDPs[id].timeout && clearTimeout(sqlDPs[id].timeout);
            sqlDPs[id].timeout = setTimeout(pushHelper, settings.debounce, id, true);
        } else {
            pushHelper(id);
        }
    }
}

function reLogHelper(_id) {
    if (!sqlDPs[_id]) {
        adapter.log.info('non-existing id ' + _id);
    } else {
        sqlDPs[_id].relogTimeout = null;
        if (sqlDPs[_id].skipped) {
            sqlDPs[_id].state = sqlDPs[_id].skipped;
            sqlDPs[_id].state.from = 'system.adapter.' + adapter.namespace;
            sqlDPs[_id].skipped = null;
            pushHistory(_id, sqlDPs[_id].state, true);
        } else {
            adapter.getForeignState(sqlDPs[_id].realId, (err, state) => {
                if (err) {
                    adapter.log.info(`init timed Relog: can not get State for ${_id} : ${err}`);
                } else if (!state) {
                    adapter.log.info(`init timed Relog: disable relog because state not set so far for ${_id}: ${JSON.stringify(state)}`);
                } else {
                    adapter.log.debug(`init timed Relog: getState ${_id}:  Value=${state.val}, ack=${state.ack}, ts=${state.ts}, lc=${state.lc}`);
                    sqlDPs[_id].state = state;
                    pushHistory(_id, sqlDPs[_id].state, true);
                }
            });
        }
    }
}

function pushHelper(_id, timeoutTriggered) {
    if (!sqlDPs[_id] || !sqlDPs[_id].state) {
        return;
    }

    const _settings = sqlDPs[_id][adapter.namespace];

    // if it was not deleted in this time
    if (_settings) {
        if (timeoutTriggered) {
            sqlDPs[_id].timeout = null;
        }

        if (sqlDPs[_id].state.val !== null && (typeof sqlDPs[_id].state.val === 'object' || typeof sqlDPs[_id].state.val === 'undefined')) {
            sqlDPs[_id].state.val = JSON.stringify(sqlDPs[_id].state.val);
        }

        if (sqlDPs[_id].state.val !== null && sqlDPs[_id].state.val !== undefined) {
            adapter.log.debug('Datatype ' + _id + ': Currently: ' + typeof sqlDPs[_id].state.val + ', StorageType: ' + _settings.storageType);

            if (typeof sqlDPs[_id].state.val === 'string' && _settings.storageType !== 'String') {
                adapter.log.debug('Do Automatic Datatype conversion for ' + _id);
                const f = parseFloat(sqlDPs[_id].state.val);

                // do not use here === or find a better way to validate if string is valid float
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
                    sqlDPs[_id].state.val = sqlDPs[_id].state.val ? 1 : 0;
                } else {
                    return adapter.log.info('Do not store value "' + sqlDPs[_id].state.val + '" for ' + _id + ' because no number');
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
    const query = SQLFuncs.getIdSelect(adapter.config.dbname);
    adapter.log.debug(query);
    clientPool.borrow((err, client) => {
        if (err) {
            return cb && cb(err);
        }

        client.execute(query, (err, rows /* , fields */) => {
            clientPool.return(client);
            if (rows && rows.rows) rows = rows.rows;
            if (err) {
                adapter.log.error('Cannot select ' + query + ': ' + err);
                return cb && cb(err);
            }

            if (rows.length) {
                let id;
                for (let r = 0; r < rows.length; r++) {
                    id = rows[r].name;
                    sqlDPs[id] = sqlDPs[id] || {};
                    sqlDPs[id].index = rows[r].id;
                    if (rows[r].type !== null) sqlDPs[id].dbtype = rows[r].type;
                }
            }
            cb && cb();
        });
    });
}

function getAllFroms(cb) {
    const query = SQLFuncs.getFromSelect(adapter.config.dbname);
    adapter.log.debug(query);
    clientPool.borrow((err, client) => {
        if (err) {
            return cb && cb(err);
        }

        client.execute(query, (err, rows /* , fields */) => {
            clientPool.return(client);
            if (rows && rows.rows) rows = rows.rows;
            if (err) {
                adapter.log.error('Cannot select ' + query + ': ' + err);
                return cb && cb(err);
            }

            if (rows.length) {
                for (let r = 0; r < rows.length; r++) {
                    from[rows[r].name] = rows[r].id;
                }
            }
            cb && cb();
        });
    });
}

function _checkRetention(query, cb) {
    adapter.log.debug(query);

    clientPool.borrow((err, client) => {
        if (err) {
            adapter.log.error(err);
            cb && cb();
        } else {
            client.execute(query, (err /* , rows, fields */ ) => {
                err && adapter.log.error('Cannot delete ' + query + ': ' + err);
                clientPool.return(client);
                cb && cb();
            });
        }
    });
}

function checkRetention(id) {
    if (sqlDPs[id] && sqlDPs[id][adapter.namespace] && sqlDPs[id][adapter.namespace].retention) {
        const d = new Date();
        const dt = d.getTime();
        // check every 6 hours
        if (!sqlDPs[id].lastCheck || dt - sqlDPs[id].lastCheck >= 21600000/* 6 hours */) {
            sqlDPs[id].lastCheck = dt;
            if (!dbNames[sqlDPs[id].type]) {
                adapter.log.error(`No type found for ${id}. Retention is not possible. Debug info: ${JSON.stringify(sqlDPs[id])}`);
            } else {
                const query = SQLFuncs.retention(adapter.config.dbname, sqlDPs[id].index, dbNames[sqlDPs[id].type], sqlDPs[id][adapter.namespace].retention);

                if (!multiRequests) {
                    if (tasks.length > 100) {
                        return adapter.log.error('Cannot queue new requests, because more than 100');
                    }

                    let start = tasks.length === 1;
                    tasks.push({operation: 'delete', query});

                    // delete counters too
                    if (sqlDPs[id].type === 'number') {
                        const query = SQLFuncs.retention(adapter.config.dbname, sqlDPs[id].index, 'ts_counter', sqlDPs[id][adapter.namespace].retention);
                        tasks.push({operation: 'delete', query});
                    }

                    start && processTasks();
                } else {
                    _checkRetention(query, () => {
                        // delete counters too
                        if (sqlDPs[id].type === 'number') {
                            const query = SQLFuncs.retention(adapter.config.dbname, sqlDPs[id].index, 'ts_counter', sqlDPs[id][adapter.namespace].retention);
                            _checkRetention(query);
                        }
                    });
                }
            }
        }
    }
}

function _insertValueIntoDB(query, id, cb) {
    adapter.log.debug(query);

    clientPool.borrow((err, client) => {
        if (err) {
            adapter.log.error(err);
            cb && cb();
        } else {
            client.execute(query, (err /* , rows, fields */) => {
                err && adapter.log.error('Cannot insert ' + query + ': ' + err);
                clientPool.return(client);
                checkRetention(id);
                cb && cb();
            });
        }
    });
}

function processReadTypes() {
    if (tasksReadType && tasksReadType.length) {
        const task = tasksReadType.shift();
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
            adapter.getForeignObject(sqlDPs[task.id].realId, (err, obj) => {
                if (err) {
                    adapter.log.warn('Error while get Object for Def: ' + err);
                }
                if (obj && obj.common && obj.common.type) {
                    adapter.log.debug(obj.common.type.toLowerCase() + ' / ' + types[obj.common.type.toLowerCase()] + ' / ' + JSON.stringify(obj.common));
                    sqlDPs[task.id].type = types[obj.common.type.toLowerCase()];
                    sqlDPs[task.id][adapter.namespace].storageType = storageTypes[sqlDPs[task.id].type];
                    adapter.log.debug('Type (from Obj) for ' + task.id + ': ' + sqlDPs[task.id].type);
                    processVerifyTypes(task);
                }
                if (sqlDPs[task.id].type === undefined) {
                    adapter.getForeignState(sqlDPs[task.id].realId, (err, state) => {
                        if (err) {
                            adapter.log.warn('Store data for ' + task.id + ' as string because no other valid type found (' + obj.common.type.toLowerCase() + ' and no state)');
                            sqlDPs[task.id].type = 1; // string
                        }
                        else if (state && state.val !== null && state.val !== undefined && types[typeof state.val] !== undefined) {
                            sqlDPs[task.id].type = types[typeof state.val];
                            sqlDPs[task.id][adapter.namespace].storageType = storageTypes[sqlDPs[task.id].type];
                        }
                        else {
                            adapter.log.warn('Store data for ' + task.id + ' as string because no other valid type found (' + (state?(typeof state.val):'state not existing') + ')');
                            sqlDPs[task.id].type = 1; // string
                        }
                        adapter.log.debug('Type (from State) for ' + task.id + ': ' + sqlDPs[task.id].type);
                        processVerifyTypes(task);
                    });
                }
            });
        }
    }
}

function processVerifyTypes(task) {
    if (sqlDPs[task.id].index !== undefined && sqlDPs[task.id].type !== undefined && sqlDPs[task.id].type !== sqlDPs[task.id].dbtype) {
        sqlDPs[task.id].dbtype = sqlDPs[task.id].type;

        const query = SQLFuncs.getIdUpdate(adapter.config.dbname, sqlDPs[task.id].index, sqlDPs[task.id].type);

        adapter.log.debug(query);

        clientPool.borrow((err, client) => {
            if (err) {
                return processVerifyTypes(task);
            }

            client.execute(query, (err, rows /* , fields */) => {
                if (err) {
                    adapter.log.error(`error updating history config for ${task.id} to pin datatype: ${query}: ${err}`);
                } else {
                    adapter.log.info('changed history configuration to pin detected datatype for ' + task.id);
                }
                clientPool.return(client);
                processVerifyTypes(task);
            });
        });

        return;
    }

    pushValueIntoDB(task.id, task.state);

    setTimeout(processReadTypes, 50);
}

function pushValueIntoDB(id, state, isCounter, cb) {
    if (typeof isCounter === 'function') {
        cb = isCounter;
        isCounter = false;
    }

    // check if we know about this ID
    if (!sqlDPs[id]) {
        return;
    }

    // Check sql connection
    if (!clientPool) {
        adapter.log.warn('No connection to SQL-DB');
        return cb && cb('No connection to SQL-DB');
    }

    let type;

    // read type of value
    if (sqlDPs[id].type !== undefined) {
        type = sqlDPs[id].type;
    } else {
        // read type from DB
        tasksReadType.push({id, state});
        return tasksReadType.length === 1 && processReadTypes();
    }

    if (type === undefined) { // Can not happen anymore
        let warn;
        if (state.val === null) {
            warn = `Ignore null value for ${id} because no type defined till now.`;
        } else {
            warn = `Cannot store values of type "${typeof state.val}" for ${id}`;
        }

        adapter.log.warn(warn);
        return cb && cb(warn);
    }

    let tmpState;
    // get sql id of state
    if (sqlDPs[id].index === undefined) {
        sqlDPs[id].isRunning = sqlDPs[id].isRunning || [];

        tmpState = Object.assign({}, state);

        sqlDPs[id].isRunning.push({id, state: tmpState, cb, isCounter});

        if (sqlDPs[id].isRunning.length === 1) {
            // read or create in DB
            return getId(id, type, (err, _id) => {
                if (err) {
                    adapter.log.warn('Cannot get index of "' + _id + '": ' + err);
                    if (sqlDPs[_id].isRunning) {
                        for (let t = 0; t < sqlDPs[_id].isRunning.length; t++) {
                            sqlDPs[_id].isRunning[t].cb && sqlDPs[_id].isRunning[t].cb('Cannot get index of "' + sqlDPs[_id].isRunning[t].id + '": ' + err);
                        }
                    }
                } else {
                    if (sqlDPs[_id].isRunning) {
                        for (let k = 0; k < sqlDPs[_id].isRunning.length; k++) {
                            pushValueIntoDB(sqlDPs[_id].isRunning[k].id, sqlDPs[_id].isRunning[k].state, sqlDPs[_id].isRunning[k].isCounter, sqlDPs[_id].isRunning[k].cb);
                        }
                    }
                }
                sqlDPs[_id].isRunning = null;
            });
        }
        return;
    }

    // get from
    if (!isCounter && state.from && !from[state.from]) {
        isFromRunning[state.from] = isFromRunning[state.from] || [];
        tmpState = Object.assign({}, state);

        isFromRunning[state.from].push({id, state: tmpState, cb});

        if (isFromRunning[state.from].length === 1) {
            // read or create in DB
            return getFrom(state.from, (err, from) => {
                if (err) {
                    adapter.log.warn('Cannot get "from" for "' + from + '": ' + err);
                    if (isFromRunning[from]) {
                        for (let t = 0; t < isFromRunning[from].length; t++) {
                            isFromRunning[from][t].cb && isFromRunning[from][t].cb(`Cannot get "from" for "${from}": ${err}`);
                        }
                    }
                } else {
                    if (isFromRunning[from]) {
                        for (let k = 0; k < isFromRunning[from].length; k++) {
                            pushValueIntoDB(isFromRunning[from][k].id, isFromRunning[from][k].state, isFromRunning[from][k].isCounter, isFromRunning[from][k].cb);
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
        if (state.val !== null && (typeof state.val === 'object' || typeof state.val === 'undefined')) {
            state.val = JSON.stringify(state.val);
        }
    } catch (err) {
        const error = `Cannot convert the object value "${id}"`;
        adapter.log.error(error);
        return cb && cb(error);
    }

    // increase timestamp if last is the same
    if (!isCounter && sqlDPs[id].ts && state.ts === sqlDPs[id].ts) {
        state.ts++;
    }

    // remember last timestamp
    sqlDPs[id].ts = state.ts;

    const query = SQLFuncs.insert(adapter.config.dbname, sqlDPs[id].index, state, from[state.from] || 0, isCounter ? 'ts_counter' : dbNames[type]);

    if (!multiRequests) {
        if (tasks.length > 100) {
            const error = 'Cannot queue new requests, because more than 100';
            adapter.log.error(error);
            cb && cb(error);
        } else {
            tasks.push({operation: 'insert', query, id, callback: cb});
            tasks.length === 1 && processTasks();
        }
    } else {
        _insertValueIntoDB(query, id, cb);
    }
}

let lockTasks = false;
function processTasks() {
    if (lockTasks) {
        return adapter.log.debug('Tries to execute task, but last one not finished!');
    }

    lockTasks = true;

    if (tasks.length) {
        if (tasks[0].operation === 'insert') {
            _insertValueIntoDB(tasks[0].query, tasks[0].id, () => {
                tasks[0].callback && tasks[0].callback();
                tasks.shift();
                lockTasks = false;
                tasks.length && setTimeout(processTasks, adapter.config.requestInterval);
            });
        }
        else if (tasks[0].operation === 'select') {
            _getDataFromDB(tasks[0].query, tasks[0].options, (err, rows) => {
                tasks[0].callback && tasks[0].callback(err, rows);
                tasks.shift();
                lockTasks = false;
                tasks.length && setTimeout(processTasks, adapter.config.requestInterval);
            });
        }
        else if (tasks[0].operation === 'userQuery') {
            _userQuery(tasks[0].msg, () => {
                tasks[0].callback && tasks[0].callback();
                tasks.shift();
                lockTasks = false;
                tasks.length && setTimeout(processTasks, adapter.config.requestInterval);
            });
        }
        else if (tasks[0].operation === 'delete') {
            _checkRetention(tasks[0].query, () => {
                tasks[0].callback && tasks[0].callback();
                tasks.shift();
                lockTasks = false;
                tasks.length && setTimeout(processTasks, adapter.config.requestInterval);
            });
        }
        else {
            adapter.log.error('unknown task: ' + tasks[0].operation);
            tasks[0].callback && tasks[0].callback();
            tasks.shift();
            lockTasks = false;
            tasks.length && setTimeout(processTasks, adapter.config.requestInterval);
        }
    }
}

// my be it is required to cache all the data in memory
function getId(id, type, cb) {
    let query = SQLFuncs.getIdSelect(adapter.config.dbname, id);
    adapter.log.debug(query);

    if (!clientPool) {
        return cb && cb('No connection', id);
    }

    clientPool.borrow((err, client) => {
        if (err) {
            return cb && cb(err, id);
        }

        client.execute(query, (err, rows /* , fields */) => {
            if (rows && rows.rows) {
                rows = rows.rows;
            }

            if (err) {
                adapter.log.error('Cannot select ' + query + ': ' + err);
                cb && cb(err, id);
                return clientPool.return(client);
            } else if (!rows.length) {
                if (type !== null && type !== undefined) {
                    // insert
                    query = SQLFuncs.getIdInsert(adapter.config.dbname, id, type);

                    adapter.log.debug(query);

                    client.execute(query, (err /* , rows, fields */) => {
                        if (err) {
                            adapter.log.error('Cannot insert ' + query + ': ' + err);
                            cb && cb(err, id);
                            clientPool.return(client);
                        } else {
                            query = SQLFuncs.getIdSelect(adapter.config.dbname,id);

                            adapter.log.debug(query);

                            client.execute(query, (err, rows /* , fields */) => {
                                if (rows && rows.rows) {
                                    rows = rows.rows;
                                }

                                if (err) {
                                    adapter.log.error('Cannot select ' + query + ': ' + err);
                                    cb && cb(err, id);
                                    clientPool.return(client);
                                } else {
                                    sqlDPs[id].index = rows[0].id;
                                    sqlDPs[id].type  = rows[0].type;

                                    cb && cb(null, id);
                                    clientPool.return(client);
                                }
                            });
                        }
                    });
                } else {
                    cb && cb('id not found', id);
                    clientPool.return(client);
                }
            } else {
                sqlDPs[id].index = rows[0].id;
                sqlDPs[id].type  = rows[0].type;

                cb && cb(null, id);
                clientPool.return(client);
            }
        });
    });
}

// my be it is required to cache all the data in memory
function getFrom(_from, cb) {
    // const sources    = (adapter.config.dbtype !== 'postgresql' ? (adapter.config.dbname + '.') : '') + 'sources';
    let query = SQLFuncs.getFromSelect(adapter.config.dbname, _from);
    adapter.log.debug(query);

    if (!clientPool) {
        return cb && cb('No connection', _from);
    }

    clientPool.borrow((err, client) => {
        if (err) {
            return cb && cb(err, _from);
        }
        client.execute(query, (err, rows /* , fields */) => {
            if (rows && rows.rows) rows = rows.rows;
            if (err) {
                adapter.log.error('Cannot select ' + query + ': ' + err);
                cb && cb(err, _from);
                return clientPool.return(client);
            }
            if (!rows.length) {
                // insert
                query = SQLFuncs.getFromInsert(adapter.config.dbname, _from);
                adapter.log.debug(query);
                client.execute(query, (err /* , rows, fields */) => {
                    if (err) {
                        adapter.log.error('Cannot insert ' + query + ': ' + err);
                        cb && cb(err, _from);
                        return clientPool.return(client);
                    }

                    query = SQLFuncs.getFromSelect(adapter.config.dbname, _from);
                    adapter.log.debug(query);
                    client.execute(query, (err, rows /* , fields */) => {
                        if (rows && rows.rows) rows = rows.rows;
                        if (err) {
                            adapter.log.error('Cannot select ' + query + ': ' + err);
                            cb && cb(err, _from);
                            return clientPool.return(client);
                        }
                        from[_from] = rows[0].id;

                        cb && cb(null, _from);
                        clientPool.return(client);
                    });
                });
            } else {
                from[_from] = rows[0].id;

                cb && cb(null, _from);
                clientPool.return(client);
            }
        });
    });
}

function sortByTs(a, b) {
    const aTs = a.ts;
    const bTs = b.ts;
    return aTs < bTs ? -1 : (aTs > bTs ? 1 : 0);
}

function _getDataFromDB(query, options, callback) {
    adapter.log.debug(query);

    clientPool.borrow((err, client) => {
        if (err) {
            return callback && callback(err);
        }
        client.execute(query, (err, rows /* , fields */) => {
            if (rows && rows.rows) rows = rows.rows;
            // because descending
            if (!err && rows && !options.start && options.count) {
                rows.sort(sortByTs);
            }

            if (rows) {
                let isNumber = null;
                for (let c = 0; c < rows.length; c++) {
                    if (isNumber === null && rows[c].val !== null) {
                        isNumber = parseFloat(rows[c].val) == rows[c].val;
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
            callback && callback(err, rows);
        });
    });
}

function getDataFromDB(db, options, callback) {
    const query = SQLFuncs.getHistory(adapter.config.dbname, db, options);
    adapter.log.debug(query);
    if (!multiRequests) {
        if (tasks.length > 100) {
            adapter.log.error('Cannot queue new requests, because more than 100');
            callback && callback('Cannot queue new requests, because more than 100');
        } else {
            tasks.push({operation: 'select', query, options, callback});
            tasks.length === 1 && processTasks();
        }
    } else {
        _getDataFromDB(query, options, callback);
    }
}

function getCounterDataFromDB(db, options, callback) {
    const query = SQLFuncs.getCounterDiff(adapter.config.dbname, options);

    adapter.log.debug(query);

    if (!multiRequests) {
        if (tasks.length > 100) {
            const error = 'Cannot queue new requests, because more than 100';
            adapter.log.error(error);
            callback && callback(error);
        } else {
            tasks.push({operation: 'select', query, options, callback});
            tasks.length === 1 && processTasks();
        }
    } else {
        _getDataFromDB(query, options, callback);
    }
}

function getCounterDiff(msg) {
    const startTime = Date.now();
    const id    = msg.message.id;
    const start = msg.message.options.start || 0;
    const end   = msg.message.options.end   || (Date.now() + 5000000);

    const options = {id, start, end};

    if (!sqlDPs[id]) {
        adapter.sendTo(msg.from, msg.command, {result: [], step: null, error: 'Not enabled'}, msg.callback);
    } else {
        if (!SQLFuncs.getCounterDiff) {
            adapter.sendTo(msg.from, msg.command, {result: [], step: null, error: 'Counter option is not enabled for this type of SQL'}, msg.callback);
        } else {
            getCounterDataFromDB(db, options, (err, data) =>
                commons.sendResponseCounter(adapter, msg, options, (err ? err.toString() : null) || data, startTime));
        }
    }
}

function getHistory(msg) {
    const startTime = Date.now();

    const options = {
        id:         msg.message.id === '*' ? null : msg.message.id,
        start:      msg.message.options.start,
        end:        msg.message.options.end || (Date.now() + 5000000),
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
    if (options.id && aliasMap[options.id]) {
        options.id = aliasMap[options.id];
    }

    if (options.ignoreNull === 'true')  options.ignoreNull = true;  // include nulls and replace them with last value
    if (options.ignoreNull === 'false') options.ignoreNull = false; // include nulls
    if (options.ignoreNull === '0')     options.ignoreNull = 0;     // include nulls and replace them with 0
    if (options.ignoreNull !== true && options.ignoreNull !== false && options.ignoreNull !== 0) {
        options.ignoreNull = false;
    }

    if (!sqlDPs[options.id]) {
        return commons.sendResponse(adapter, msg, options, [], startTime);
    }

    if (options.start > options.end) {
        const _end    = options.end;
        options.end   = options.start;
        options.start =_end;
    }

    if (!options.start && !options.count) {
        options.start = Date.now() - 5030000; // - 1 year
    }

    if (sqlDPs[options.id].type === undefined && sqlDPs[options.id].dbtype !== undefined) {
        if (sqlDPs[options.id][adapter.namespace] && sqlDPs[options.id][adapter.namespace].storageType) {
            if (storageTypes.indexOf(sqlDPs[options.id][adapter.namespace].storageType) === sqlDPs[options.id].dbtype) {
                adapter.log.debug('For getHistory for id ' + options.id + ': Type empty, use storageType dbtype ' + sqlDPs[options.id].dbtype);
                sqlDPs[options.id].type = sqlDPs[options.id].dbtype;
            }
        }
        else {
            adapter.log.debug('For getHistory for id ' + options.id + ': Type empty, use dbtype ' + sqlDPs[options.id].dbtype);
            sqlDPs[options.id].type = sqlDPs[options.id].dbtype;
        }
    }
    if (options.id && sqlDPs[options.id].index === undefined) {
        // read or create in DB
        return getId(options.id, null, err => {
            if (err) {
                adapter.log.warn('Cannot get index of "' + options.id + '": ' + err);
                commons.sendResponse(adapter, msg, options, [], startTime);
            } else {
                getHistory(msg);
            }
        });
    }
    if (options.id && sqlDPs[options.id].type === undefined) {
        adapter.log.warn('For getHistory for id ' + options.id + ': Type empty. Need to write data first. Index = ' + sqlDPs[options.id].index);
        commons.sendResponse(adapter, msg, options, 'Please wait till next data record is logged and reload.', startTime);
        return;
    }
    const type = sqlDPs[options.id].type;
    if (options.id) {
        options.index = options.id;
        options.id = sqlDPs[options.id].index;
    }

    // if specific id requested
    if (options.id || options.id === 0) {
        getDataFromDB(dbNames[type], options, (err, data) =>
            commons.sendResponse(adapter, msg, options, (err ? err.toString() : null) || data, startTime));
    } else {
        // if all IDs requested
        let rows = [];
        let count = 0;
        for (let db = 0; db < dbNames.length; db++) {
            count++;
            getDataFromDB(dbNames[db], options, (err, data) => {
                if (data) {
                    rows = rows.concat(data);
                }
                if (!--count) {
                    rows.sort(sortByTs);
                    commons.sendResponse(adapter, msg, options, rows, startTime);
                }
            });
        }
    }
}

function storeState(msg) {
    if (!msg.message || !msg.message.id || !msg.message.state) {
        adapter.log.error('storeState called with invalid data');
        return adapter.sendTo(msg.from, msg.command, {
            error:  'Invalid call: ' + JSON.stringify(msg)
        }, msg.callback);
    }

    let id;
    if (Array.isArray(msg.message)) {
        for (let i = 0; i < msg.message.length; i++) {
            id = aliasMap[msg.message[i].id] ? aliasMap[msg.message[i].id] : msg.message[i].id;
            pushValueIntoDB(id, msg.message[i].state);
        }
    } else if (Array.isArray(msg.message.state)) {
        for (let j = 0; j < msg.message.state.length; j++) {
            id = aliasMap[msg.message.id] ? aliasMap[msg.message.id] : msg.message.id;
            pushValueIntoDB(id, msg.message.state[j]);
        }
    } else {
        id = aliasMap[msg.message.id] ? aliasMap[msg.message.id] : msg.message.id;
        pushValueIntoDB(id, msg.message.state);
    }

    adapter.sendTo(msg.from, msg.command, {
        success:    true,
        connected:  !!clientPool
    }, msg.callback);
}

function getDpOverview(msg) {
    const result = {};
    const query = SQLFuncs.getIdSelect(adapter.config.dbname);
    adapter.log.info(query);

    clientPool.borrow((err, client) => {
        if (err) {
            return adapter.sendTo(msg.from, msg.command, {
                error:  'Cannot select ' + query + ': ' + err
            }, msg.callback);
        }
        client.execute(query, (err, rows /* , fields */) => {
            if (rows && rows.rows) {
                rows = rows.rows;
            }
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
                for (let r = 0; r < rows.length; r++) {
                    if (!result[rows[r].type]) result[rows[r].type] = {};
                    result[rows[r].type][rows[r].id] = {};

                    result[rows[r].type][rows[r].id].name = rows[r].name;
                    switch(dbNames[rows[r].type]) {
                        case 'ts_number':
                            result[rows[r].type][rows[r].id].type = 'number';
                            break;

                        case 'ts_string':
                            result[rows[r].type][rows[r].id].type = 'string';
                            break;

                        case 'ts_bool':
                            result[rows[r].type][rows[r].id].type = 'boolean';
                            break;
                    }
                }

                adapter.log.info('initialisation result: ' + JSON.stringify(result));
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
            const query = SQLFuncs.getFirstTs(adapter.config.dbname, dbNames[typeId]);
            adapter.log.info(query);

            dbClient.execute(query, (err, rows /* , fields */) => {
                if (rows && rows.rows) {
                    rows = rows.rows;
                }

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
                    for (let r = 0; r < rows.length; r++) {
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
        const result = {};
        for (let ti = 0; ti < dbNames.length; ti++ ) {
            if (resultData[ti]) {
                for (let index in resultData[ti]) {
                    if (!resultData[ti].hasOwnProperty(index)) continue;

                    const id = resultData[ti][index].name;
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
            success: true,
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

    const obj = {};
    obj.common = {};
    obj.common.custom = {};

    if (msg.message.options) {
        obj.common.custom[adapter.namespace] = msg.message.options;
    } else {
        obj.common.custom[adapter.namespace] = {};
    }

    obj.common.custom[adapter.namespace].enabled = true;

    adapter.extendForeignObject(msg.message.id, obj, err => {
        if (err) {
            adapter.log.error('enableHistory: ' + err);
            adapter.sendTo(msg.from, msg.command, {
                error:  err
            }, msg.callback);
        } else {
            adapter.log.info(JSON.stringify(obj));
            adapter.sendTo(msg.from, msg.command, {
                success: true
            }, msg.callback);
        }
    });
}

function disableHistory(msg) {
    if (!msg.message || !msg.message.id) {
        adapter.log.error('disableHistory called with invalid data');
        return adapter.sendTo(msg.from, msg.command, {
            error:  'Invalid call'
        }, msg.callback);
    }

    const obj = {};
    obj.common = {};
    obj.common.custom = {};
    obj.common.custom[adapter.namespace] = {};
    obj.common.custom[adapter.namespace].enabled = false;

    adapter.extendForeignObject(msg.message.id, obj, err => {
        if (err) {
            adapter.log.error('disableHistory: ' + err);
            adapter.sendTo(msg.from, msg.command, {
                error:  err
            }, msg.callback);
        } else {
            adapter.log.info(JSON.stringify(obj));
            adapter.sendTo(msg.from, msg.command, {
                success: true
            }, msg.callback);
        }
    });
}

function getEnabledDPs(msg) {
    const data = {};
    for (const id in sqlDPs) {
        if (sqlDPs.hasOwnProperty(id) && sqlDPs[id] && sqlDPs[id][adapter.namespace]) {
            data[sqlDPs[id].realId] = sqlDPs[id][adapter.namespace];
        }
    }

    adapter.sendTo(msg.from, msg.command, data, msg.callback);
}

function main() {
    setConnected(false);

    

    adapter.config.dbname = adapter.config.dbname || 'iobroker';

    if (adapter.config.writeNulls === undefined) {
        adapter.config.writeNulls = true;
    }

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
    if (!multiRequests) {
        adapter.config.writeNulls = false;
    }

    adapter.config.port = parseInt(adapter.config.port, 10) || 0;

    if (adapter.config.round !== null && adapter.config.round !== undefined) {
        adapter.config.round = Math.pow(10, parseInt(adapter.config.round, 10));
    } else {
        adapter.config.round = null;
    }

    if (adapter.config.dbtype === 'postgresql' && !SQL.PostgreSQLClient) {
        const postgres = require(__dirname + '/lib/postgresql-client');
        for (const attr in postgres) {
            if (postgres.hasOwnProperty(attr) && !SQL[attr]) {
                SQL[attr] = postgres[attr];
            }
        }
    } else
    if (adapter.config.dbtype === 'mssql' && !SQL.MSSQLClient) {
        const mssql = require(__dirname + '/lib/mssql-client');
        for (const attr_ in mssql) {
            if (mssql.hasOwnProperty(attr_) && !SQL[attr_]) {
                SQL[attr_] = mssql[attr_];
            }
        }
    }
    SQLFuncs = require(__dirname + '/lib/' + adapter.config.dbtype);

    if (adapter.config.dbtype === 'sqlite' || adapter.config.host) {
        connect(() => {
            fixSelector(() => {
                // read all custom settings
                adapter.objects.getObjectView('custom', 'state', {}, (err, doc) => {
                    let count = 0;
                    if (doc && doc.rows) {
                        for (let i = 0, l = doc.rows.length; i < l; i++) {
                            if (doc.rows[i].value) {
                                let id = doc.rows[i].id;
                                const realId = id;
                                if (doc.rows[i].value[adapter.namespace] && doc.rows[i].value[adapter.namespace].aliasId) {
                                    aliasMap[id] = doc.rows[i].value[adapter.namespace].aliasId;
                                    adapter.log.debug('Found Alias: ' + id + ' --> ' + aliasMap[id]);
                                    id = aliasMap[id];
                                }

                                let storedIndex = null;
                                let storedType = null;
                                if (sqlDPs[id] && sqlDPs[id].index !== undefined) {
                                    storedIndex = sqlDPs[id].index;
                                }
                                if (sqlDPs[id] && sqlDPs[id].dbtype !== undefined) {
                                    storedType = sqlDPs[id].dbtype;
                                }

                                sqlDPs[id] = doc.rows[i].value;
                                if (storedIndex !== null) {
                                    sqlDPs[id].index = storedIndex;
                                }
                                if (storedType !== null) {
                                    sqlDPs[id].dbtype = storedType;
                                }

                                if (!sqlDPs[id][adapter.namespace]) {
                                    delete sqlDPs[id];
                                } else {
                                    count++;
                                    adapter.log.info('enabled logging of ' + id + ', Alias=' + (id !== realId) + ', ' + count + ' points now activated');

                                    if (sqlDPs[id][adapter.namespace].retention !== undefined && sqlDPs[id][adapter.namespace].retention !== null && sqlDPs[id][adapter.namespace].retention !== '') {
                                        sqlDPs[id][adapter.namespace].retention = parseInt(sqlDPs[id][adapter.namespace].retention, 10) || 0;
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
                                    if (sqlDPs[id][adapter.namespace] && sqlDPs[id][adapter.namespace].changesRelogInterval > 0) {
                                        if (sqlDPs[id].relogTimeout) clearTimeout(sqlDPs[id].relogTimeout);
                                        sqlDPs[id].relogTimeout = setTimeout(reLogHelper, (sqlDPs[id][adapter.namespace].changesRelogInterval * 500 * Math.random()) + sqlDPs[id][adapter.namespace].changesRelogInterval * 500, id);
                                    }

                                    sqlDPs[id].realId  = realId;
                                }
                            }
                        }
                    }

                    adapter.config.writeNulls && writeNulls();

                    if (count < 20) {
                        for (const _id in sqlDPs) {
                            if (sqlDPs.hasOwnProperty(_id)) {
                                adapter.subscribeForeignStates(sqlDPs[_id].realId);
                            }
                        }
                    } else {
                        subscribeAll = true;
                        adapter.subscribeForeignStates('*');
                    }
                    adapter.subscribeForeignObjects('*');
                    adapter.log.debug('Initialization done');
                    setConnected(true);
                    processStartValues();
                });
            });
        });
    }

    prepareStatistic();
}

function prepareStatistic() {
    try {
        adapter.log.debug(`statistic enabled: ${adapter.config.createStatistic}, update interval: ${adapter.config.statisticRefreshInterval}`);
        if (adapter.config.createStatistic) {
            // if configured refresh statistic periodically

            schedule.scheduleJob('0 */' + adapter.config.statisticRefreshInterval + ' * * *', refreshStatistic);
        }
    } catch (err) {
        adapter.log.error(`[refreshStatistic] error: ${err.message}, stack: ${err.stack}`);
    }
}

function createStatisticObjectNumber(id, name, unit){
    adapter.setObjectNotExists(id, {
        type: 'state',
        common: {
            name: name,
            desc: 'sql statistic',
            type: 'number',
            unit: unit,
            read: true,
            write: false
        },
        native: {}
    }, function(err, obj) {
        if (!err && obj) adapter.log.debug('statistic object '+ id +' created');
    });
}

function createStatisticObjectString(id, name){
    adapter.setObjectNotExists(id, {
        type: 'state',
        common: {
            name: name,
            desc: 'sql statistic',
            type: 'string',
            read: true,
            write: false
        },
        native: {}
    }, function(err, obj) {
        if (!err && obj) adapter.log.debug('statistic object '+ id +' created');
    });
}

async function refreshStatistic() {
    try {
        adapter.log.info(`refresh statistics for '${adapter.config.dbtype}', database '${adapter.config.dbname}'`);
        let refreshStart = new Date().getTime();
        let idPrefix = `statistic.databases`

        adapter.log.info(`${adapter.name}.${adapter.instance}`);

        if (connected) {
            let sumAllEntries = 0;
            let sumAllDeadEntries = 0;

            // ioBroker Database size
            let databaseSizeId = `${idPrefix}.${adapter.config.dbname}.size`;
            await createStatisticObjectNumber(databaseSizeId, "size of database", 'MB');

            let databaseEntriesId = `${idPrefix}.${adapter.config.dbname}.dataSets`;
            await createStatisticObjectNumber(databaseEntriesId, "number of all data sets in the database", '');

            let databaseDeadEntriesId = `${idPrefix}.${adapter.config.dbname}.brokenDataSets`;
            await createStatisticObjectNumber(databaseDeadEntriesId, "number of all broken data sets in the database", '');

            let databaseSizeQuery = '';
            if (adapter.config.dbtype !== 'sqlite') {
                databaseSizeQuery = SQLFuncs.getDatabaseSize(adapter.config.dbname);
            } else {
                adapter.log.info('statistic for sqlite database not implemented!');
            }

            if (databaseSizeQuery) {
                let queryResultDbSize = await getQueryResult(databaseSizeQuery);

                if (queryResultDbSize && Object.keys(queryResultDbSize).length === 1) {
                    let databaseSize = Math.round(queryResultDbSize[0].size * 1000) / 1000;
                    adapter.setState(databaseSizeId, databaseSize, true);
                }
            }

            // Table sizes & entries
            let tableSizeQuery = '';
            if (adapter.config.dbtype !== 'sqlite') {
                tableSizeQuery = SQLFuncs.getTablesSize(adapter.config.dbname);
            } else {
                adapter.log.info('statistic for sqlite database not implemented!');
            }

            if (tableSizeQuery) {
                let queryResultTableSize = await getQueryResult(tableSizeQuery);

                adapter.log.info(JSON.stringify(queryResultTableSize));

                if (queryResultTableSize && Object.keys(queryResultTableSize).length > 0) {
                    let tableIdPrefix = `${idPrefix}.${adapter.config.dbname}.tables.`;

                    for (const table of queryResultTableSize) {
                        adapter.log.info(JSON.stringify(table));

                        let tableId = tableIdPrefix + table.name + ".size";
                        await createStatisticObjectNumber(tableId, 'size of table', 'MB');

                        let tableSize = Math.round(table.size * 1000) / 1000;
                        adapter.setState(tableId, tableSize, true);

                        // entries, deadEntries, deadEntriesIds
                        let entriesId = tableIdPrefix + table.name + ".dataSets";
                        await createStatisticObjectNumber(entriesId, 'number of all data sets in the table', '');

                        // not create for table sources
                        let deadEntriesId = tableIdPrefix + table.name + ".brokenDataSets";
                        let deadEntriesStringId = tableIdPrefix + table.name + ".brokenDataSetsIDs";
                        if (dbNames.includes(table.name) || table.name === 'datapoints') {
                            await createStatisticObjectNumber(deadEntriesId, 'number of all broken data sets in the table', '');
                            await createStatisticObjectString(deadEntriesStringId, "id list of broken data sets in the table");
                        }

                        let sumEntries = 0;
                        let sumDeadEntries = 0;
                        let deadEntriesList = [];

                        // table 'datapoints'
                        if (table.name === 'datapoints') {

                            let datapointsQuery = '';
                            if (adapter.config.dbtype === 'mysql') {
                                datapointsQuery = `SELECT id, name FROM ${adapter.config.dbname}.datapoints`;
                            } else if (adapter.config.dbtype === 'mssql') {
                                datapointsQuery = `SELECT id, name FROM ${adapter.config.dbname}.datapoints`;
                            } else if (adapter.config.dbtype === 'postgresql') {
                                datapointsQuery = `SELECT id, name FROM ${adapter.config.dbname}.datapoints`;
                            } else if (adapter.config.dbtype === 'sqlite') {
                                adapter.log.warn('sqlite3 not implemented yet!');
                            }

                            if (datapointsQuery) {
                                let dpResult = await getQueryResult(datapointsQuery);

                                if (dpResult && Object.keys(dpResult).length > 0) {
                                    for (const datapoint of dpResult) {
                                        try {
                                            sumEntries++;

                                            // check if iobroker object for datapoint name exist
                                            let existingDpInIoBroker = await adapter.getForeignObjectAsync(datapoint.name);
                                            if (!existingDpInIoBroker) {
                                                sumDeadEntries++;
                                                deadEntriesList.push({ id: datapoint.id, name: datapoint.name });
                                            } else if (existingDpInIoBroker && existingDpInIoBroker.common) {
                                                adapter.log.info(JSON.stringify(datapoint));
                                                if (existingDpInIoBroker.common === null || (existingDpInIoBroker.common.custom && !existingDpInIoBroker.common.custom.hasOwnProperty(`${adapter.name}.${adapter.instance}`))) {
                                                    sumDeadEntries++;
                                                    deadEntriesList.push(`${datapoint.id}:${datapoint.name}`);
                                                }
                                            }
                                        } catch (ex) {
                                            adapter.log.error(`[refreshStatistic] error: ${ex.message}, stack: ${ex.stack}`);
                                        }
                                    }
                                }
                            }
                        } else {
                            // other tables

                            let entriesQuery = '';
                            if (adapter.config.dbtype === 'mysql') {
                                entriesQuery = `SELECT id, Count(id) as 'count', IF(id NOT IN (select id from ${adapter.config.dbname}.datapoints), 1, 0) as 'dead' FROM ${adapter.config.dbname}.${table.name} GROUP BY id`
                            } else if (adapter.config.dbtype === 'mssql') {
                                entriesQuery = `SELECT id, Count(id) as 'count', IF(id NOT IN (select id from ${adapter.config.dbname}.datapoints), 1, 0) as 'dead' FROM ${adapter.config.dbname}.${table.name} GROUP BY id`;
                            } else if (adapter.config.dbtype === 'postgresql') {
                                entriesQuery = `SELECT id, Count(id) as 'count', IF(id NOT IN (select id from ${adapter.config.dbname}.datapoints), 1, 0) as 'dead' FROM ${adapter.config.dbname}.${table.name} GROUP BY id`;
                            } else if (adapter.config.dbtype === 'sqlite') {
                                adapter.log.warn('sqlite3 not implemented yet!');
                            }

                            if (entriesQuery) {
                                let result = await getQueryResult(entriesQuery);

                                if (result && Object.keys(result).length > 0) {
                                    for (const entry of result) {
                                        sumEntries = sumEntries + entry.count;

                                        // deadEntries for table ts_bool, ts_number, ts_string
                                        if (dbNames.includes(table.name)) {
                                            if (entry.dead === 1) {
                                                sumDeadEntries = sumDeadEntries + entry.count;
                                                deadEntriesList.push({ id: entry.id });
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        sumAllEntries = sumAllEntries + sumEntries;
                        sumAllDeadEntries = sumAllDeadEntries + sumDeadEntries;

                        adapter.setState(entriesId, sumEntries, true);
                        adapter.setState(deadEntriesId, sumDeadEntries, true);
                        if (deadEntriesList.length > 0) {
                            adapter.setState(deadEntriesStringId, JSON.stringify(deadEntriesList), true);
                        } else {
                            adapter.setState(deadEntriesStringId, 'none', true);
                        }
                    }
                }
            }
            adapter.setState(databaseEntriesId, sumAllEntries, true);
            adapter.setState(databaseDeadEntriesId, sumAllDeadEntries, true);

            let lastRefresh = new Date().getTime();
            let duration = Math.round(((lastRefresh - refreshStart) / 1000) * 100) / 100;

            adapter.setState(`${adapter.namespace}.lastRefreshStatistic`, lastRefresh, true);
            adapter.setState(`${adapter.namespace}.lastRefreshStatisticDuration`, duration, true);

            adapter.log.info(`refresh statistics successful!`);
        }
    } catch (err) {
        adapter.log.error(`[refreshStatistic] error: ${err.message}, stack: ${err.stack}`);
    }
}

async function getQueryResult(query) {
    return new Promise((resolve, reject) => {
        adapter.sendTo(adapter.namespace, 'query', query, function (result) {
            if (!result.error) {
                resolve(result.result);
            } else {
                resolve(null);
            }
        });
    });
}


// close connection to DB
process.on('SIGINT', () => finish());

// close connection to DB
process.on('SIGTERM', () => finish());

process.on('uncaughtException', err => adapter.log.warn('Exception: ' + err));

// If started as allInOne/compact mode => return function to create instance
if (module.parent) {
    module.exports = startAdapter;
} else {
    // or start the instance directly
    startAdapter();
}
