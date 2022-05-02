/* jshint -W097 */
/* jshint strict: false */
/* jslint node: true */
'use strict';

const utils       = require('@iobroker/adapter-core'); // Get common adapter utils
const adapterName = require('./package.json').name.split('.').pop();
const SQL         = require('sql-client');
const commons     = require('./lib/aggregate');
const fs          = require('fs');
let   SQLFuncs    = null;

const clients = {
    postgresql: {name: 'PostgreSQLClient',  multiRequests: true},
    mysql:      {name: 'MySQL2Client',       multiRequests: true},
    sqlite:     {name: 'SQLite3Client',     multiRequests: false},
    mssql:      {name: 'MSSQLClient',       multiRequests: true}
};

const types   = {
    number:  0,
    string:  1,
    boolean: 2,
    object:  1
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
const MAXTASKS      = 100;
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
let reconnectTimeout = null;
let testConnectTimeout = null;
let dpOverviewTimeout = null;

function isEqual(a, b) {
    //console.log('Compare ' + JSON.stringify(a) + ' with ' +  JSON.stringify(b));
    // Create arrays of property names
    if (a === null || a === undefined || b === null || b === undefined) {
        return a === b;
    }

    const aProps = Object.getOwnPropertyNames(a);
    const bProps = Object.getOwnPropertyNames(b);

    // If number of properties is different,
    // objects are not equivalent
    if (aProps.length !== bProps.length) {
        //console.log('num props different: ' + JSON.stringify(aProps) + ' / ' + JSON.stringify(bProps));
        return false;
    }

    for (let i = 0; i < aProps.length; i++) {
        const propName = aProps[i];

        if (typeof a[propName] !== typeof b[propName]) {
            //console.log('type props ' + propName + ' different');
            return false;
        } else
        if (typeof a[propName] === 'object') {
            if (!isEqual(a[propName], b[propName])) {
                return false;
            }
        } else {
            // If values of same property are not equal,
            // objects are not equivalent
            if (a[propName] !== b[propName]) {
                //console.log('props ' + propName + ' different');
                return false;
            }
        }
    }

    // If we made it this far, objects
    // are considered equivalent
    return true;
}

let adapter;
function startAdapter(options) {
    options = options || {};

    Object.assign(options, {name: adapterName});

    adapter = new utils.Adapter(options);

    adapter.on('objectChange', (id, obj) => {
        let tmpState;
        const now = Date.now();
        const formerAliasId = aliasMap[id] ? aliasMap[id] : id;

        if (obj && obj.common && obj.common.custom  && obj.common.custom[adapter.namespace] && typeof obj.common.custom[adapter.namespace] === 'object' && obj.common.custom[adapter.namespace].enabled) {
            const realId = id;
            let checkForRemove = true;

            if (obj.common.custom && obj.common.custom[adapter.namespace] && obj.common.custom[adapter.namespace].aliasId) {
                if (obj.common.custom[adapter.namespace].aliasId !== id) {
                    aliasMap[id] = obj.common.custom[adapter.namespace].aliasId;
                    adapter.log.debug(`Registered Alias: ${id} --> ${aliasMap[id]}`);
                    id = aliasMap[id];
                    checkForRemove = false;
                } else {
                    adapter.log.warn(`Ignoring Alias-ID because identical to ID for ${id}`);
                    obj.common.custom[adapter.namespace].aliasId = '';
                }
            }

            if (checkForRemove && aliasMap[id]) {
                adapter.log.debug(`Removed Alias: ${id} !-> ${aliasMap[id]}`);
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

            if (sqlDPs[id] && sqlDPs[id].index === undefined) {
                getId(id, sqlDPs[id].dbtype, () => reInit(id, realId, formerAliasId, obj));
            } else {
                reInit(id, realId, formerAliasId, obj);
            }
        } else {
            if (aliasMap[id]) {
                adapter.log.debug(`Removed Alias: ${id} !-> ${aliasMap[id]}`);
                delete aliasMap[id];
            }

            id = formerAliasId;

            if (sqlDPs[id] && sqlDPs[id][adapter.namespace]) {
                adapter.log.info(`disabled logging of ${id}`);
                if (sqlDPs[id].relogTimeout) {
                    clearTimeout(sqlDPs[id].relogTimeout);
                    sqlDPs[id].relogTimeout = null;
                }
                if (sqlDPs[id].timeout) {
                    clearTimeout(sqlDPs[id].timeout);
                    sqlDPs[id].timeout = null;
                }

                tmpState = Object.assign({}, sqlDPs[id].state);
                const state = sqlDPs[id].state ? tmpState : null;

                if (sqlDPs[id][adapter.namespace] && adapter.config.writeNulls) {
                    if (sqlDPs[id].skipped && !sqlDPs[id][adapter.namespace].disableSkippedValueLogging) {
                        pushValueIntoDB(id, sqlDPs[id].skipped, false, true);
                        sqlDPs[id].skipped = null;
                    }

                    const nullValue = {val: null, ts: now, lc: now, q: 0x40, from: `system.adapter.${adapter.namespace}`};

                    if (sqlDPs[id][adapter.namespace].changesOnly && state && state.val !== null) {
                        (function (_id, _state, _nullValue) {
                            _state.ts   = now;
                            _state.from = `system.adapter.${adapter.namespace}`;
                            nullValue.ts += 4;
                            nullValue.lc += 4; // because of MS SQL
                            adapter.log.debug(`Write 1/2 "${_state.val}" _id: ${_id}`);
                            pushValueIntoDB(_id, _state, false, true,() => {
                                // terminate values with null to indicate adapter stop. timestamp + 1
                                adapter.log.debug(`Write 2/2 "null" _id: ${_id}`);
                                pushValueIntoDB(_id, _nullValue, () => delete sqlDPs[id][adapter.namespace]);
                            });
                        })(id, state, nullValue);
                    } else {
                        // terminate values with null to indicate adapter stop. timestamp + 1
                        adapter.log.debug(`Write 0 NULL _id: ${id}`);
                        pushValueIntoDB(id, nullValue, () => delete sqlDPs[id][adapter.namespace]);
                    }
                } else {
                    delete sqlDPs[id][adapter.namespace];
                }
            }
        }
    });

    adapter.on('stateChange', (id, state) => {
        id = aliasMap[id] ? aliasMap[id] : id;
        pushHistory(id, state);
    });

    adapter.on('unload', callback => finish(callback));

    adapter.on('ready', () => main());

    adapter.on('message', msg => processMessage(msg));

    return adapter;
}

function borrowClientFromPool(callback) {
    if (typeof callback !== 'function') {
        return;
    }

    if (!clientPool) {
        setConnected(false);
        return callback('No database connection');
    }
    setConnected(true);

    clientPool.borrow((err, client) => {
        if (!err && client) {
            // make sure we always have at least one error listener to prevent crashes
            if (client.on && client.listenerCount && client.listenerCount('error') === 0) {
                client.on('error', err =>
                    adapter.log.warn(`SQL client error: ${err}`));
            }
        }

        callback(err, client);
    });
}

function returnClientToPool(client) {
    return clientPool && clientPool.return(client);
}

function reInit(id, realId, formerAliasId, obj) {

    // maxLength
    if (!obj.common.custom[adapter.namespace].maxLength && obj.common.custom[adapter.namespace].maxLength !== '0' && obj.common.custom[adapter.namespace].maxLength !== 0) {
        obj.common.custom[adapter.namespace].maxLength = parseInt(adapter.config.maxLength, 10) || 960;
    } else {
        obj.common.custom[adapter.namespace].maxLength = parseInt(obj.common.custom[adapter.namespace].maxLength, 10);
    }

    // retention
    if (obj.common.custom[adapter.namespace].retention || obj.common.custom[adapter.namespace].retention === 0) {
        obj.common.custom[adapter.namespace].retention = parseInt(obj.common.custom[adapter.namespace].retention, 10) || 0;
    } else {
        obj.common.custom[adapter.namespace].retention = adapter.config.retention;
    }

    // debounceTime and debounce compatibility handling
    if (!obj.common.custom[adapter.namespace].blockTime && obj.common.custom[adapter.namespace].blockTime !== '0' && obj.common.custom[adapter.namespace].blockTime !== 0) {
        if (!obj.common.custom[adapter.namespace].debounce && obj.common.custom[adapter.namespace].debounce !== '0' && obj.common.custom[adapter.namespace].debounce !== 0) {
            obj.common.custom[adapter.namespace].blockTime = parseInt(adapter.config.blockTime, 10) || 0;
        } else {
            obj.common.custom[adapter.namespace].blockTime = parseInt(obj.common.custom[adapter.namespace].debounce, 10) || 0;
        }
    } else {
        obj.common.custom[adapter.namespace].blockTime = parseInt(obj.common.custom[adapter.namespace].blockTime, 10) || 0;
    }
    if (!obj.common.custom[adapter.namespace].debounceTime && obj.common.custom[adapter.namespace].debounceTime !== '0' && obj.common.custom[adapter.namespace].debounceTime !== 0) {
        obj.common.custom[adapter.namespace].debounceTime = parseInt(adapter.config.debounceTime, 10) || 0;
    } else {
        obj.common.custom[adapter.namespace].debounceTime = parseInt(obj.common.custom[adapter.namespace].debounceTime, 10) || 0;
    }

    // changesOnly
    obj.common.custom[adapter.namespace].changesOnly = obj.common.custom[adapter.namespace].changesOnly === 'true' || obj.common.custom[adapter.namespace].changesOnly === true;

    // ignoreZero
    obj.common.custom[adapter.namespace].ignoreZero = obj.common.custom[adapter.namespace].ignoreZero === 'true' || obj.common.custom[adapter.namespace].ignoreZero === true;

    // round
    if (obj.common.custom[adapter.namespace].round !== null && obj.common.custom[adapter.namespace].round !== undefined && obj.common.custom[adapter.namespace] !== '') {
        obj.common.custom[adapter.namespace].round = parseInt(obj.common.custom[adapter.namespace], 10);
        if (!isFinite(obj.common.custom[adapter.namespace].round) || obj.common.custom[adapter.namespace].round < 0) {
            obj.common.custom[adapter.namespace].round = adapter.config.round;
        } else {
            obj.common.custom[adapter.namespace].round = Math.pow(10, parseInt(obj.common.custom[adapter.namespace].round, 10));
        }
    } else {
        obj.common.custom[adapter.namespace].round = adapter.config.round;
    }

    // ignoreAboveNumber
    if (obj.common.custom[adapter.namespace].ignoreAboveNumber !== undefined && obj.common.custom[adapter.namespace].ignoreAboveNumber !== null && obj.common.custom[adapter.namespace].ignoreAboveNumber !== '') {
        obj.common.custom[adapter.namespace].ignoreAboveNumber = parseFloat(obj.common.custom[adapter.namespace].ignoreAboveNumber) || null;
    }

    // ignoreBelowNumber incl. ignoreBelowZero compatibility handling
    if (obj.common.custom[adapter.namespace].ignoreBelowNumber !== undefined && obj.common.custom[adapter.namespace].ignoreBelowNumber !== null && obj.common.custom[adapter.namespace].ignoreBelowNumber !== '') {
        obj.common.custom[adapter.namespace].ignoreBelowNumber = parseFloat(obj.common.custom[adapter.namespace].ignoreBelowNumber) || null;
    } else if (obj.common.custom[adapter.namespace].ignoreBelowZero === 'true' || obj.common.custom[adapter.namespace].ignoreBelowZero === true) {
        obj.common.custom[adapter.namespace].ignoreBelowNumber = 0;
    }

    // disableSkippedValueLogging
    if (obj.common.custom[adapter.namespace].disableSkippedValueLogging !== undefined && obj.common.custom[adapter.namespace].disableSkippedValueLogging !== null && obj.common.custom[adapter.namespace].disableSkippedValueLogging !== '') {
        obj.common.custom[adapter.namespace].disableSkippedValueLogging = obj.common.custom[adapter.namespace].disableSkippedValueLogging === 'true' || obj.common.custom[adapter.namespace].disableSkippedValueLogging === true;
    } else {
        obj.common.custom[adapter.namespace].disableSkippedValueLogging = adapter.config.disableSkippedValueLogging;
    }

    // enableDebugLogs
    if (obj.common.custom[adapter.namespace].enableDebugLogs !== undefined && obj.common.custom[adapter.namespace].enableDebugLogs !== null && obj.common.custom[adapter.namespace].enableDebugLogs !== '') {
        obj.common.custom[adapter.namespace].enableDebugLogs = obj.common.custom[adapter.namespace].enableDebugLogs === 'true' || obj.common.custom[adapter.namespace].enableDebugLogs === true;
    } else {
        obj.common.custom[adapter.namespace].enableDebugLogs = adapter.config.enableDebugLogs;
    }

    // changesRelogInterval
    if (obj.common.custom[adapter.namespace].changesRelogInterval || obj.common.custom[adapter.namespace].changesRelogInterval === 0) {
        obj.common.custom[adapter.namespace].changesRelogInterval = parseInt(obj.common.custom[adapter.namespace].changesRelogInterval, 10) || 0;
    } else {
        obj.common.custom[adapter.namespace].changesRelogInterval = adapter.config.changesRelogInterval;
    }

    // changesMinDelta
    if (obj.common.custom[adapter.namespace].changesMinDelta || obj.common.custom[adapter.namespace].changesMinDelta === 0) {
        obj.common.custom[adapter.namespace].changesMinDelta = parseFloat(obj.common.custom[adapter.namespace].changesMinDelta.toString().replace(/,/g, '.')) || 0;
    } else {
        obj.common.custom[adapter.namespace].changesMinDelta = adapter.config.changesMinDelta;
    }

    // storageType
    if (!obj.common.custom[adapter.namespace].storageType) {
        obj.common.custom[adapter.namespace].storageType = false;
    }

    // add one day if retention is too small
    if (obj.common.custom[adapter.namespace].retention && obj.common.custom[adapter.namespace].retention <= 604800) {
        obj.common.custom[adapter.namespace].retention += 86400;
    }

    if (sqlDPs[formerAliasId] &&
        sqlDPs[formerAliasId][adapter.namespace] &&
        isEqual(obj.common.custom[adapter.namespace], sqlDPs[formerAliasId][adapter.namespace])) {
        return obj.common.custom[adapter.namespace].enableDebugLogs && adapter.log.debug(`Object ${id} unchanged. Ignore`);
    }

    // relogTimeout
    if (sqlDPs[formerAliasId] && sqlDPs[formerAliasId].relogTimeout) {
        clearTimeout(sqlDPs[formerAliasId].relogTimeout);
        sqlDPs[formerAliasId].relogTimeout = null;
    }

    const writeNull = !sqlDPs[id];
    const state     = sqlDPs[id] ? sqlDPs[id].state   : null;
    const list      = sqlDPs[id] ? sqlDPs[id].list    : null;
    const inFlight  = sqlDPs[id] ? sqlDPs[id].inFlight: null;
    const timeout   = sqlDPs[id] ? sqlDPs[id].timeout : null;
    const ts        = sqlDPs[id] ? sqlDPs[id].ts : null;

    sqlDPs[id]         = obj.common.custom;
    sqlDPs[id].state   = state;
    sqlDPs[id].list    = list || [];
    sqlDPs[id].inFlight = inFlight || {};
    sqlDPs[id].timeout = timeout;
    sqlDPs[id].ts      = ts;
    sqlDPs[id].realId  = realId;

    // changesRelogInterval
    if (sqlDPs[id][adapter.namespace].changesRelogInterval > 0) {
        sqlDPs[id].relogTimeout = setTimeout(reLogHelper, (sqlDPs[id][adapter.namespace].changesRelogInterval * 500 * Math.random()) + sqlDPs[id][adapter.namespace].changesRelogInterval * 500, id);
    }

    if (writeNull && adapter.config.writeNulls) {
        writeNulls(id);
    }

    adapter.log.info(`enabled logging of ${id}, Alias=${id !== realId}`);
}

function setConnected(isConnected) {
    if (connected !== isConnected) {
        connected = isConnected;
        adapter.setState('info.connection', connected, true);
    }
}

let postgresDbCreated = false;
function connect(callback) {
    if (reconnectTimeout) {
        clearTimeout(reconnectTimeout);
        reconnectTimeout = null;
    }

    if (!clientPool) {
        setConnected(false);

        let params = {
            server:     adapter.config.host, // needed for MSSQL
            host:       adapter.config.host, // needed for PostgreSQL , MySQL
            user:       adapter.config.user || '',
            password:   adapter.config.password || '',
            max_idle:   (adapter.config.dbtype === 'sqlite') ? 1 : 2
        };

        if (adapter.config.maxConnections) {
            params.max_active = adapter.config.maxConnections;
            params.max_wait = 10000; // hard code for now
            params.when_exhausted = 'block';
        }

        if (adapter.config.port) {
            params.port = adapter.config.port;
        }

        if (!adapter.config.dbtype) {
            return adapter.log.error('DB Type is not defined!');
        } else
        if (!clients[adapter.config.dbtype] || !clients[adapter.config.dbtype].name) {
            return adapter.log.error(`Unknown type "${adapter.config.dbtype}"`);
        } else
        if (!SQL[clients[adapter.config.dbtype].name]) {
            return adapter.log.error(`SQL package "${clients[adapter.config.dbtype].name}" is not installed.`);
        }

        if (adapter.config.dbtype === 'postgresql') {
            params.database = 'postgres';
            if (adapter.config.encrypt) {
                params.ssl = {
                    rejectUnauthorized: !!adapter.config.rejectUnauthorized
                };
            }
        } else if (adapter.config.dbtype === 'mssql') {
            params.options = {
                encrypt: !!adapter.config.encrypt,
            };
            if (adapter.config.encrypt) {
                params.options.trustServerCertificate = !adapter.config.rejectUnauthorized
            }
        } else if (adapter.config.dbtype === 'mysql') {
            if (adapter.config.encrypt) {
                params.ssl = {
                    rejectUnauthorized: !!adapter.config.rejectUnauthorized
                };
            }
        }

        if (adapter.config.dbtype === 'sqlite') {
            params = getSqlLiteDir(adapter.config.fileName);
        } else
        // special solution for postgres. Connect first to Db "postgres", create new DB "iobroker" and then connect to "iobroker" DB.
        if (adapter.config.dbtype === 'postgresql' && !postgresDbCreated) {
            // connect first to DB postgres and create iobroker DB
            adapter.log.info(`Postgres connection options: ${JSON.stringify(params).replace(params.password, '****')}`);
            const _client = new SQL[clients[adapter.config.dbtype].name](params);
            _client.on && _client.on('error', err =>
                adapter.log.warn(`SQL client error: ${err}`));

            return _client.connect(err => {
                if (err) {
                    adapter.log.error(err);
                    reconnectTimeout && clearTimeout(reconnectTimeout);
                    reconnectTimeout = setTimeout(() => connect(callback), 30000);
                    return;
                }

                if (adapter.config.doNotCreateDatabase) {
                    _client.disconnect();
                    postgresDbCreated = true;
                    reconnectTimeout && clearTimeout(reconnectTimeout);
                    reconnectTimeout = setTimeout(() => connect(callback), 100);
                } else {
                    _client.execute(`CREATE DATABASE ${adapter.config.dbname};`, (err /* , rows, fields */) => {
                        _client.disconnect();
                        if (err && err.code !== '42P04') { // if error not about yet exists
                            postgresDbCreated = false;
                            adapter.log.error(err);
                            reconnectTimeout && clearTimeout(reconnectTimeout);
                            reconnectTimeout = setTimeout(() => connect(callback), 30000);
                        } else {
                            postgresDbCreated = true;
                            reconnectTimeout && clearTimeout(reconnectTimeout);
                            reconnectTimeout = setTimeout(() => connect(callback), 100);
                        }
                    });
                }
            });
        }

        if (adapter.config.dbtype === 'postgresql') {
            params.database = adapter.config.dbname;
        }

        try {
            if (!clients[adapter.config.dbtype].name) {
                adapter.log.error(`Unknown SQL type selected: "${adapter.config.dbtype}"`);
            } else if (!SQL[`${clients[adapter.config.dbtype].name}Pool`]) {
                adapter.log.error(`Selected SQL DB was not installed properly: "${adapter.config.dbtype}". SQLite requires build tools on system. See README.md`);
            } else {
                clientPool = new SQL[`${clients[adapter.config.dbtype].name}Pool`](params);

                return clientPool.open(err => {
                    if (err) {
                        clientPool = null;
                        setConnected(false);
                        adapter.log.error(err);
                        reconnectTimeout && clearTimeout(reconnectTimeout);
                        reconnectTimeout = setTimeout(() => connect(callback), 30000);
                    } else {
                        reconnectTimeout && clearTimeout(reconnectTimeout);
                        reconnectTimeout = setImmediate(() => connect(callback));
                    }
                });
            }
        } catch (ex) {
            if (ex.toString() === 'TypeError: undefined is not a function') {
                adapter.log.error(`Node.js DB driver for "${adapter.config.dbtype}" could not be installed.`);
            } else {
                adapter.log.error(ex.toString());
                adapter.log.error(ex.stack);
            }
            clientPool = null;
            setConnected(false);
            reconnectTimeout && clearTimeout(reconnectTimeout);
            reconnectTimeout = setTimeout(() => connect(callback), 30000);
            return;
        }
    }

    allScripts(SQLFuncs.init(adapter.config.dbname, adapter.config.doNotCreateDatabase), err => {
        if (err) {
            //adapter.log.error(err);
            reconnectTimeout && clearTimeout(reconnectTimeout);
            reconnectTimeout = setTimeout(() => connect(callback), 30000);
        } else {
            adapter.log.info(`Connected to ${adapter.config.dbtype}`);
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
        const tools = require(`${utils.controllerDir}/lib/tools`);
        let config = tools.getConfigFileName().replace(/\\/g, '/');
        const parts = config.split('/');
        parts.pop();
        config = `${parts.join('/')}/sqlite`;

        // create sqlite directory
        if (!fs.existsSync(config)) {
            fs.mkdirSync(config);
        }

        return `${config}/${fileName}`;
    }
}

function testConnection(msg) {
    if (!msg.message.config) {
        return msg.callback && adapter.sendTo(msg.from, msg.command, {error: 'invalid config'}, msg.callback);
    }

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
    } else if (msg.message.config.dbtype === 'mssql' && !SQL.MSSQLClient) {
        const mssql = require('./lib/mssql-client');
        Object.keys(mssql)
            .filter(attr => !SQL[attr])
            .forEach(attr => SQL[attr] = mssql[attr]);
    } else if (msg.message.config.dbtype === 'mysql' && !SQL.MySQL2Client) {
        const mysql = require('./lib/mysql-client');
        Object.keys(mysql)
            .filter(attr => !SQL[attr])
            .forEach(attr => SQL[attr] = mysql[attr]);
    }

    if (msg.message.config.dbtype === 'postgresql') {
        params.database = 'postgres';
    } else if (msg.message.config.dbtype === 'sqlite') {
        params = getSqlLiteDir(msg.message.config.fileName);
    }

    if (msg.message.config.dbtype === 'postgresql') {
        if (msg.message.config.encrypt) {
            params.ssl = {
                rejectUnauthorized: !!msg.message.config.rejectUnauthorized
            };
        }
    } else if (msg.message.config.dbtype === 'mssql') {
        params.options = {
            encrypt: !!msg.message.config.encrypt,
        };
        if (msg.message.config.encrypt) {
            params.options. trustServerCertificate= !msg.message.config.rejectUnauthorized
        }
    } else if (msg.message.config.dbtype === 'mysql') {
        if (msg.message.config.encrypt) {
            params.ssl = {
                rejectUnauthorized: !!msg.message.config.rejectUnauthorized
            };
        }
    }

    try {
        const client = new SQL[clients[msg.message.config.dbtype].name](params);
        client.on && client.on('error', err =>
            adapter.log.warn(`SQL client error: ${err}`));

        testConnectTimeout = setTimeout(() => {
            testConnectTimeout = null;
            adapter.sendTo(msg.from, msg.command, {error: 'connect timeout'}, msg.callback);
        }, 5000);

        client.connect(err => {
            if (err) {
                if (testConnectTimeout) {
                    clearTimeout(testConnectTimeout);
                    testConnectTimeout = null;
                }
                return adapter.sendTo(msg.from, msg.command, {error: err.toString()}, msg.callback);
            }

            client.execute('SELECT 2 + 3 AS x', (err /* , rows, fields */) => {
                client.disconnect();

                if (testConnectTimeout) {
                    clearTimeout(testConnectTimeout);
                    testConnectTimeout = null;
                    return adapter.sendTo(msg.from, msg.command, {error: err ? err.toString() : null}, msg.callback);
                }
            });
        });
    } catch (ex) {
        if (testConnectTimeout) {
            clearTimeout(testConnectTimeout);
            testConnectTimeout = null;
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
                adapter.sendTo(msg.from, msg.command, {error: null, result: 'deleted'}, msg.callback);
                // restart adapter
                setTimeout(() =>
                    adapter.getForeignObject(`system.adapter.${adapter.namespace}`, (err, obj) => {
                        if (!err) {
                            adapter.setForeignObject(obj._id, obj);
                        } else {
                            adapter.log.error(`Cannot read object "system.adapter.${adapter.namespace}": ${err}`);
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
        if (typeof msg.message !== 'string' || !msg.message.length) {
            throw new Error('No query provided');
        }
        adapter.log.debug(msg.message);

        borrowClientFromPool((err, client) => {
            if (err) {
                adapter.sendTo(msg.from, msg.command, {error: err.toString()}, msg.callback);
                callback && callback();
            } else {
                client.execute(msg.message, (err, rows /* , fields */) => {
                    if (rows && rows.rows) rows = rows.rows;
                    returnClientToPool(client);
                    //convert ts for postgresql and ms sqlserver
                    if (!err && rows && rows[0] && typeof rows[0].ts === 'string') {
                        for (let i = 0; i < rows.length; i++) {
                            rows[i].ts = parseInt(rows[i].ts, 10);
                        }
                    }
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
        if (tasks.length > MAXTASKS) {
            const error = `Cannot queue new requests, because more than ${MAXTASKS}`;
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
        borrowClientFromPool((err, client) => {
            if (err || !client) {
                clientPool && clientPool.close();
                clientPool = null;
                setConnected(false);
                adapter.log.error(err);
                return cb && cb(err);
            }

            adapter.log.debug(script);

            client.execute(script, (err /* , rows, fields */) => {
                adapter.log.debug(`Response: ${JSON.stringify(err)}`);
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
                            adapter.log.debug(`OK. Table "${match[1]}" yet exists`);
                            err = null;
                        } else {
                            adapter.log.error(script);
                            adapter.log.error(err);
                        }
                    } else if (script.startsWith('CREATE INDEX')) {
                        adapter.log.info('Ignore Error on Create index. You might want to create the index yourself!');
                        adapter.log.info(script);
                        adapter.log.info(`${err.code}: ${err}`);
                        err = null;
                    } else {
                        adapter.log.error(script);
                        adapter.log.error(err);
                    }
                }
                returnClientToPool(client);
                cb && cb(err);
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

    function allFinished() {
        if (clientPool) {
            clientPool.close();
            clientPool = null;
            setConnected(false);
        }
        if (typeof finished === 'object') {
            setTimeout(cb => {
                for (let f = 0; f < cb.length; f++) {
                    typeof cb[f] === 'function' && cb[f]();
                }
            }, 500, finished);
            finished = true;
        }
    }

    function finishId(id) {
        if (!sqlDPs[id]) {
            return;
        }
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

        if (sqlDPs[id].skipped && !(sqlDPs[id][adapter.namespace] && sqlDPs[id][adapter.namespace].disableSkippedValueLogging)) {
            count++;
            pushValueIntoDB(id, sqlDPs[id].skipped, false, true,() => {
                if (!--count) {
                    pushValuesIntoDB(id, sqlDPs[id].list, () => {
                        allFinished();
                    });
                }
            });
            sqlDPs[id].skipped = null;
        }

        const nullValue = {val: null, ts: now, lc: now, q: 0x40, from: `system.adapter.${adapter.namespace}`};

        if (sqlDPs[id][adapter.namespace] && adapter.config.writeNulls) {
            if (sqlDPs[id][adapter.namespace].changesOnly && state && state.val !== null) {
                count++;
                (function (_id, _state, _nullValue) {
                    _state.ts   = now;
                    _state.from = `system.adapter.${adapter.namespace}`;
                    nullValue.ts += 4;
                    nullValue.lc += 4; // because of MS SQL
                    adapter.log.debug(`Write 1/2 "${_state.val}" _id: ${_id}`);
                    pushValueIntoDB(_id, _state, false, true,() => {
                        // terminate values with null to indicate adapter stop. timestamp + 1
                        adapter.log.debug(`Write 2/2 "null" _id: ${_id}`);
                        pushValueIntoDB(_id, _nullValue, false, true,() => {
                            if (!--count) {
                                pushValuesIntoDB(id, sqlDPs[id].list, () => {
                                    allFinished();
                                });
                            }
                        });
                    });
                })(id, state, nullValue);
            } else {
                // terminate values with null to indicate adapter stop. timestamp + 1
                count++;
                adapter.log.debug(`Write 0 NULL _id: ${id}`);
                pushValueIntoDB(id, nullValue, false, true,() => {
                    if (!--count) {
                        pushValuesIntoDB(id, sqlDPs[id].list, () => {
                            allFinished();
                        });
                    }
                });
            }
        }
    }

    if (!subscribeAll) {
        for (const _id in sqlDPs) {
            if (sqlDPs.hasOwnProperty(_id) && sqlDPs.hasOwnProperty(sqlDPs[_id].realId)) {
                adapter.unsubscribeForeignStates(sqlDPs[_id].realId);
            }
        }
    } else {
        subscribeAll = false;
        adapter.unsubscribeForeignStates('*');
    }

    if (reconnectTimeout) {
        clearTimeout(reconnectTimeout);
        reconnectTimeout = null;
    }
    if (testConnectTimeout) {
        clearTimeout(testConnectTimeout);
        testConnectTimeout = null;
    }
    if (dpOverviewTimeout) {
        clearTimeout(dpOverviewTimeout);
        dpOverviewTimeout = null;
    }

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
        if (!sqlDPs.hasOwnProperty(id)) {
            continue;
        }
        dpcount++;
        delay += (dpcount%50 === 0) ? 1000: 0;
        setTimeout(finishId, delay, id);
    }

    if (!dpcount && callback) {
        if (clientPool) {
            clientPool.close();
            clientPool = null;
            setConnected(false);
        }
        callback();
    }
}

function processMessage(msg) {
    if (msg.command === 'features') {
        adapter.sendTo(msg.from, msg.command, {supportedFeatures: ['update', 'delete', 'deleteRange', 'deleteAll', 'storeState']}, msg.callback);
    } else
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
    else if (msg.command === 'update') {
        updateState(msg);
    }
    else if (msg.command === 'delete') {
        deleteState(msg);
    }
    else if (msg.command === 'deleteAll') {
        deleteStateAll(msg);
    }
    else if (msg.command === 'deleteRange') {
        deleteState(msg);
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
                setTimeout(() => adapter.stop(), 200);
            }
        });
    }
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
                    from: `system.adapter.${adapter.namespace}`
                });

                if (state) {
                    state.ts = now;
                    state.lc = now;
                    state.from = `system.adapter.${adapter.namespace}`;
                    pushHistory(task.id, state);
                }

                setImmediate(processStartValues);
            });
        }
        else {
            pushHistory(task.id, {
                val:  null,
                ts:   task.now || Date.now(),
                lc:   task.now || Date.now(),
                ack:  true,
                q:    0x40,
                from: `system.adapter.${adapter.namespace}`
            });

            setImmediate(processStartValues);
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

        if (state && state.val === undefined) {
            return adapter.log.warn(`state value undefined received for ${id} which is not allowed. Ignoring.`);
        }

        if (typeof state.val === 'string' && settings.storageType !== 'String') {
            if (isFinite(state.val)) {
                state.val = parseFloat(state.val);
            }
        }

        settings.enableDebugLogs && adapter.log.debug(`new value received for ${id} (storageType ${settings.storageType}), new-value=${state.val}, ts=${state.ts}, relog=${timerRelog}`);

        let ignoreDebonce = false;

        if (!timerRelog) {
            const valueUnstable = !!sqlDPs[id].timeout;
            // When a debounce timer runs and the value is the same as the last one, ignore it
            if (sqlDPs[id].timeout && state.ts !== state.lc) {
                settings.enableDebugLogs && adapter.log.debug(`value not changed debounce ${id}, value=${state.val}, ts=${state.ts}, debounce timer keeps running`);
                return;
            } else if (sqlDPs[id].timeout) { // if value changed, clear timer
                settings.enableDebugLogs && adapter.log.debug(`value changed during debounce time ${id}, value=${state.val}, ts=${state.ts}, debounce timer restarted`);
                clearTimeout(sqlDPs[id].timeout);
                sqlDPs[id].timeout = null;
            }

            if (!valueUnstable && settings.blockTime && sqlDPs[id].state && (sqlDPs[id].state.ts + settings.blockTime) > state.ts) {
                settings.enableDebugLogs && adapter.log.debug(`value ignored blockTime ${id}, value=${state.val}, ts=${state.ts}, lastState.ts=${sqlDPs[id].state.ts}, blockTime=${settings.blockTime}`);
                return;
            }

            if (settings.ignoreZero && (state.val === undefined || state.val === null || state.val === 0)) {
                settings.enableDebugLogs && adapter.log.debug(`value ignore because zero or null ${id}, new-value=${state.val}, ts=${state.ts}`);
                return;
            } else
            if (typeof settings.ignoreBelowNumber === 'number' && typeof state.val === 'number' && state.val < settings.ignoreBelowNumber) {
                settings.enableDebugLogs && adapter.log.debug(`value ignored because below ${settings.ignoreBelowNumber} for ${id}, new-value=${state.val}, ts=${state.ts}`);
                return;
            }
            if (typeof settings.ignoreAboveNumber === 'number' && typeof state.val === 'number' && state.val > settings.ignoreAboveNumber) {
                settings.enableDebugLogs && adapter.log.debug(`value ignored because above ${settings.ignoreAboveNumber} for ${id}, new-value=${state.val}, ts=${state.ts}`);
                return;
            }

            if (sqlDPs[id].state && settings.changesOnly) {
                if (settings.changesRelogInterval === 0) {
                    if ((sqlDPs[id].state.val !== null || state.val === null) && state.ts !== state.lc) {
                        // remember new timestamp
                        if (!valueUnstable && !settings.disableSkippedValueLogging) {
                            sqlDPs[id].skipped = state;
                        }
                        settings.enableDebugLogs && adapter.log.debug(`value not changed ${id}, last-value=${sqlDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
                        return;
                    }
                } else if (sqlDPs[id].lastLogTime) {
                    if ((sqlDPs[id].state.val !== null || state.val === null) && (state.ts !== state.lc) && (Math.abs(sqlDPs[id].lastLogTime - state.ts) < settings.changesRelogInterval * 1000)) {
                        // remember new timestamp
                        if (!valueUnstable && !settings.disableSkippedValueLogging) {
                            sqlDPs[id].skipped = state;
                        }
                        settings.enableDebugLogs && adapter.log.debug(`value not changed ${id}, last-value=${sqlDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
                        return;
                    }
                    if (state.ts !== state.lc) {
                        settings.enableDebugLogs && adapter.log.debug(`value-not-changed-relog ${id}, value=${state.val}, lastLogTime=${sqlDPs[id].lastLogTime}, ts=${state.ts}`);
                        ignoreDebonce = true;
                    }
                }
                if (typeof state.val === 'number') {
                    if (
                        sqlDPs[id].state.val !== null &&
                        settings.changesMinDelta !== 0 &&
                        Math.abs(sqlDPs[id].state.val - state.val) < settings.changesMinDelta
                    ) {
                        if (!valueUnstable && !settings.disableSkippedValueLogging) {
                            sqlDPs[id].skipped = state;
                        }
                        settings.enableDebugLogs && adapter.log.debug(`Min-Delta not reached ${id}, last-value=${sqlDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
                        return;
                    } else if (settings.changesMinDelta !== 0) {
                        settings.enableDebugLogs && adapter.log.debug(`Min-Delta reached ${id}, last-value=${sqlDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
                    }
                } else {
                    settings.enableDebugLogs && adapter.log.debug(`Min-Delta ignored because no number ${id}, last-value=${sqlDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
                }
            }
        }

        if (settings.counter && sqlDPs[id].state) {
            if (sqlDPs[id].type !== types.number) {
                adapter.log.error('Counter must have type "number"!');
            } else if (state.val < sqlDPs[id].state.val) {
                // if actual value is less then last seen counter, store both values
                pushValueIntoDB(id, sqlDPs[id].state, true);
                pushValueIntoDB(id, state, true);
            }
        }

        if (sqlDPs[id].relogTimeout) {
            clearTimeout(sqlDPs[id].relogTimeout);
            sqlDPs[id].relogTimeout = null;
        }

        if (timerRelog) {
            state = Object.assign({}, state);
            state.ts = Date.now();
            state.from = `system.adapter.${adapter.namespace}`;
            settings.enableDebugLogs && adapter.log.debug(`timed-relog ${id}, value=${state.val}, lastLogTime=${sqlDPs[id].lastLogTime}, ts=${state.ts}`);
            ignoreDebonce = true;
        } else {
            if (settings.changesOnly && sqlDPs[id].skipped) {
                settings.enableDebugLogs && adapter.log.debug(`Skipped value logged ${id}, value=${sqlDPs[id].skipped.val}, ts=${sqlDPs[id].skipped.ts}`);
                pushHelper(id, sqlDPs[id].skipped);
                sqlDPs[id].skipped = null;
            }
            if (sqlDPs[id].state && ((sqlDPs[id].state.val === null && state.val !== null) || (sqlDPs[id].state.val !== null && state.val === null))) {
                ignoreDebonce = true;
            } else if (!sqlDPs[id].state && state.val === null) {
                ignoreDebonce = true;
            }
        }

        if (settings.debounceTime && !ignoreDebonce && !timerRelog) {
            // Discard changes in de-bounce time to store last stable value
            sqlDPs[id].timeout && clearTimeout(sqlDPs[id].timeout);
            sqlDPs[id].timeout = setTimeout((id, state) => {
                sqlDPs[id].timeout = null;
                sqlDPs[id].state = state;
                sqlDPs[id].lastLogTime = state.ts;
                settings.enableDebugLogs && adapter.log.debug(`Value logged ${id}, value=${sqlDPs[id].state.val}, ts=${sqlDPs[id].state.ts}`);
                pushHelper(id);
                if (settings.changesRelogInterval > 0) {
                    sqlDPs[id].relogTimeout = setTimeout(reLogHelper, settings.changesRelogInterval * 1000, id);
                }
            }, settings.debounceTime, id, state);
        } else {
            if (!timerRelog) {
                sqlDPs[id].state = state;
            }
            sqlDPs[id].lastLogTime = state.ts;

            settings.enableDebugLogs && adapter.log.debug(`Value logged ${id}, value=${sqlDPs[id].state.val}, ts=${sqlDPs[id].state.ts}`);
            pushHelper(id, state);
            if (settings.changesRelogInterval > 0) {
                sqlDPs[id].relogTimeout = setTimeout(reLogHelper, settings.changesRelogInterval * 1000, id);
            }
        }
    }
}

function reLogHelper(_id) {
    if (!sqlDPs[_id]) {
        adapter.log.info(`non-existing id ${_id}`);
    } else {
        sqlDPs[_id].relogTimeout = null;
        if (sqlDPs[_id].skipped) {
            pushHistory(_id, sqlDPs[_id].skipped, true);
        } else if (sqlDPs[_id].state) {
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

function pushHelper(_id, state) {
    if (!sqlDPs[_id] || (!sqlDPs[_id].state&& !state)) {
        return;
    }
    if (!state) {
        state = sqlDPs[_id].state;
    }

    const _settings = sqlDPs[_id][adapter.namespace];

    // if it was not deleted in this time
    if (_settings) {
        if (state.val !== null && (typeof state.val === 'object' || typeof state.val === 'undefined')) {
            state.val = JSON.stringify(state.val);
        }

        if (state.val !== null && state.val !== undefined) {
            _settings.enableDebugLogs && adapter.log.debug(`Datatype ${_id}: Currently: ${typeof state.val}, StorageType: ${_settings.storageType}`);

            if (typeof state.val === 'string' && _settings.storageType !== 'String') {
                _settings.enableDebugLogs && adapter.log.debug(`Do Automatic Datatype conversion for ${_id}`);
                if (isFinite(state.val)) {
                    state.val = parseFloat(state.val);
                } else if (state.val === 'true') {
                    state.val = true;
                } else if (state.val === 'false') {
                    state.val = false;
                }
            }

            if (_settings.storageType === 'String' && typeof state.val !== 'string') {
                state.val = state.val.toString();
            } else if (_settings.storageType === 'Number' && typeof state.val !== 'number') {
                if (typeof state.val === 'boolean') {
                    state.val = state.val ? 1 : 0;
                } else {
                    return adapter.log.info(`Do not store value "${state.val}" for ${_id} because no number`);
                }
            } else if (_settings.storageType === 'Boolean' && typeof state.val !== 'boolean') {
                state.val = !!state.val;
            }
        } else {
            _settings.enableDebugLogs && adapter.log.debug(`Datatype ${_id}: Currently: null`);
        }

        pushValueIntoDB(_id, state);
    }
}

function getAllIds(cb) {
    const query = SQLFuncs.getIdSelect(adapter.config.dbname);
    adapter.log.debug(query);

    borrowClientFromPool((err, client) => {
        if (err) {
            return cb && cb(err);
        }

        client.execute(query, (err, rows /* , fields */) => {
            returnClientToPool(client);
            if (rows && rows.rows) rows = rows.rows;
            if (err) {
                adapter.log.error(`Cannot select ${query}: ${err}`);
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

    borrowClientFromPool((err, client) => {
        if (err) {
            return cb && cb(err);
        }

        client.execute(query, (err, rows /* , fields */) => {
            returnClientToPool(client);
            if (rows && rows.rows) rows = rows.rows;
            if (err) {
                adapter.log.error(`Cannot select ${query}: ${err}`);
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

    borrowClientFromPool((err, client) => {
        if (err) {
            adapter.log.error(err);
            cb && cb();
        } else {
            client.execute(query, (err /* , rows, fields */ ) => {
                returnClientToPool(client);
                err && adapter.log.warn(`Retention: Cannot delete ${query}: ${err}`);
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
                adapter.log.error(`No type ${sqlDPs[id].type} found for ${id}. Retention is not possible.`);
            } else {
                const query = SQLFuncs.retention(adapter.config.dbname, sqlDPs[id].index, dbNames[sqlDPs[id].type], sqlDPs[id][adapter.namespace].retention);

                if (!multiRequests) {
                    if (tasks.length > MAXTASKS) {
                        return adapter.log.error(`Cannot queue new requests, because more than ${MAXTASKS}`);
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

    borrowClientFromPool((err, client) => {
        if (err) {
            adapter.log.error(err);
            cb && cb(); // BF asked (2021.12.14): may be return here err?
        } else {
            client.execute(query, (err /* , rows, fields */) => {
                returnClientToPool(client);
                if (err) {
                    adapter.log.error(`Cannot insert ${query}: ${err} (id: ${id})`);
                } else {
                    checkRetention(id);
                }
                cb && cb(); // BF asked (2021.12.14): may be return here err?
            });
        }
    });
}

function _executeQuery(query, id, cb) {
    adapter.log.debug(query);

    borrowClientFromPool((err, client) => {
        if (err) {
            adapter.log.error(err);
            cb && cb();
        } else {
            client.execute(query, (err /* , rows, fields */) => {
                returnClientToPool(client);
                err && adapter.log.error(`Cannot query ${query}: ${err} (id: ${id})`);
                cb && cb();
            });
        }
    });
}

function processReadTypes() {
    if (tasksReadType && tasksReadType.length) {
        const task = tasksReadType[0];

        if (!sqlDPs[task.id] || !sqlDPs[task.id][adapter.namespace]) {
            adapter.log.warn(`Ignore type lookup for ${task.id} because not enabled anymore`);
            task.cb && task.cb(`Ignore type lookup for ${task.id} because not enabled anymore`)
            task.cb = null;
            return setImmediate(() => {
                tasksReadType.shift();
                processReadTypes();
            });
        }

        adapter.log.debug(`Type set in Def for ${task.id}: ${sqlDPs[task.id][adapter.namespace].storageType}`);

        if (sqlDPs[task.id][adapter.namespace].storageType) {
            sqlDPs[task.id].type = types[sqlDPs[task.id][adapter.namespace].storageType.toLowerCase()];
            adapter.log.debug(`Type (from Def) for ${task.id}: ${sqlDPs[task.id].type}`);
            processVerifyTypes(task);
        } else if (sqlDPs[task.id].dbtype !== undefined) {
            sqlDPs[task.id].type = sqlDPs[task.id].dbtype;
            sqlDPs[task.id][adapter.namespace].storageType = storageTypes[sqlDPs[task.id].type];
            adapter.log.debug(`Type (from DB-Type) for ${task.id}: ${sqlDPs[task.id].type}`);
            processVerifyTypes(task);
        } else {
            adapter.getForeignObject(sqlDPs[task.id].realId, (err, obj) => {
                err && adapter.log.warn(`Error while get Object for Def: ${err}`);

                if (!sqlDPs[task.id] || !sqlDPs[task.id][adapter.namespace]) {
                    adapter.log.warn(`Ignore type lookup for ${task.id} because not enabled anymore`);
                    task.cb && task.cb(`Ignore type lookup for ${task.id} because not enabled anymore`)
                    task.cb = null;
                    return setImmediate(() => {
                        tasksReadType.shift();
                        processReadTypes();
                    });
                } else
                // read type from object
                if (obj && obj.common && obj.common.type && types[obj.common.type.toLowerCase()] !== undefined) {
                    adapter.log.debug(`${obj.common.type.toLowerCase()} / ${types[obj.common.type.toLowerCase()]} / ${JSON.stringify(obj.common)}`);
                    sqlDPs[task.id].type = types[obj.common.type.toLowerCase()];
                    sqlDPs[task.id][adapter.namespace].storageType = storageTypes[sqlDPs[task.id].type];
                    adapter.log.debug(`Type (from Obj) for ${task.id}: ${sqlDPs[task.id].type}`);
                    processVerifyTypes(task);
                } else if (sqlDPs[task.id].type === undefined) {
                    adapter.getForeignState(sqlDPs[task.id].realId, (err, state) => {
                        if (!sqlDPs[task.id] || !sqlDPs[task.id][adapter.namespace]) {
                            adapter.log.warn(`Ignore type lookup for ${task.id} because not enabled anymore`);
                            task.cb && task.cb(`Ignore type lookup for ${task.id} because not enabled anymore`)
                            task.cb = null;
                            return setImmediate(() => {
                                tasksReadType.shift();
                                processReadTypes();
                            });
                        }

                        if (err) {
                            adapter.log.warn(`Store data for ${task.id} as string because no other valid type found`);
                            sqlDPs[task.id].type = 1; // string
                        } else if (state && state.val !== null && state.val !== undefined && types[typeof state.val] !== undefined) {
                            sqlDPs[task.id].type = types[typeof state.val];
                            sqlDPs[task.id][adapter.namespace].storageType = storageTypes[sqlDPs[task.id].type];
                        } else {
                            adapter.log.warn(`Store data for ${task.id} as string because no other valid type found (${state ? (typeof state.val) : 'state not existing'})`);
                            sqlDPs[task.id].type = 1; // string
                        }

                        adapter.log.debug(`Type (from State) for ${task.id}: ${sqlDPs[task.id].type}`);
                        processVerifyTypes(task);
                    });
                } else {
                    // all OK
                    task.cb && task.cb();
                    task.cb = null;
                    tasksReadType.shift();
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

        return borrowClientFromPool((err, client) => {
            if (err) {
                return processVerifyTypes(task);
            }

            client.execute(query, (err, rows /* , fields */) => {
                returnClientToPool(client);
                if (err) {
                    adapter.log.error(`error updating history config for ${task.id} to pin datatype: ${query}: ${err}`);
                } else {
                    adapter.log.info(`changed history configuration to pin detected datatype for ${task.id}`);
                }
                processVerifyTypes(task);
            });
        });
    }

    task.cb && task.cb();
    task.cb = null;

    setTimeout(() => {
        tasksReadType.shift();
        processReadTypes();
    }, 50);
}

function prepareTaskReadDbId(id, state, isCounter, cb) {
    const type = sqlDPs[id].type;

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
                adapter.log.debug(`prepareTaskCheckTypeAndDbId getId Result - isRunning length = ${sqlDPs[_id].isRunning ? sqlDPs[_id].isRunning.length : 'none'}`);
                if (err) {
                    adapter.log.warn(`Cannot get index of "${_id}": ${err}`);
                    sqlDPs[_id].isRunning &&
                        sqlDPs[_id].isRunning.forEach(r => r.cb && r.cb(`Cannot get index of "${r.id}": ${err}`));
                } else {
                    sqlDPs[_id].isRunning &&
                        sqlDPs[_id].isRunning.forEach(r => r.cb && r.cb());
                }

                sqlDPs[_id].isRunning = null;
            });
        } else {
            return;
        }
    }

    // get from
    if (!isCounter && state.from && !from[state.from]) {
        isFromRunning[state.from] = isFromRunning[state.from] || [];
        tmpState = Object.assign({}, state);

        isFromRunning[state.from].push({id, state: tmpState, cb});

        if (isFromRunning[state.from].length === 1) {
            // read or create in DB
            return getFrom(state.from, (err, from) => {
                adapter.log.debug(`prepareTaskCheckTypeAndDbId getFrom ${from} Result - isRunning length = ${isFromRunning[from] ? isFromRunning[from].length : 'none'}`)
                if (err) {
                    adapter.log.warn(`Cannot get "from" for "${from}": ${err}`);
                    isFromRunning[from] &&
                        isFromRunning[from].forEach(f => f.cb && f.cb(`Cannot get "from" for "${from}": ${err}`));
                } else {
                    isFromRunning[from] &&
                        isFromRunning[from].forEach(f => f.cb && f.cb());
                }
                isFromRunning[from] = null;
            });
        } else {
            return;
        }
    }

    state.ts = parseInt(state.ts, 10);

    try {
        if (state.val !== null && (typeof state.val === 'object' || typeof state.val === 'undefined')) {
            state.val = JSON.stringify(state.val);
        }
    } catch (err) {
        const error = `Cannot convert the object value "${id}"`;
        adapter.log.error(error);
        return cb && cb(error);
    }

    cb && cb();
}

function prepareTaskCheckTypeAndDbId(id, state, isCounter, cb) {
    // check if we know about this ID
    if (!sqlDPs[id]) {
        return cb && setImmediate(cb, `Unknown ID: ${id}`);
    }

    // Check sql connection
    if (!clientPool) {
        adapter.log.warn('No Connection to database');
        return cb && setImmediate(cb, 'No Connection to database');
    }
    adapter.log.debug(`prepareTaskCheckTypeAndDbId CALLED for ${id}`);

    // read type of value
    if (sqlDPs[id].type !== undefined) {
        prepareTaskReadDbId(id, state, isCounter, cb)
    } else {
        // read type from DB
        tasksReadType.push({id, state, cb: () => prepareTaskReadDbId(id, state, isCounter, cb)});

        tasksReadType.length === 1 && processReadTypes();
    }
}

function pushValueIntoDB(id, state, isCounter, storeInCacheOnly, cb) {
    if (typeof storeInCacheOnly === 'function') {
        cb = storeInCacheOnly;
        storeInCacheOnly = false;
    }

    if (typeof isCounter === 'function') {
        cb = isCounter;
        isCounter = false;
    }

    if (!sqlDPs[id] || !state) return;

    adapter.log.debug(`pushValueIntoDB called for ${id} (type: ${sqlDPs[id].type}, ID: ${sqlDPs[id].index}) and state: ${JSON.stringify(state)}`);

    prepareTaskCheckTypeAndDbId(id, state, isCounter, err => {
        adapter.log.debug(`pushValueIntoDB-prepareTaskCheckTypeAndDbId RESULT for ${id} (type: ${sqlDPs[id].type}, ID: ${sqlDPs[id].index}) and state: ${JSON.stringify(state)}: ${err}`);
        if (err) {
            return cb && cb(err);
        }

        const type = sqlDPs[id].type;

        // increase timestamp if last is the same
        if (!isCounter && sqlDPs[id].ts && state.ts === sqlDPs[id].ts) {
            state.ts++;
        }

        // remember last timestamp
        sqlDPs[id].ts = state.ts;

        // if it was not deleted in this time
        sqlDPs[id].list = sqlDPs[id].list || [];

        sqlDPs[id].list.push({state, from: from[state.from] || 0, db: isCounter ? 'ts_counter' : dbNames[type]});

        const _settings = sqlDPs[id][adapter.namespace];
        if ((cb || _settings && sqlDPs[id].list.length > _settings.maxLength) && !storeInCacheOnly) {
            _settings.enableDebugLogs && adapter.log.debug(`inserting ${sqlDPs[id].list.length} entries from ${id} to DB`);
            const inFlightId = `${id}_${Date.now()}_${Math.random()}`;
            sqlDPs[id].inFlight = sqlDPs[id].inFlight || {};
            sqlDPs[id].inFlight[inFlightId] = sqlDPs[id].list;
            sqlDPs[id].list = []
            pushValuesIntoDB(id, sqlDPs[id].inFlight[inFlightId], err => {
                delete sqlDPs[id].inFlight[inFlightId];
                cb && cb(err);
            });
        } else if (cb && storeInCacheOnly) {
            setImmediate(cb);
        }
    });
}


function pushValuesIntoDB(id, list, cb) {
    if (!list.length) {
        return cb && setImmediate(cb);
    }

    if (!multiRequests) {
        if (tasks.length > MAXTASKS) {
            const error = `Cannot queue new requests, because more than ${MAXTASKS}`;
            adapter.log.error(error);
            cb && cb(error);
        } else {
            tasks.push({operation: 'insert', index: sqlDPs[id].index, list, id, callback: cb});
            tasks.length === 1 && processTasks();
        }
    } else {
        const query = SQLFuncs.insert(adapter.config.dbname, sqlDPs[id].index, list);
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
        if (tasks[0].operation === 'query') {
            _executeQuery(tasks[0].query, tasks[0].id, () => {
                tasks[0].callback && tasks[0].callback();
                tasks.shift();
                lockTasks = false;
                tasks.length && setTimeout(processTasks, adapter.config.requestInterval);
            });
        } else if (tasks[0].operation === 'insert') {
            const callbacks = [];
            if (tasks[0].callback) {
                callbacks.push(tasks[0].callback);
            }
            for (let i = 1; i < tasks.length; i++) {
                if (tasks[i].operation === 'insert' && tasks[0].index === tasks[i].index) {
                    tasks[0].list = tasks[0].list.concat(tasks[i].list);
                    if (tasks[i].callback) {
                        callbacks.push(tasks[i].callback);
                    }
                    tasks.splice(i, 1);
                    i--;
                }
            }
            const query = SQLFuncs.insert(adapter.config.dbname, tasks[0].index, tasks[0].list);
            _insertValueIntoDB(query, tasks[0].id, () => {
                callbacks.forEach(cb => cb());
                tasks.shift();
                lockTasks = false;
                tasks.length && setTimeout(processTasks, adapter.config.requestInterval);
            });
        } else if (tasks[0].operation === 'select') {
            _getDataFromDB(tasks[0].query, tasks[0].options, (err, rows) => {
                tasks[0].callback && tasks[0].callback(err, rows);
                tasks.shift();
                lockTasks = false;
                tasks.length && setTimeout(processTasks, adapter.config.requestInterval);
            });
        } else if (tasks[0].operation === 'userQuery') {
            _userQuery(tasks[0].msg, () => {
                tasks[0].callback && tasks[0].callback();
                tasks.shift();
                lockTasks = false;
                tasks.length && setTimeout(processTasks, adapter.config.requestInterval);
            });
        } else if (tasks[0].operation === 'delete') {
            _checkRetention(tasks[0].query, () => {
                tasks[0].callback && tasks[0].callback();
                tasks.shift();
                lockTasks = false;
                tasks.length && setTimeout(processTasks, adapter.config.requestInterval);
            });
        } else {
            adapter.log.error(`unknown task: ${tasks[0].operation}`);
            tasks[0].callback && tasks[0].callback();
            tasks.shift();
            lockTasks = false;
            tasks.length && setTimeout(processTasks, adapter.config.requestInterval);
        }
    }
}

// may be it is required to cache all the data in memory
function getId(id, type, cb) {
    let query = SQLFuncs.getIdSelect(adapter.config.dbname, id);
    adapter.log.debug(query);

    borrowClientFromPool((err, client) => {
        if (err) {
            return cb && cb(err, id);
        }

        client.execute(query, (err, rows /* , fields */) => {
            if (rows && rows.rows) {
                rows = rows.rows;
            }

            if (err) {
                returnClientToPool(client);
                adapter.log.error(`Cannot select ${query}: ${err}`);
                return cb && cb(err, id);
            } else if (!rows.length) {
                if (type !== null && type !== undefined) {
                    // insert
                    query = SQLFuncs.getIdInsert(adapter.config.dbname, id, type);

                    adapter.log.debug(query);

                    client.execute(query, (err /* , rows, fields */) => {
                        if (err) {
                            returnClientToPool(client);
                            adapter.log.error(`Cannot insert ${query}: ${err}`);
                            cb && cb(err, id);
                        } else {
                            query = SQLFuncs.getIdSelect(adapter.config.dbname,id);

                            adapter.log.debug(query);

                            client.execute(query, (err, rows /* , fields */) => {
                                returnClientToPool(client);
                                if (rows && rows.rows) {
                                    rows = rows.rows;
                                }

                                if (err) {
                                    adapter.log.error(`Cannot select ${query}: ${err}`);
                                    cb && cb(err, id);
                                } else if (rows[0]) {
                                    sqlDPs[id].index = rows[0].id;
                                    sqlDPs[id].type  = rows[0].type;

                                    cb && cb(null, id);
                                } else {
                                    adapter.log.error(`No result for select ${query}: after insert`);
                                    cb && cb(new Error(`No result for select ${query}: after insert`), id);
                                }
                            });
                        }
                    });
                } else {
                    returnClientToPool(client);
                    cb && cb('id not found', id);
                }
            } else {
                sqlDPs[id].index = rows[0].id;
                if (rows[0].type === null || typeof rows[0].type !== 'number') {
                    sqlDPs[id].type = type;

                    const query = SQLFuncs.getIdUpdate(adapter.config.dbname, sqlDPs[id].index, sqlDPs[id].type);

                    adapter.log.debug(query);

                    client.execute(query, (err, rows /* , fields */) => {
                        returnClientToPool(client);
                        if (err) {
                            adapter.log.error(`error updating history config for ${id} to pin datatype: ${query}: ${err}`);
                        } else {
                            adapter.log.info(`changed history configuration to pin detected datatype for ${id}`);
                        }
                        cb && cb(null, id);
                    });
                } else {
                    returnClientToPool(client);

                    sqlDPs[id].type  = rows[0].type;

                    cb && cb(null, id);
                }
            }
        });
    });
}

// my be it is required to cache all the data in memory
function getFrom(_from, cb) {
    // const sources    = (adapter.config.dbtype !== 'postgresql' ? (adapter.config.dbname + '.') : '') + 'sources';
    let query = SQLFuncs.getFromSelect(adapter.config.dbname, _from);
    adapter.log.debug(query);

    borrowClientFromPool((err, client) => {
        if (err) {
            return cb && cb(err, _from);
        }
        client.execute(query, (err, rows /* , fields */) => {
            if (rows && rows.rows) rows = rows.rows;
            if (err) {
                returnClientToPool(client);
                adapter.log.error(`Cannot select ${query}: ${err}`);
                return cb && cb(err, _from);
            }
            if (!rows.length) {
                // insert
                query = SQLFuncs.getFromInsert(adapter.config.dbname, _from);
                adapter.log.debug(query);
                client.execute(query, (err /* , rows, fields */) => {
                    if (err) {
                        returnClientToPool(client);
                        adapter.log.error(`Cannot insert ${query}: ${err}`);
                        return cb && cb(err, _from);
                    }

                    query = SQLFuncs.getFromSelect(adapter.config.dbname, _from);
                    adapter.log.debug(query);
                    client.execute(query, (err, rows /* , fields */) => {
                        returnClientToPool(client);

                        if (rows && rows.rows) rows = rows.rows;
                        if (err) {
                            adapter.log.error(`Cannot select ${query}: ${err}`);
                            return cb && cb(err, _from);
                        }
                        from[_from] = rows[0].id;

                        cb && cb(null, _from);
                    });
                });
            } else {
                returnClientToPool(client);

                from[_from] = rows[0].id;

                cb && cb(null, _from);
            }
        });
    });
}

function sortByTs(a, b) {
    const aTs = a.ts;
    const bTs = b.ts;
    return aTs < bTs ? -1 : (aTs > bTs ? 1 : 0);
}


function getOneCachedData(id, options, cache, addId) {
    addId = addId || options.addId;

    if (sqlDPs[id]) {
        let res = [];
        for (let inFlightId in sqlDPs[id].inFlight) {
            adapter.log.debug(`getOneCachedData: add ${sqlDPs[id].inFlight[inFlightId].length} inFlight datapoints for ${options.index || options.id}`);
            res = res.concat(sqlDPs[id].inFlight[inFlightId]);
        }
        res = res.concat(sqlDPs[id].list);
        // todo can be optimized
        if (res) {
            let iProblemCount = 0;
            let vLast = null;
            for (let i = res.length - 1; i >= 0 ; i--) {
                if (!res[i] || !res[i].state) {
                    iProblemCount++;
                    continue;
                }
                if (options.start && res[i].state.ts < options.start) {
                    // add one before start
                    cache.unshift(Object.assign({}, res[i].state));
                    break;
                } else if (res[i].state.ts > options.end) {
                    // add one after end
                    vLast = res[i].state;
                    continue;
                }

                if (vLast) {
                    cache.unshift(Object.assign({}, vLast));
                    vLast = null;
                }

                cache.unshift(Object.assign({}, res[i].state));

                if ((options.returnNewestEntries && options.count && cache.length >= options.count) && (options.aggregate === 'onchange' || options.aggregate === '' || options.aggregate === 'none')) {
                    break;
                }
            }

            iProblemCount && adapter.log.warn(`getOneCachedData: got null states ${iProblemCount} times for ${options.index || options.id}`);

            adapter.log.debug(`getOneCachedData: got ${res.length} datapoints for ${options.index || options.id}`);
        } else {
            adapter.log.debug(`getOneCachedData: datapoints for ${options.index || options.id} do not yet exist`);
        }
    }
}

function getCachedData(options, callback) {
    const cache = [];

    if (options.index || options.id) {
        getOneCachedData(options.index || options.id, options, cache);
    } else {
        for (const id in sqlDPs) {
            if (sqlDPs.hasOwnProperty(id)) {
                getOneCachedData(id, options, cache, true);
            }
        }
    }

    let earliestTs = null;
    for (let c = 0; c < cache.length; c++) {
        if (typeof cache[c].ts === 'string') {
            cache[c].ts = parseInt(cache[c].ts, 10);
        }

        if (adapter.common.loglevel === 'debug') {
            cache[c].date = new Date(parseInt(cache[c].ts, 10));
        }
        if (options.ack) {
            cache[c].ack = !!cache[c].ack;
        }
        if (cache[c].val !== null && isFinite(cache[c].val) && options.round) {
            cache[c].val = Math.round(cache[c].val * options.round) / options.round;
        }
        if (sqlDPs[options.index].type === 2) {
            cache[c].val = !!cache[c].val;
        }
        if (options.addId && !cache[c].id && options.id) {
            cache[c].id = options.index || options.id;
        }
        if (cache[c].ts < earliestTs || earliestTs === null) {
            earliestTs = cache[c].ts;
        }
    }

    options.length = cache.length;
    callback(cache, options.returnNewestEntries && options.count && cache.length >= options.count, !!Object.keys(sqlDPs[options.index || options.id].inFlight).length, earliestTs);
}


function _getDataFromDB(query, options, callback) {
    adapter.log.debug(query);

    borrowClientFromPool((err, client) => {
        if (err) {
            return callback && callback(err);
        }
        client.execute(query, (err, rows /* , fields */) => {
            returnClientToPool(client);

            if (!err && rows && rows.rows) rows = rows.rows;

            if (!err && rows) {
                for (let c = 0; c < rows.length; c++) {
                    if (typeof rows[c].ts === 'string') {
                        rows[c].ts = parseInt(rows[c].ts, 10);
                    }

                    if (adapter.common.loglevel === 'debug') {
                        rows[c].date = new Date(parseInt(rows[c].ts, 10));
                    }
                    if (options.ack) {
                        rows[c].ack = !!rows[c].ack;
                    }
                    if (rows[c].val !== null && isFinite(rows[c].val) && options.round) {
                        rows[c].val = Math.round(rows[c].val * options.round) / options.round;
                    }
                    if (sqlDPs[options.index].type === 2) {
                        rows[c].val = !!rows[c].val;
                    }
                    if (options.addId && !rows[c].id && options.id) {
                        rows[c].id = options.index || options.id;
                    }
                }
            }
            callback && callback(err, rows);
        });
    });
}

function getDataFromDB(db, options, callback) {
    const query = SQLFuncs.getHistory(adapter.config.dbname, db, options);
    adapter.log.debug(query);
    if (!multiRequests) {
        if (tasks.length > MAXTASKS) {
            adapter.log.error(`Cannot queue new requests, because more than ${MAXTASKS}`);
            callback && callback(`Cannot queue new requests, because more than ${MAXTASKS}`);
        } else {
            tasks.push({operation: 'select', query, options, callback});
            tasks.length === 1 && processTasks();
        }
    } else {
        _getDataFromDB(query, options, callback);
    }
}

function getCounterDataFromDB(options, callback) {
    const query = SQLFuncs.getCounterDiff(adapter.config.dbname, options);

    adapter.log.debug(query);

    if (!multiRequests) {
        if (tasks.length > MAXTASKS) {
            const error = `Cannot queue new requests, because more than ${MAXTASKS}`;
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


    if (!sqlDPs[id]) {
        adapter.sendTo(msg.from, msg.command, {result: [], step: null, error: 'Not enabled'}, msg.callback);
    } else {
        if (!SQLFuncs.getCounterDiff) {
            adapter.sendTo(msg.from, msg.command, {result: [], step: null, error: 'Counter option is not enabled for this type of SQL'}, msg.callback);
        } else {
            const options = {id: sqlDPs[id].index, start, end, index: id};
            getCounterDataFromDB(options, (err, data) =>
                commons.sendResponseCounter(adapter, msg, options, (err ? err.toString() : null) || data, startTime));
        }
    }
}

function getHistory(msg) {
    const startTime = Date.now();

    if (!msg.message || !msg.message.options) {
        return adapter.sendTo(msg.from, msg.command, {
            error:  'Invalid call. No options for getHistory provided'
        }, msg.callback);
    }

    const options = {
        id:         msg.message.id === '*' ? null : msg.message.id,
        start:      msg.message.options.start,
        end:        msg.message.options.end || (Date.now() + 5000000),
        step:       parseInt(msg.message.options.step, 10)  || null,
        count:      parseInt(msg.message.options.count, 10) || 500,
        ignoreNull: msg.message.options.ignoreNull,
        aggregate:  msg.message.options.aggregate || 'average', // One of: max, min, average, total, none, on-change
        limit:      parseInt(msg.message.options.limit, 10) || parseInt(msg.message.options.count, 10) || adapter.config.limit || 2000,
        from:       msg.message.options.from  || false,
        q:          msg.message.options.q     || false,
        ack:        msg.message.options.ack   || false,
        ms:         msg.message.options.ms    || false,
        addId:      msg.message.options.addId || false,
        sessionId:  msg.message.options.sessionId,
        returnNewestEntries: msg.message.options.returnNewestEntries || false,
        percentile: msg.message.options.aggregate === 'percentile' ? parseInt(msg.message.options.percentile, 10) || 50 : null,
        quantile: msg.message.options.aggregate === 'quantile' ? parseFloat(msg.message.options.quantile) || 0.5 : null,
        integralUnit: msg.message.options.aggregate === 'integral' ? parseInt(msg.message.options.integralUnit, 10) || 60 : null,
        integralInterpolation: msg.message.options.aggregate === 'integral' ? msg.message.options.integralInterpolation || 'none' : null,
        removeBorderValues: msg.message.options.removeBorderValues || false,
    };

    if (!options.start && options.count) {
        options.returnNewestEntries = true;
    }

    if (msg.message.options.round !== null && msg.message.options.round !== undefined && msg.message.options.round !== '') {
        msg.message.options.round = parseInt(msg.message.options.round, 10);
        if (!isFinite(msg.message.options.round) || msg.message.options.round < 0) {
            options.round = adapter.config.round;
        } else {
            options.round = Math.pow(10, parseInt(msg.message.options.round, 10));
        }
    } else {
        options.round = adapter.config.round;
    }

    adapter.log.debug(`getHistory call: ${JSON.stringify(options)}`);

    if (options.id && aliasMap[options.id]) {
        options.id = aliasMap[options.id];
    }

    if (options.aggregate === 'percentile' && options.percentile < 0 || options.percentile > 100) {
        adapter.log.error(`Invalid percentile value: ${options.percentile}, use 50 as default`);
        options.percentile = 50;
    }

    if (options.aggregate === 'quantile' && options.quantile < 0 || options.quantile > 1) {
        adapter.log.error(`Invalid quantile value: ${options.quantile}, use 0.5 as default`);
        options.quantile = 0.5;
    }

    if (options.aggregate === 'integral' && (typeof options.integralUnit !== 'number' || options.integralUnit <= 0)) {
        adapter.log.error(`Invalid integralUnit value: ${options.integralUnit}, use 60s as default`);
        options.integralUnit = 60;
    }

    const debugLog = options.debugLog = !!(sqlDPs[options.id] && sqlDPs[options.id][adapter.namespace] && sqlDPs[options.id][adapter.namespace].enableDebugLogs);

    if (options.ignoreNull === 'true')  options.ignoreNull = true;  // include nulls and replace them with last value
    if (options.ignoreNull === 'false') options.ignoreNull = false; // include nulls
    if (options.ignoreNull === '0')     options.ignoreNull = 0;     // include nulls and replace them with 0
    if (options.ignoreNull !== true && options.ignoreNull !== false && options.ignoreNull !== 0) {
        options.ignoreNull = false;
    }

    if (!sqlDPs[options.id]) {
        return commons.sendResponse(adapter, msg, options, `Logging not activated for ${options.id}`, startTime);
    }

    if (options.start > options.end) {
        const _end    = options.end;
        options.end   = options.start;
        options.start =_end;
    }

    if (!options.start && !options.count) {
        options.start = Date.now() - 2592000000; // - 1 month
    }

    if (sqlDPs[options.id].type === undefined && sqlDPs[options.id].dbtype !== undefined) {
        if (sqlDPs[options.id][adapter.namespace] && sqlDPs[options.id][adapter.namespace].storageType) {
            if (storageTypes.indexOf(sqlDPs[options.id][adapter.namespace].storageType) === sqlDPs[options.id].dbtype) {
                debugLog && adapter.log.debug(`For getHistory for id ${options.id}: Type empty, use storageType dbtype ${sqlDPs[options.id].dbtype}`);
                sqlDPs[options.id].type = sqlDPs[options.id].dbtype;
            }
        }
        else {
            debugLog && adapter.log.debug(`For getHistory for id ${options.id}: Type empty, use dbtype ${sqlDPs[options.id].dbtype}`);
            sqlDPs[options.id].type = sqlDPs[options.id].dbtype;
        }
    }
    if (options.id && sqlDPs[options.id].index === undefined) {
        // read or create in DB
        return getId(options.id, null, err => {
            if (err) {
                adapter.log.warn(`Cannot get index of "${options.id}": ${err}`);
                commons.sendResponse(adapter, msg, options, [], startTime);
            } else {
                getHistory(msg);
            }
        });
    }
    if (options.id && sqlDPs[options.id].type === undefined) {
        adapter.log.warn(`For getHistory for id "${options.id}": Type empty. Need to write data first. Index = ${sqlDPs[options.id].index}`);
        commons.sendResponse(adapter, msg, options, 'Please wait till next data record is logged and reload.', startTime);
        return;
    }

    const type = sqlDPs[options.id].type;
    if (options.id) {
        options.index = options.id;
        options.id = sqlDPs[options.id].index;
    }

    if (options.debugLog) {
        options.log = adapter.log.debug;
    }

    // if specific id requested
    if (options.id) {
        getCachedData(options, (cacheData, isFull, includesInFlightData, earliestTs) => {
            debugLog && adapter.log.debug(`after getCachedData: length = ${cacheData.length}, isFull=${isFull}`);

            // if all data read
            if ((isFull && cacheData.length) && (options.aggregate === 'onchange' || options.aggregate === '' || options.aggregate === 'none')) {
                cacheData = cacheData.sort(sortByTs);
                if (options.count && cacheData.length > options.count && options.aggregate === 'none') {
                    cacheData = cacheData.slice(-options.count);
                    debugLog && adapter.log.debug(`cut cacheData to ${options.count} values`);
                }
                adapter.log.debug(`Send: ${cacheData.length} values in: ${Date.now() - startTime}ms`);

                adapter.sendTo(msg.from, msg.command, {
                    result: cacheData,
                    step:   null,
                    error:  null
                }, msg.callback);
            } else {
                const origEnd = options.end;
                if (includesInFlightData) {
                    options.end = earliestTs;
                }
                // if not all data read
                getDataFromDB(dbNames[type], options, (err, data) => {
                    options.end = origEnd;
                    if (options.aggregate === 'none' && options.count && options.returnNewestEntries) {
                        cacheData = cacheData.reverse()
                        data = cacheData.concat(data);
                    } else {
                        data = data.concat(cacheData);
                    }
                    debugLog && adapter.log.debug(`after getDataFromDB: length = ${data.length}`);
                    if (options.count && data.length > options.count && options.aggregate === 'none' && !options.returnNewestEntries) {
                        if (options.start) {
                            for (let i = 0; i < data.length; i++) {
                                if (data[i].ts < options.start) {
                                    data.splice(i, 1);
                                    i--;
                                } else {
                                    break;
                                }
                            }
                        }
                        data = data.slice(0, options.count);
                        options.debugLog && adapter.log.debug(`pre-cut data to ${options.count} oldest values`);
                    }

                    data.sort(sortByTs);

                    commons.sendResponse(adapter, msg, options, (err ? err.toString() : null) || data, startTime)
                });
            }
        });
    } else {
        // if all IDs requested
        let rows = [];
        let count = 0;
        getCachedData(options, (cacheData, isFull, includesInFlightData, earliestTs) => {
            if (isFull && cacheData.length) {
                cacheData.sort(sortByTs);
                commons.sendResponse(adapter, msg, options, cacheData, startTime);
            } else {
                if (includesInFlightData) {
                    options.end = earliestTs;
                }
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
        });
    }
}

function update(id, state, cb) {
    // first try to find the value in not yet saved data
    let found = false;
    if (sqlDPs[id]) {
        const res = sqlDPs[id].list;
        if (res) {
            for (let i = res.length - 1; i >= 0; i--) {
                if (res[i].state.ts === state.ts) {
                    if (state.val !== undefined) {
                        res[i].state.val = state.val;
                    }
                    if (state.q !== undefined && res[i].q !== undefined) {
                        res[i].state.q = state.q;
                    }
                    if (state.from !== undefined && res[i].from !== undefined) {
                        res[i].state.from = state.from;
                    }
                    if (state.ack !== undefined) {
                        res[i].state.ack = state.ack;
                    }
                    found = true;
                    break;
                }
            }
        }
    }

    if (!found) {
        prepareTaskCheckTypeAndDbId(id, state, false, err => {
            if (err) {
                return cb && cb(err);
            }

            const type = sqlDPs[id].type;

            const query = SQLFuncs.update(adapter.config.dbname, sqlDPs[id].index, state, from[state.from], dbNames[type]);

            if (!multiRequests) {
                if (tasks.length > MAXTASKS) {
                    const error = `Cannot queue new requests, because more than ${MAXTASKS}`;
                    adapter.log.error(error);
                    cb && cb(error);
                } else {
                    tasks.push({operation: 'query', query, id, callback: cb});
                    tasks.length === 1 && processTasks();
                }
            } else {
                _executeQuery(query, id, cb);
            }
        });
    } else {
        cb && cb();
    }
}

function _delete(id, state, cb) {
    // first try to find the value in not yet saved data
    let found = false;
    if (sqlDPs[id]) {
        const res = sqlDPs[id].list;
        if (res) {
            if (!state.ts && !state.start && !state.end) {
                sqlDPs[id].list = [];
            } else {
                for (let i = res.length - 1; i >= 0; i--) {
                    if (state.start && state.end) {
                        if (res[i].state.ts >= state.start && res[i].state.ts <= state.end) {
                            res.splice(i, 1);
                        }
                    } else if (state.start) {
                        if (res[i].state.ts >= state.start) {
                            res.splice(i, 1);
                        }
                    } else if (state.end) {
                        if (res[i].state.ts <= state.end) {
                            res.splice(i, 1);
                        }
                    } else
                    if (res[i].state.ts === state.ts) {
                        res.splice(i, 1);
                        found = true;
                        break;
                    }
                }
            }
        }
    }

    if (!found) {
        prepareTaskCheckTypeAndDbId(id, state, false, err => {
            if (err) {
                return cb && cb(err);
            }

            const type = sqlDPs[id].type;

            let query;
            if (state.start && state.end) {
                query = SQLFuncs.delete(adapter.config.dbname, dbNames[type], sqlDPs[id].index, state.start, state.end);
            } else if (state.ts) {
                query = SQLFuncs.delete(adapter.config.dbname, dbNames[type], sqlDPs[id].index, state.ts);
            } else {
                query = SQLFuncs.delete(adapter.config.dbname, dbNames[type], sqlDPs[id].index);
            }

            if (!multiRequests) {
                if (tasks.length > MAXTASKS) {
                    const error = `Cannot queue new requests, because more than ${MAXTASKS}`;
                    adapter.log.error(error);
                    cb && cb(error);
                } else {
                    tasks.push({operation: 'query', query, id, callback: cb});
                    tasks.length === 1 && processTasks();
                }
            } else {
                _executeQuery(query, id, cb);
            }
        });
    } else {
        cb && cb();
    }
}

function updateState(msg) {
    if (!msg.message) {
        adapter.log.error('updateState called with invalid data');
        return adapter.sendTo(msg.from, msg.command, {
            error: `Invalid call: ${JSON.stringify(msg)}`
        }, msg.callback);
    }
    let id;
    if (Array.isArray(msg.message)) {
        adapter.log.debug(`updateState ${msg.message.length} items`);
        for (let i = 0; i < msg.message.length; i++) {
            id = aliasMap[msg.message[i].id] ? aliasMap[msg.message[i].id] : msg.message[i].id;

            if (msg.message[i].state && typeof msg.message[i].state === 'object') {
                update(id, msg.message[i].state);
            } else {
                adapter.log.warn(`Invalid state for ${JSON.stringify(msg.message[i])}`);
            }
        }
    } else if (msg.message.state && Array.isArray(msg.message.state)) {
        adapter.log.debug(`updateState ${msg.message.state.length} items`);
        id = aliasMap[msg.message.id] ? aliasMap[msg.message.id] : msg.message.id;
        for (let j = 0; j < msg.message.state.length; j++) {
            if (msg.message.state[j] && typeof msg.message.state[j] === 'object') {
                update(id, msg.message.state[j]);
            } else {
                adapter.log.warn(`Invalid state for ${JSON.stringify(msg.message.state[j])}`);
            }
        }
    } else if (msg.message.id && msg.message.state && typeof msg.message.state === 'object') {
        adapter.log.debug('updateState 1 item');
        id = aliasMap[msg.message.id] ? aliasMap[msg.message.id] : msg.message.id;
        return update(id, msg.message.state, () => adapter.sendTo(msg.from, msg.command, {
            success:    true,
            connected:  !!clientPool
        }, msg.callback));
    } else {
        adapter.log.error('updateState called with invalid data');
        return adapter.sendTo(msg.from, msg.command, {
            error: `Invalid call: ${JSON.stringify(msg)}`
        }, msg.callback);
    }

    adapter.sendTo(msg.from, msg.command, {
        success: true,
        connected: !!clientPool
    }, msg.callback);
}

function deleteState(msg) {
    if (!msg.message) {
        adapter.log.error('deleteState called with invalid data');
        return adapter.sendTo(msg.from, msg.command, {
            error: `Invalid call: ${JSON.stringify(msg)}`
        }, msg.callback);
    }
    let id;
    if (Array.isArray(msg.message)) {
        adapter.log.debug(`deleteState ${msg.message.length} items`);
        for (let i = 0; i < msg.message.length; i++) {
            id = aliasMap[msg.message[i].id] ? aliasMap[msg.message[i].id] : msg.message[i].id;

            // {id: 'blabla', ts: 892}
            if (msg.message[i].ts) {
                _delete(id, {ts: msg.message[i].ts});
            } else
            if (msg.message[i].start) {
                if (typeof msg.message[i].start === 'string') {
                    msg.message[i].start = new Date(msg.message[i].start).getTime();
                }
                if (typeof msg.message[i].end === 'string') {
                    msg.message[i].end = new Date(msg.message[i].end).getTime();
                }
                _delete(id, {start: msg.message[i].start, end: msg.message[i].end || Date.now()});
            } else
            if (typeof msg.message[i].state === 'object' && msg.message[i].state && msg.message[i].state.ts) {
                _delete(id, {ts: msg.message[i].state.ts});
            } else
            if (typeof msg.message[i].state === 'object' && msg.message[i].state && msg.message[i].state.start) {
                if (typeof msg.message[i].state.start === 'string') {
                    msg.message[i].state.start = new Date(msg.message[i].state.start).getTime();
                }
                if (typeof msg.message[i].state.end === 'string') {
                    msg.message[i].state.end = new Date(msg.message[i].state.end).getTime();
                }
                _delete(id, {start: msg.message[i].state.start, end: msg.message[i].state.end || Date.now()});
            } else {
                adapter.log.warn(`Invalid state for ${JSON.stringify(msg.message[i])}`);
            }
        }
    } else if (msg.message.state && Array.isArray(msg.message.state)) {
        adapter.log.debug(`deleteState ${msg.message.state.length} items`);
        id = aliasMap[msg.message.id] ? aliasMap[msg.message.id] : msg.message.id;

        for (let j = 0; j < msg.message.state.length; j++) {
            if (msg.message.state[j] && typeof msg.message.state[j] === 'object') {
                if (msg.message.state[j].ts) {
                    _delete(id, {ts: msg.message.state[j].ts});
                } else if (msg.message.state[j].start) {
                    if (typeof msg.message.state[j].start === 'string') {
                        msg.message.state[j].start = new Date(msg.message.state[j].start).getTime();
                    }
                    if (typeof msg.message.state[j].end === 'string') {
                        msg.message.state[j].end = new Date(msg.message.state[j].end).getTime();
                    }
                    _delete(id, {start: msg.message.state[j].start, end: msg.message.state[j].end || Date.now()});
                }
            } else if (msg.message.state[j] && typeof msg.message.state[j] === 'number') {
                _delete(id, {ts: msg.message.state[j]});
            } else {
                adapter.log.warn(`Invalid state for ${JSON.stringify(msg.message.state[j])}`);
            }
        }
    } else if (msg.message.ts && Array.isArray(msg.message.ts)) {
        adapter.log.debug(`deleteState ${msg.message.ts.length} items`);
        id = aliasMap[msg.message.id] ? aliasMap[msg.message.id] : msg.message.id;
        for (let j = 0; j < msg.message.ts.length; j++) {
            if (msg.message.ts[j] && typeof msg.message.ts[j] === 'number') {
                _delete(id, {ts: msg.message.ts[j]});
            } else {
                adapter.log.warn(`Invalid state for ${JSON.stringify(msg.message.ts[j])}`);
            }
        }
    } else if (msg.message.id && msg.message.state && typeof msg.message.state === 'object') {
        adapter.log.debug('deleteState 1 item');
        id = aliasMap[msg.message.id] ? aliasMap[msg.message.id] : msg.message.id;
        return _delete(id, {ts: msg.message.state.ts}, () => adapter.sendTo(msg.from, msg.command, {
            success: true,
            connected: !!clientPool
        }, msg.callback));
    } else if (msg.message.id && msg.message.ts && typeof msg.message.ts === 'number') {
        adapter.log.debug('deleteState 1 item');
        id = aliasMap[msg.message.id] ? aliasMap[msg.message.id] : msg.message.id;
        return _delete(id, {ts: msg.message.ts}, () => adapter.sendTo(msg.from, msg.command, {
            success: true,
            connected: !!clientPool
        }, msg.callback));
    } else {
        adapter.log.error('deleteState called with invalid data');
        return adapter.sendTo(msg.from, msg.command, {error: `Invalid call: ${JSON.stringify(msg)}`}, msg.callback);
    }

    adapter.sendTo(msg.from, msg.command, {
        success: true,
        connected: !!clientPool
    }, msg.callback);
}

function deleteStateAll(msg) {
    if (!msg.message) {
        adapter.log.error('deleteState called with invalid data');
        return adapter.sendTo(msg.from, msg.command, {
            error: `Invalid call: ${JSON.stringify(msg)}`
        }, msg.callback);
    }
    let id;
    if (Array.isArray(msg.message)) {
        adapter.log.debug(`deleteStateAll ${msg.message.length} items`);
        for (let i = 0; i < msg.message.length; i++) {
            id = aliasMap[msg.message[i].id] ? aliasMap[msg.message[i].id] : msg.message[i].id;
            _delete(id, {});
        }
    } else if (msg.message.id) {
        adapter.log.debug('deleteStateAll 1 item');
        id = aliasMap[msg.message.id] ? aliasMap[msg.message.id] : msg.message.id;
        return _delete(id, {}, () => adapter.sendTo(msg.from, msg.command, {
            success: true,
            connected: !!clientPool
        }, msg.callback));
    } else {
        adapter.log.error('deleteStateAll called with invalid data');
        return adapter.sendTo(msg.from, msg.command, {error: `Invalid call: ${JSON.stringify(msg)}`}, msg.callback);
    }

    adapter.sendTo(msg.from, msg.command, {
        success: true,
        connected: !!clientPool
    }, msg.callback);
}

function storeState(msg) {
    if (!msg.message) {
        adapter.log.error('storeState called with invalid data');
        return adapter.sendTo(msg.from, msg.command, {
            error:  `Invalid call: ${JSON.stringify(msg)}`
        }, msg.callback);
    }

    let pushFunc = pushValueIntoDB;
    if (msg.message.rules) {
        pushFunc = pushHistory;
    }

    let id;
    if (Array.isArray(msg.message)) {
        adapter.log.debug(`storeState ${msg.message.length} items`);
        for (let i = 0; i < msg.message.length; i++) {
            id = aliasMap[msg.message[i].id] ? aliasMap[msg.message[i].id] : msg.message[i].id;
            if (msg.message[i].state && typeof msg.message[i].state === 'object') {
                pushFunc(id, msg.message[i].state);
            } else {
                adapter.log.warn(`Invalid state for ${JSON.stringify(msg.message[i])}`);
            }
        }
    } else if (msg.message.state && Array.isArray(msg.message.state)) {
        adapter.log.debug(`storeState ${msg.message.state.length} items`);
        id = aliasMap[msg.message.id] ? aliasMap[msg.message.id] : msg.message.id;
        for (let j = 0; j < msg.message.state.length; j++) {
            if (msg.message.state[j] && typeof msg.message.state[j] === 'object') {
                pushFunc(id, msg.message.state[j]);
            } else {
                adapter.log.warn(`Invalid state for ${JSON.stringify(msg.message.state[j])}`);
            }
        }
    } else if (msg.message.id && msg.message.state && typeof msg.message.state === 'object') {
        adapter.log.debug('storeState 1 item');
        id = aliasMap[msg.message.id] ? aliasMap[msg.message.id] : msg.message.id;
        pushFunc(id, msg.message.state);
    } else {
        adapter.log.error('storeState called with invalid data');
        return adapter.sendTo(msg.from, msg.command, {
            error: `Invalid call: ${JSON.stringify(msg)}`
        }, msg.callback);
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

    borrowClientFromPool((err, client) => {
        if (err) {
            return adapter.sendTo(msg.from, msg.command, {
                error: `Cannot select ${query}: ${err}`
            }, msg.callback);
        }
        client.execute(query, (err, rows /* , fields */) => {
            if (rows && rows.rows) {
                rows = rows.rows;
            }
            if (err) {
                returnClientToPool(client);

                adapter.log.error(`Cannot select ${query}: ${err}`);

                adapter.sendTo(msg.from, msg.command, {
                    error: `Cannot select ${query}: ${err}`
                }, msg.callback);
                return;
            }

            adapter.log.info(`Query result ${JSON.stringify(rows)}`);

            if (rows.length) {
                for (let r = 0; r < rows.length; r++) {
                    if (!result[rows[r].type]) {
                        result[rows[r].type] = {};
                    }

                    result[rows[r].type][rows[r].id] = {};

                    result[rows[r].type][rows[r].id].name = rows[r].name;

                    switch (dbNames[rows[r].type]) {
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

                adapter.log.info(`initialisation result: ${JSON.stringify(result)}`);
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
                    returnClientToPool(dbClient);

                    adapter.log.error(`Cannot select ${query}: ${err}`);
                    adapter.sendTo(msg.from, msg.command, {
                        error: `Cannot select ${query}: ${err}`
                    }, msg.callback);
                    return;
                }

                adapter.log.info(`Query result ${JSON.stringify(rows)}`);

                if (rows.length) {
                    for (let r = 0; r < rows.length; r++) {
                        if (resultData[typeId][rows[r].id]) {
                            resultData[typeId][rows[r].id].ts = rows[r].ts;
                        }
                    }
                }

                adapter.log.info(`enhanced result (${typeId}): ${JSON.stringify(resultData)}`);
                dpOverviewTimeout = setTimeout(() => {
                    dpOverviewTimeout = null;
                    getFirstTsForIds();
                }, 5000, dbClient, typeId + 1, resultData, msg);
            });
        }
    } else {
        returnClientToPool(dbClient);

        adapter.log.info('consolidate data ...');
        const result = {};
        for (let ti = 0; ti < dbNames.length; ti++ ) {
            if (resultData[ti]) {
                for (let index in resultData[ti]) {
                    if (!resultData[ti].hasOwnProperty(index)) {
                        continue;
                    }

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
        adapter.log.info(`Result: ${JSON.stringify(result)}`);
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
            error: 'Invalid call'
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
            adapter.log.error(`enableHistory: ${err}`);
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
            error: 'Invalid call'
        }, msg.callback);
    }

    const obj = {};
    obj.common = {};
    obj.common.custom = {};
    obj.common.custom[adapter.namespace] = {};
    obj.common.custom[adapter.namespace].enabled = false;

    adapter.extendForeignObject(msg.message.id, obj, err => {
        if (err) {
            adapter.log.error(`disableHistory: ${err}`);
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
        if (sqlDPs.hasOwnProperty(id) && sqlDPs[id] && sqlDPs[id][adapter.namespace] && sqlDPs[id][adapter.namespace].enabled) {
            data[sqlDPs[id].realId] = sqlDPs[id][adapter.namespace];
        }
    }

    adapter.sendTo(msg.from, msg.command, data, msg.callback);
}

function main() {
    setConnected(false);

    // set default history if not yet set
    adapter.getForeignObject('system.config', (err, obj) => {
        if (obj && obj.common && !obj.common.defaultHistory) {
            obj.common.defaultHistory = adapter.namespace;
            adapter.setForeignObject('system.config', obj, err => {
                if (err) {
                    adapter.log.error(`Cannot set default history instance: ${err}`);
                } else {
                    adapter.log.info(`Set default history instance to "${adapter.namespace}"`);
                }
            });
        }
    });

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
        adapter.log.error(`Unknown DB type: ${adapter.config.dbtype}`);
        adapter.stop();
    }
    if (adapter.config.multiRequests !== undefined && adapter.config.dbtype !== 'SQLite3Client' && adapter.config.dbtype !== 'sqlite') {
        clients[adapter.config.dbtype].multiRequests = adapter.config.multiRequests;
    }
    if (adapter.config.maxConnections !== undefined && adapter.config.dbtype !== 'SQLite3Client' && adapter.config.dbtype !== 'sqlite') {
        adapter.config.maxConnections = parseInt(adapter.config.maxConnections, 10);
        if (adapter.config.maxConnections !== 0 && !adapter.config.maxConnections) {
            adapter.config.maxConnections = 100;
        }
    } else {
        adapter.config.maxConnections = null;
    }

    if (adapter.config.changesMinDelta !== null && adapter.config.changesMinDelta !== undefined) {
        adapter.config.changesMinDelta = parseFloat(adapter.config.changesMinDelta.toString().replace(/,/g, '.'));
    } else {
        adapter.config.changesMinDelta = 0;
    }

    if (adapter.config.blockTime !== null && adapter.config.blockTime !== undefined) {
        adapter.config.blockTime = parseInt(adapter.config.blockTime, 10) || 0;
    } else {
        if (adapter.config.debounce !== null && adapter.config.debounce !== undefined) {
            adapter.config.debounce = parseInt(adapter.config.debounce, 10) || 0;
        } else {
            adapter.config.blockTime = 0;
        }
    }

    if (adapter.config.debounceTime !== null && adapter.config.debounceTime !== undefined) {
        adapter.config.debounceTime = parseInt(adapter.config.debounceTime, 10) || 0;
    } else {
        adapter.config.debounceTime = 0;
    }

    multiRequests = clients[adapter.config.dbtype].multiRequests;
    if (!multiRequests) {
        adapter.config.writeNulls = false;
    }

    adapter.config.port = parseInt(adapter.config.port, 10) || 0;

    if (adapter.config.round !== null && adapter.config.round !== undefined && adapter.config.round !== '') {
        adapter.config.round = parseInt(adapter.config.round, 10);
        if (!isFinite(adapter.config.round) || adapter.config.round < 0) {
            adapter.config.round = null;
            adapter.log.info(`Invalid round value: ${adapter.config.round} - ignore, do not round values`);
        } else {
            adapter.config.round = Math.pow(10, parseInt(adapter.config.round, 10));
        }
    } else {
        adapter.config.round = null;
    }

    if (adapter.config.dbtype === 'postgresql' && !SQL.PostgreSQLClient) {
        const postgres = require(`${__dirname}/lib/postgresql-client`);
        for (const attr in postgres) {
            if (postgres.hasOwnProperty(attr) && !SQL[attr]) {
                SQL[attr] = postgres[attr];
            }
        }
    } else if (adapter.config.dbtype === 'mssql' && !SQL.MSSQLClient) {
        const mssql = require(`${__dirname}/lib/mssql-client`);
        for (const attr_ in mssql) {
            if (mssql.hasOwnProperty(attr_) && !SQL[attr_]) {
                SQL[attr_] = mssql[attr_];
            }
        }
    } else if (adapter.config.dbtype === 'mysql' && !SQL.MySQL2Client) {
        const mysql = require(`${__dirname}/lib/mysql-client`);
        for (const attr_ in mysql) {
            if (mysql.hasOwnProperty(attr_) && !SQL[attr_]) {
                SQL[attr_] = mysql[attr_];
            }
        }
    }
    SQLFuncs = require(`${__dirname}/lib/${adapter.config.dbtype}`);

    if (adapter.config.dbtype === 'sqlite' || adapter.config.host) {
        connect(() => {
            // read all custom settings
            adapter.getObjectView('system', 'custom' , {}, (err, doc) => {
                let count = 0;
                if (doc && doc.rows) {
                    for (let i = 0, l = doc.rows.length; i < l; i++) {
                        if (doc.rows[i].value) {
                            let id = doc.rows[i].id;
                            const realId = id;
                            if (doc.rows[i].value[adapter.namespace] && doc.rows[i].value[adapter.namespace].aliasId) {
                                aliasMap[id] = doc.rows[i].value[adapter.namespace].aliasId;
                                adapter.log.debug(`Found Alias: ${id} --> ${aliasMap[id]}`);
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

                            if (!sqlDPs[id][adapter.namespace] || typeof sqlDPs[id][adapter.namespace] !== 'object' || sqlDPs[id][adapter.namespace].enabled === false) {
                                delete sqlDPs[id];
                            } else {
                                count++;
                                adapter.log.info(`enabled logging of ${id}, Alias=${id !== realId}, ${count} points now activated`);

                                // maxLength
                                if (!sqlDPs[id][adapter.namespace].maxLength && sqlDPs[id][adapter.namespace].maxLength !== '0' && sqlDPs[id][adapter.namespace].maxLength !== 0) {
                                    sqlDPs[id][adapter.namespace].maxLength = parseInt(adapter.config.maxLength, 10) || 960;
                                } else {
                                    sqlDPs[id][adapter.namespace].maxLength = parseInt(sqlDPs[id][adapter.namespace].maxLength, 10);
                                }

                                // retention
                                if (sqlDPs[id][adapter.namespace].retention !== undefined && sqlDPs[id][adapter.namespace].retention !== null && sqlDPs[id][adapter.namespace].retention !== '') {
                                    sqlDPs[id][adapter.namespace].retention = parseInt(sqlDPs[id][adapter.namespace].retention, 10) || 0;
                                } else {
                                    sqlDPs[id][adapter.namespace].retention = adapter.config.retention;
                                }

                                // debounceTime and debounce compatibility handling
                                if (!sqlDPs[id][adapter.namespace].blockTime && sqlDPs[id][adapter.namespace].blockTime !== '0' && sqlDPs[id][adapter.namespace].blockTime !== 0) {
                                    if (!sqlDPs[id][adapter.namespace].debounce && sqlDPs[id][adapter.namespace].debounce !== '0' && sqlDPs[id][adapter.namespace].debounce !== 0) {
                                        sqlDPs[id][adapter.namespace].blockTime = parseInt(adapter.config.blockTime, 10) || 0;
                                    } else {
                                        sqlDPs[id][adapter.namespace].blockTime = parseInt(sqlDPs[id][adapter.namespace].debounce, 10) || 0;
                                    }
                                } else {
                                    sqlDPs[id][adapter.namespace].blockTime = parseInt(sqlDPs[id][adapter.namespace].blockTime, 10) || 0;
                                }
                                if (!sqlDPs[id][adapter.namespace].debounceTime && sqlDPs[id][adapter.namespace].debounceTime !== '0' && sqlDPs[id][adapter.namespace].debounceTime !== 0) {
                                    sqlDPs[id][adapter.namespace].debounceTime = parseInt(adapter.config.debounceTime, 10) || 0;
                                } else {
                                    sqlDPs[id][adapter.namespace].debounceTime = parseInt(sqlDPs[id][adapter.namespace].debounceTime, 10) || 0;
                                }

                                // changesOnly
                                sqlDPs[id][adapter.namespace].changesOnly = sqlDPs[id][adapter.namespace].changesOnly === 'true' || sqlDPs[id][adapter.namespace].changesOnly === true;

                                // ignoreZero
                                sqlDPs[id][adapter.namespace].ignoreZero = sqlDPs[id][adapter.namespace].ignoreZero === 'true' || sqlDPs[id][adapter.namespace].ignoreZero === true;

                                // round
                                if (sqlDPs[id][adapter.namespace].round !== null && sqlDPs[id][adapter.namespace].round !== undefined && sqlDPs[id][adapter.namespace] !== '') {
                                    sqlDPs[id][adapter.namespace].round = parseInt(sqlDPs[id][adapter.namespace], 10);
                                    if (!isFinite(sqlDPs[id][adapter.namespace].round) || sqlDPs[id][adapter.namespace].round < 0) {
                                        sqlDPs[id][adapter.namespace].round = adapter.config.round;
                                    } else {
                                        sqlDPs[id][adapter.namespace].round = Math.pow(10, parseInt(sqlDPs[id][adapter.namespace].round, 10));
                                    }
                                } else {
                                    sqlDPs[id][adapter.namespace].round = adapter.config.round;
                                }

                                // ignoreAboveNumber
                                if (sqlDPs[id][adapter.namespace].ignoreAboveNumber !== undefined && sqlDPs[id][adapter.namespace].ignoreAboveNumber !== null && sqlDPs[id][adapter.namespace].ignoreAboveNumber !== '') {
                                    sqlDPs[id][adapter.namespace].ignoreAboveNumber = parseFloat(sqlDPs[id][adapter.namespace].ignoreAboveNumber) || null;
                                }

                                // ignoreBelowNumber and ignoreBelowZero compatibility handling
                                if (sqlDPs[id][adapter.namespace].ignoreBelowNumber !== undefined && sqlDPs[id][adapter.namespace].ignoreBelowNumber !== null && sqlDPs[id][adapter.namespace].ignoreBelowNumber !== '') {
                                    sqlDPs[id][adapter.namespace].ignoreBelowNumber = parseFloat(sqlDPs[id][adapter.namespace].ignoreBelowNumber) || null;
                                } else if (sqlDPs[id][adapter.namespace].ignoreBelowZero === 'true' || sqlDPs[id][adapter.namespace].ignoreBelowZero === true) {
                                    sqlDPs[id][adapter.namespace].ignoreBelowNumber = 0;
                                }

                                // disableSkippedValueLogging
                                if (sqlDPs[id][adapter.namespace].disableSkippedValueLogging !== undefined && sqlDPs[id][adapter.namespace].disableSkippedValueLogging !== null && sqlDPs[id][adapter.namespace].disableSkippedValueLogging !== '') {
                                    sqlDPs[id][adapter.namespace].disableSkippedValueLogging = sqlDPs[id][adapter.namespace].disableSkippedValueLogging === 'true' || sqlDPs[id][adapter.namespace].disableSkippedValueLogging === true;
                                } else {
                                    sqlDPs[id][adapter.namespace].disableSkippedValueLogging = adapter.config.disableSkippedValueLogging;
                                }

                                // enableDebugLogs
                                if (sqlDPs[id][adapter.namespace].enableDebugLogs !== undefined && sqlDPs[id][adapter.namespace].enableDebugLogs !== null && sqlDPs[id][adapter.namespace].enableDebugLogs !== '') {
                                    sqlDPs[id][adapter.namespace].enableDebugLogs = sqlDPs[id][adapter.namespace].enableDebugLogs === 'true' || sqlDPs[id][adapter.namespace].enableDebugLogs === true;
                                } else {
                                    sqlDPs[id][adapter.namespace].enableDebugLogs = adapter.config.enableDebugLogs;
                                }

                                // changesRelogInterval
                                if (sqlDPs[id][adapter.namespace].changesRelogInterval !== undefined && sqlDPs[id][adapter.namespace].changesRelogInterval !== null && sqlDPs[id][adapter.namespace].changesRelogInterval !== '') {
                                    sqlDPs[id][adapter.namespace].changesRelogInterval = parseInt(sqlDPs[id][adapter.namespace].changesRelogInterval, 10) || 0;
                                } else {
                                    sqlDPs[id][adapter.namespace].changesRelogInterval = adapter.config.changesRelogInterval;
                                }

                                // changesMinDelta
                                if (sqlDPs[id][adapter.namespace].changesMinDelta !== undefined && sqlDPs[id][adapter.namespace].changesMinDelta !== null && sqlDPs[id][adapter.namespace].changesMinDelta !== '') {
                                    sqlDPs[id][adapter.namespace].changesMinDelta = parseFloat(sqlDPs[id][adapter.namespace].changesMinDelta.toString().replace(/,/g, '.')) || 0;
                                } else {
                                    sqlDPs[id][adapter.namespace].changesMinDelta = adapter.config.changesMinDelta;
                                }

                                // storageType
                                if (!sqlDPs[id][adapter.namespace].storageType) {
                                    sqlDPs[id][adapter.namespace].storageType = false;
                                }

                                // add one day if retention is too small
                                if (sqlDPs[id][adapter.namespace].retention && sqlDPs[id][adapter.namespace].retention <= 604800) {
                                    sqlDPs[id][adapter.namespace].retention += 86400;
                                }

                                // relogTimeout
                                if (sqlDPs[id][adapter.namespace] && sqlDPs[id][adapter.namespace].changesRelogInterval > 0) {
                                    sqlDPs[id].relogTimeout && clearTimeout(sqlDPs[id].relogTimeout);
                                    sqlDPs[id].relogTimeout = setTimeout(reLogHelper, (sqlDPs[id][adapter.namespace].changesRelogInterval * 500 * Math.random()) + sqlDPs[id][adapter.namespace].changesRelogInterval * 500, id);
                                }

                                sqlDPs[id].realId = realId;
                                sqlDPs[id].list = sqlDPs[id].list || [];
                                sqlDPs[id].inFlight = sqlDPs[id].inFlight || {};
                            }
                        }
                    }
                }

                adapter.config.writeNulls && writeNulls();

                if (count < 200) {
                    Object.keys(sqlDPs).forEach(id =>
                        sqlDPs[id] && sqlDPs[id][adapter.namespace] && !sqlDPs[id].realId && adapter.log.warn(`No realID found for ${id}`));

                    // BF (2020.05.20) change it later to
                    // adapter.subscribeForeignStates(Object.keys(sqlDPs).forEach(id => sqlDPs[id] && sqlDPs[id][adapter.namespace] && sqlDPs[id].realId).filter(id => id));

                    Object.keys(sqlDPs).forEach(id =>
                        sqlDPs[id] && sqlDPs[id][adapter.namespace] && sqlDPs[id].realId && adapter.subscribeForeignStates(sqlDPs[id].realId));
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
    }
}

// If started as allInOne/compact mode => return function to create instance
if (module.parent) {
    module.exports = startAdapter;
} else {
    // or start the instance directly
    startAdapter();
}
