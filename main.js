/* jshint -W097 */// jshint strict:false
/*jslint node: true */
"use strict";

var utils   = require(__dirname + '/lib/utils'); // Get common adapter utils
var SQL     = require('sql-client');
var fs      = require('fs');

var clients = {
    postgresql: {name: 'PostgreSQLClient'},
    mysql:      {name: 'MySQLClient'},
    sqlite:     {name: 'SQLite3Client'}
};

var types = {
    'number':  0,
    'boolean': 0,
    'string':  1
};

var dbNames = [
    'ts_number',
    'ts_string'
];

var sqlDPs  = {};
var clientPool;
var from    = {};

var adapter = utils.adapter('sql');
adapter.on('objectChange', function (id, obj) {
    if (obj && obj.common && obj.common.history && obj.common.history[adapter.namespace]) {
        history[id] = obj.common.history;
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
    }
}

function main() {
    if (!clients[adapter.config.dbtype]) {
        adapter.log.error('Unknown DB type: ' + adapter.config.dbtype);
        adapter.stop();
    }

    // read all history settings
    adapter.objects.getObjectView('history', 'state', {}, function (err, doc) {
        if (doc && doc.rows) {
            for (var i = 0, l = doc.rows.length; i < l; i++) {
                if (doc.rows[i].value) {
                    var id = doc.rows[i].id;
                    sqlDPs[id] = doc.rows[i].value;

                    if (!sqlDPs[id][adapter.namespace]) {
                        delete sqlDPs[id];
                    } else {
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
    });

    adapter.subscribeForeignStates('*');
    adapter.subscribeForeignObjects('*');

    connect();
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
                if (!sqlDPs[_id]) return;
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
            // get list of directories
            var dayList = getDirectories(adapter.config.storeDir).sort(function (a, b) {
                return a - b;
            });
            // calculate date
            d.setSeconds(-(sqlDPs[id][adapter.namespace].retention));
            var day = ts2day(Math.round(d.getTime() / 1000));
            for (var i = 0; i < dayList.length; i++) {
                if (dayList[i] < day) {
                    var file = adapter.config.storeDir + dayList[i] + '/sqlDPs.' + id + '.json';
                    if (fs.existsSync(file)) {
                        adapter.log.info('Delete old sqlDPs "' + file + '"');
                        try {
                            fs.unlinkSync(file);
                        } catch(ex) {
                            adapter.log.error('Cannot delete file "' + file + '": ' + ex);
                        }
                        var files = fs.readdirSync(adapter.config.storeDir + dayList[i]);
                        if (!files.length) {
                            adapter.log.info('Delete old sqlDPs dir "' + adapter.config.storeDir + dayList[i] + '"');
                            try {
                                fs.unlink(adapter.config.storeDir + dayList[i]);
                            } catch(ex) {
                                adapter.log.error('Cannot delete directory "' + adapter.config.storeDir + dayList[i] + '": ' + ex);
                            }
                        }
                    }
                } else {
                    break;
                }
            }
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
    var query = "INSERT INTO iobroker." + dbNames[type] + "(ts,val,ack,_from,q) VALUES(" + state.ts + ", " + state.val + ", " + (state.ack ? 1 : 0) + ", " + (from[state.from] || 0) + ", " + state.q + ");";

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

function aggregate(data, options) {
    if (data && data.length) {
        if (typeof data[0].val !== 'number') {
            return {result: data, step: 0, sourceLength: data.length};
        }
        var start = new Date(options.start * 1000);
        var end   = new Date(options.end * 1000);

        var step = 1; // 1 Step is 1 second
        if (options.step) {
            step = options.step;
        } else{
            step = Math.round((options.end - options.start) / options.count) ;
        }

        // Limit 2000
        if ((options.end - options.start) / step > options.limit){
            step = Math.round((options.end - options.start)/ options.limit);
        }

        var stepEnd;
        var i = 0;
        var result = [];
        var iStep = 0;
        options.aggregate = options.aggregate || 'max';
        
        while (i < data.length && new Date(data[i].ts * 1000) < end) {
            stepEnd = new Date(start);
            var x = stepEnd.getSeconds();
            stepEnd.setSeconds(x + step);

            if (stepEnd < start) {
                // Summer time
                stepEnd.setHours(start.getHours() + 2);
            }

            // find all entries in this time period
            var value = null;
            var count = 0;

            while (i < data.length && new Date(data[i].ts * 1000) < stepEnd) {
                if (options.aggregate == 'max') {
                    // Find max
                    if (value === null || data[i].val > value) value = data[i].val;
                } else if (options.aggregate == 'min') {
                    // Find min
                    if (value === null || data[i].val < value) value = data[i].val;
                } else if (options.aggregate == 'average') {
                    if (value === null) value = 0;
                    value += data[i].val;
                    count++;
                } else if (options.aggregate == 'total') {
                    // Find sum
                    if (value === null) value = 0;
                    value += parseFloat(data[i].val);
                }
                i++;
            }

            if (options.aggregate == 'average') {
                if (!count) {
                    value = null;
                } else {
                    value /= count;
                    value = Math.round(value * 100) / 100;
                }
            }
            if (value !== null || !options.ignoreNull) {
                result[iStep] = {ts: stepEnd.getTime() / 1000};
                result[iStep].val = value;
                iStep++;
            }

            start = stepEnd;
        }

        return {result: result, step: step, sourceLength: data.length};
    } else {
        return {result: [], step: 0, sourceLength: 0};
    }
}

function sortByTs(a, b) {
    var aTs = a.ts;
    var bTs = b.ts;
    return ((aTs < bTs) ? -1 : ((aTs > bTs) ? 1 : 0));
}

function sendResponse(msg, options, data, startTime) {
    var aggregateData;
    data = data.sort(sortByTs);
    if (options.count && !options.start && data.length > options.count) {
        data.splice(0, data.length - options.count);
    }
    if (data[0]) {
        options.start = options.start || data[0].ts;

        if (!options.aggregate || options.aggregate === 'none') {
            aggregateData = {result: data, step: 0, sourceLength: data.length};
        } else {
            aggregateData = aggregate(data, options);
        }

        adapter.log.info('Send: ' + aggregateData.result.length + ' of: ' + aggregateData.sourceLength + ' in: ' + (new Date().getTime() - startTime) + 'ms');
        adapter.sendTo(msg.from, msg.command, {
            result: aggregateData.result,
            step: aggregateData.step,
            error: null
        }, msg.callback);
    } else {
        adapter.log.info('No Data');
        adapter.sendTo(msg.from, msg.command, {result: [].result, step: null, error: null}, msg.callback);
    }
}

function getHistory(msg) {
    var startTime = new Date().getTime();
    var id = msg.message.id;
    var options = {
        start:      msg.message.options.start,
        end:        msg.message.options.end || Math.round((new Date()).getTime() / 1000) + 5000,
        step:       parseInt(msg.message.options.step) || null,
        count:      parseInt(msg.message.options.count) || 500,
        ignoreNull: msg.message.options.ignoreNull,
        aggregate:  msg.message.options.aggregate || 'average', // One of: max, min, average, total
        limit:      msg.message.options.limit || adapter.config.limit || 2000
    };

    if (options.start > options.end){
        var _end = options.end;
        options.end   = options.start;
        options.start =_end;
    }

    if (!options.start && !options.count) {
        options.start = Math.round((new Date()).getTime() / 1000) - 5030; // - 1 year
    }

    getCachedData(id, options, function (cacheData, isFull) {
        // if all data read
        if (isFull && cacheData.length) {
            sendResponse(msg, options, cacheData, startTime);
        } else {
            getFileData(id, options, function (fileData) {
                sendResponse(msg, options, cacheData.concat(fileData), startTime);
            });
        }
    });
}

