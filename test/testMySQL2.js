/* jshint -W097 */// jshint strict:false
/*jslint node: true */
/*jshint expr: true*/
var expect = require('chai').expect;
var setup  = require(__dirname + '/lib/setup');

var objects = null;
var states  = null;
var onStateChanged = null;
var onObjectChanged = null;
var sendToID = 1;

var adapterShortName = setup.adapterName.substring(setup.adapterName.indexOf('.')+1);

var now = new Date().getTime();

function checkConnectionOfAdapter(cb, counter) {
    counter = counter || 0;
    if (counter > 20) {
        cb && cb('Cannot check connection');
        return;
    }

    states.getState('system.adapter.' + adapterShortName + '.0.alive', function (err, state) {
        if (err) console.error('MySQL-with-dash: ' + err);
        if (state && state.val) {
            cb && cb();
        } else {
            setTimeout(function () {
                checkConnectionOfAdapter(cb, counter + 1);
            }, 1000);
        }
    });
}

function checkValueOfState(id, value, cb, counter) {
    counter = counter || 0;
    if (counter > 20) {
        cb && cb('Cannot check value Of State ' + id);
        return;
    }

    states.getState(id, function (err, state) {
        if (err) console.error('MySQL-with-dash: ' + err);
        if (value === null && !state) {
            cb && cb();
        } else
        if (state && (value === undefined || state.val === value)) {
            cb && cb();
        } else {
            setTimeout(function () {
                checkValueOfState(id, value, cb, counter + 1);
            }, 500);
        }
    });
}

function sendTo(target, command, message, callback) {
    onStateChanged = function (id, state) {
        if (id === 'messagebox.system.adapter.test.0') {
            callback(state.message);
        }
    };

    states.pushMessage('system.adapter.' + target, {
        command:    command,
        message:    message,
        from:       'system.adapter.test.0',
        callback: {
            message: message,
            id:      sendToID++,
            ack:     false,
            time:    (new Date()).getTime()
        }
    });
}

describe('Test MySQL-with-dash', function() {
    before('Test MySQL-with-dash: Start js-controller', function (_done) {
        this.timeout(600000); // because of first install from npm
        setup.adapterStarted = false;

        setup.setupController(function () {
            var config = setup.getAdapterConfig();
            // enable adapter
            config.common.enabled  = true;
            config.common.loglevel = 'debug';

            config.native.dbtype   = 'mysql';
            config.native.user     = 'root';
            config.native.dbname   = 'io-broker';
            if (process.env.APPVEYOR && process.env.APPVEYOR==='True') {
                config.native.password = 'Password12!';
            }

            setup.setAdapterConfig(config.common, config.native);

            setup.startController(true, function(id, obj) {}, function (id, state) {
                    if (onStateChanged) onStateChanged(id, state);
                },
                function (_objects, _states) {
                    objects = _objects;
                    states  = _states;
                    _done();
                });
        });
    });

    it('Test MySQL-with-dash: Check if adapter started', function (done) {
        this.timeout(60000);
        checkConnectionOfAdapter(function () {
            objects.setObject('system.adapter.test.0', {
                    common: {

                    },
                    type: 'instance'
                },
                function () {
                    states.subscribeMessage('system.adapter.test.0');
                    sendTo('sql.0', 'enableHistory', {
                        id: 'system.adapter.sql.0.memRss',
                        options: {
                            changesOnly:  true,
                            debounce:     0,
                            retention:    31536000,
                            changesMinDelta: 0.5,
                            storageType: 'Number'
                        }
                    }, function (result) {
                        expect(result.error).to.be.undefined;
                        expect(result.success).to.be.true;
                        sendTo('sql.0', 'enableHistory', {
                            id: 'system.adapter.sql.0.memHeapTotal',
                            options: {
                                changesOnly:  false,
                                debounce:     0,
                                retention:    31536000,
                                storageType: 'String'
                            }
                        }, function (result) {
                            expect(result.error).to.be.undefined;
                            expect(result.success).to.be.true;
                            sendTo('sql.0', 'enableHistory', {
                                id: 'system.adapter.sql.0.uptime',
                                options: {
                                    changesOnly:  false,
                                    debounce:     0,
                                    retention:    31536000,
                                    storageType: 'Boolean'
                                }
                            }, function (result) {
                                expect(result.error).to.be.undefined;
                                expect(result.success).to.be.true;
                                // wait till adapter receives the new settings
                                setTimeout(function () {
                                    done();
                                }, 10000);
                            });
                        });
                    });
                });
        });
    });
    it('Test ' + adapterShortName + ': Check Enabled Points after Enable', function (done) {
        this.timeout(5000);

        sendTo('sql.0', 'getEnabledDPs', {}, function (result) {
            console.log(JSON.stringify(result));
            expect(Object.keys(result).length).to.be.equal(3);
            expect(result['system.adapter.sql.0.memRss'].enabled).to.be.true;
            done();
        });
    });
    it('Test MySQL-with-dash: Write values into DB', function (done) {
        this.timeout(10000);

        this.timeout(10000);

        states.setState('system.adapter.sql.0.memRss', {val: true, ts: now - 20000}, function (err) {
            if (err) {
                console.log(err);
            }
            setTimeout(function () {
                states.setState('system.adapter.sql.0.memRss', {val: 2, ts: now - 10000}, function (err) {
                    if (err) {
                        console.log(err);
                    }
                    setTimeout(function () {
                        states.setState('system.adapter.sql.0.memRss', {val: 2, ts: now - 5000}, function (err) {
                            if (err) {
                                console.log(err);
                            }
                            setTimeout(function () {
                                states.setState('system.adapter.sql.0.memRss', {val: 2.2, ts: now - 4000}, function (err) {
                                    if (err) {
                                        console.log(err);
                                    }
                                    setTimeout(function () {
                                        states.setState('system.adapter.sql.0.memRss', {val: '2.5', ts: now - 3000}, function (err) {
                                            if (err) {
                                                console.log(err);
                                            }
                                            setTimeout(function () {
                                                states.setState('system.adapter.sql.0.memRss', {val: 3, ts: now - 1000}, function (err) {
                                                    if (err) {
                                                        console.log(err);
                                                    }
                                                    setTimeout(function () {
                                                        states.setState('system.adapter.sql.0.memRss', {val: 'Test', ts: now - 500}, function (err) {
                                                            if (err) {
                                                                console.log(err);
                                                            }
                                                            setTimeout(done, 5000);
                                                        });
                                                    }, 100);
                                                });
                                            }, 100);
                                        });
                                    }, 100);
                                });
                            }, 100);
                        });
                    }, 100);
                });
            }, 100);
        });
    });
    it('Test MySQL-with-dash: Read values from DB using query', function (done) {
        this.timeout(10000);

        sendTo('sql.0', 'query', 'SELECT id FROM iobroker.datapoints WHERE name="system.adapter.sql.0.memRss"', function (result) {
            console.log('MySQL-with-dash: ' + JSON.stringify(result.result, null, 2));
            sendTo('sql.0', 'query', 'SELECT * FROM iobroker.ts_number WHERE id=' + result.result[0].id, function (result) {
                console.log('MySQL-with-dash: ' + JSON.stringify(result.result, null, 2));
                expect(result.result.length).to.be.at.least(4);
                var found = 0;
                for (var i = 0; i < result.result.length; i++) {
                    if (result.result[i].val >= 1 && result.result[i].val <= 3) found ++;
                }
                expect(found).to.be.equal(4);

                setTimeout(function () {
                    done();
                }, 3000);
            });
        });
    });
    it('Test MySQL-with-dash: Read values from DB using GetHistory', function (done) {
        this.timeout(10000);

        sendTo('sql.0', 'getHistory', {
            id: 'system.adapter.sql.0.memRss',
            options: {
                start:     now - 30000,
                limit:     50,
                count:     50,
                aggregate: 'none'
            }
        }, function (result) {
            console.log('MySQL-with-dash: ' + JSON.stringify(result.result, null, 2));
            expect(result.result.length).to.be.at.least(4);
            var found = 0;
            for (var i = 0; i < result.result.length; i++) {
                if (result.result[i].val >= 1 && result.result[i].val <= 3) found ++;
            }
            expect(found).to.be.equal(4);

            sendTo('sql.0', 'getHistory', {
                id: 'system.adapter.sql.0.memRss',
                options: {
                    start:     now - 15000,
                    end:       now,
                    limit:     2,
                    count:     2,
                    aggregate: 'none'
                }
            }, function (result) {
                console.log('MySQL-with-dash: ' + JSON.stringify(result.result, null, 2));
                expect(result.result.length).to.be.equal(2);
                var found = 0;
                for (var i = 0; i < result.result.length; i++) {
                    if (result.result[i].val >= 2 && result.result[i].val <= 3) found ++;
                }
                expect(found).to.be.equal(2);
                done();
            });
        });
    });
    it('Test ' + adapterShortName + ': Check Datapoint Types', function (done) {
        this.timeout(5000);

        sendTo('sql.0', 'query', "SELECT name, type FROM iobroker.datapoints", function (result) {
            console.log('MySQL: ' + JSON.stringify(result.result, null, 2));
            expect(result.result.length).to.least(3);
            for (var i = 0; i < result.result.length; i++) {
                if (result.result[i].name === 'system.adapter.sql.0.memRss') {
                    expect(result.result[i].type).to.be.equal(0);
                }
                else if (result.result[i].name === 'system.adapter.sql.0.memHeapTotal') {
                    expect(result.result[i].type).to.be.equal(1);
                }
                else if (result.result[i].name === 'system.adapter.sql.0.uptime') {
                    expect(result.result[i].type).to.be.equal(2);
                }
            }

            setTimeout(function () {
                done();
            }, 3000);
        });
    });
    it('Test ' + adapterShortName + ': Disable Datapoint again', function (done) {
        this.timeout(5000);

        sendTo('sql.0', 'disableHistory', {
            id: 'system.adapter.sql.0.memRss',
        }, function (result) {
            expect(result.error).to.be.undefined;
            expect(result.success).to.be.true;
            done();
        });
    });
    it('Test ' + adapterShortName + ': Check Enabled Points after Disable', function (done) {
        this.timeout(5000);

        sendTo('sql.0', 'getEnabledDPs', {}, function (result) {
            console.log(JSON.stringify(result));
            expect(Object.keys(result).length).to.be.equal(2);
            done();
        });
    });

    after('Test MySQL-with-dash: Stop js-controller', function (done) {
        this.timeout(6000);

        setup.stopController(function (normalTerminated) {
            console.log('MySQL-with-dash: Adapter normal terminated: ' + normalTerminated);
            done();
        });
    });
});
