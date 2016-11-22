var expect = require('chai').expect;
var setup  = require(__dirname + '/lib/setup');

var objects = null;
var states  = null;
var onStateChanged = null;
var onObjectChanged = null;
var sendToID = 1;

function checkConnectionOfAdapter(cb, counter) {
    counter = counter || 0;
    if (counter > 20) {
        cb && cb('Cannot check connection');
        return;
    }

    states.getState('system.adapter.sql.0.alive', function (err, state) {
        if (err) console.error(err);
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
        if (err) console.error(err);
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

describe('Test SQLite', function() {
    before('Test SQLite: Start js-controller', function (_done) {
        this.timeout(600000); // because of first install from npm

        setup.setupController(function () {
            var config = setup.getAdapterConfig();
            // enable adapter
            config.common.enabled  = true;
            config.common.loglevel = 'debug';

            config.native.dbtype   = 'sqlite';

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

    it('Test SQLite: Check if adapter started', function (done) {
        this.timeout(60000);
        checkConnectionOfAdapter(function () {
            objects.setObject('system.adapter.test.0', {
                    common: {

                    },
                    type: 'instance'
                },
                function () {
                    states.subscribeMessage('system.adapter.test.0');
                    objects.getObject('system.adapter.sql.0.memRss', function (err, obj) {
                        obj.common.history = {
                            'sql.0': {
                                enabled:      true,
                                changesOnly:  false,
                                debounce:     0,
                                retention:    31536000
                            }
                        };
                        objects.setObject('system.adapter.sql.0.memRss', obj, function (err) {
                            // wait till adapter receives the new settings
                            setTimeout(function () {
                                done();
                            }, 3000);
                        });
                });
            });
        });
    });
    after('Test SQLite: Write values into DB', function (done) {
        this.timeout(25000);
        var now = new Date().getTime();

        states.setState('system.adapter.sql.0.memRss', {val: 1, ts: now - 2000}, function (err) {
            if (err) {
                console.log(err);
            }
            setTimeout(function () {
                states.setState('system.adapter.sql.0.memRss', {val: 2, ts: now - 1000}, function (err) {
                    if (err) {
                        console.log(err);
                    }
                    setTimeout(function () {
                        states.setState('system.adapter.sql.0.memRss', {val: 3, ts: now}, function (err) {
                            if (err) {
                                console.log(err);
                            }
                            setTimeout(function () {
                                sendTo('sql.0', 'query', 'SELECT id FROM datapoints WHERE name="system.adapter.sql.0.memRss"', function (result) {
                                    sendTo('sql.0', 'query', 'SELECT * FROM ts_number WHERE id=' + result.result[0].id, function (result) {
                                        console.log(JSON.stringify(result.result, null, 2));
                                        expect(result.result.length).to.be.at.least(3);
                                        var found = 0;
                                        for (var i = 0; i < result.result.length; i++) {
                                            if (result.result[i].val >= 1 && result.result[i].val <= 3) found ++;
                                        }
                                        expect(found).to.be.equal(3);
                                        done();
                                    });
                                });
                            }, 2000);
                        });
                    }, 500);
                });
            }, 500);
        });
    });

    after('Test SQLite: Stop js-controller', function (done) {
        this.timeout(6000);

        setup.stopController(function (normalTerminated) {
            console.log('Adapter normal terminated: ' + normalTerminated);
            done();
        });
    });
});
