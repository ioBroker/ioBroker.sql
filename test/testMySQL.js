var expect = require('chai').expect;
var setup  = require(__dirname + '/lib/setup');

var objects = null;
var states  = null;
var onStateChanged = null;
var onObjectChanged = null;
var sendToID = 1;

var adapterShortName = setup.adapterName.substring(setup.adapterName.indexOf('.')+1);

function checkConnectionOfAdapter(cb, counter) {
    counter = counter || 0;
    if (counter > 20) {
        cb && cb('Cannot check connection');
        return;
    }

    states.getState('system.adapter.' + adapterShortName + '.0.alive', function (err, state) {
        if (err) console.error('MySQL: ' + err);
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
        if (err) console.error('MySQL: ' + err);
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

describe('Test MySQL', function() {
    before('Test MySQL: Start js-controller', function (_done) {
        this.timeout(600000); // because of first install from npm

        setup.setupController(function () {
            var config = setup.getAdapterConfig();
            // enable adapter
            config.common.enabled  = true;
            config.common.loglevel = 'debug';

            config.native.dbtype   = 'mysql';
            config.native.user     = 'root';

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

    it('Test MySQL: Check if adapter started', function (done) {
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
                        obj.common.custom = {
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
    it('Test MySQL: Write values into DB', function (done) {
        this.timeout(10000);
        var now = new Date().getTime();

        states.setState('system.adapter.sql.0.memRss', {val: 1, ts: now - 2000}, function (err) {
            if (err) {
                console.log('MySQL: ' + err);
            }
            setTimeout(function () {
                states.setState('system.adapter.sql.0.memRss', {val: 2, ts: now - 1000}, function (err) {
                    if (err) {
                        console.log('MySQL: ' + err);
                    }
                    setTimeout(function () {
                        states.setState('system.adapter.sql.0.memRss', {val: 3, ts: now}, function (err) {
                            if (err) {
                                console.log('MySQL: ' + err);
                            }
                            setTimeout(function () {
                                done();
                            }, 2000);
                        });
                    }, 500);
                });
            }, 500);
        });
    });
    it('Test MySQL: Read values from DB using query', function (done) {
        this.timeout(10000);

        sendTo('sql.0', 'query', 'SELECT id FROM iobroker.datapoints WHERE name="system.adapter.sql.0.memRss"', function (result) {
            console.log('MySQL: ' + JSON.stringify(result.result, null, 2));
            sendTo('sql.0', 'query', 'SELECT * FROM iobroker.ts_number WHERE id=' + result.result[0].id, function (result) {
                console.log('MySQL: ' + JSON.stringify(result.result, null, 2));
                expect(result.result.length).to.be.at.least(3);
                var found = 0;
                for (var i = 0; i < result.result.length; i++) {
                    if (result.result[i].val >= 1 && result.result[i].val <= 3) found ++;
                }
                expect(found).to.be.equal(3);

                setTimeout(function () {
                    done();
                }, 3000);
            });
        });
    });
    it('Test MySQL: Read values from DB using GetHistory', function (done) {
        this.timeout(10000);

        sendTo('sql.0', 'getHistory', {
            id: 'system.adapter.sql.0.memRss',
            options: {
                start:     new Date().getTime() - 1000000,
                end:       new Date().getTime(),
                count:     50,
                aggregate: 'onchange'
            }
        }, function (result) {
            console.log('MySQL: ' + JSON.stringify(result.result, null, 2));
            expect(result.result.length).to.be.at.least(3);
            var found = 0;
            for (var i = 0; i < result.result.length; i++) {
                if (result.result[i].val >= 1 && result.result[i].val <= 3) found ++;
            }
            expect(found).to.be.equal(3);

            sendTo('sql.0', 'getHistory', {
                id: 'system.adapter.sql.0.memRss',
                options: {
                    start:     new Date().getTime() - 1000000,
                    end:       new Date().getTime(),
                    count:     2,
                    aggregate: 'onchange'
                }
            }, function (result) {
                console.log('MySQL: ' + JSON.stringify(result.result, null, 2));
                expect(result.result.length).to.be.equal(4);
                done();
            });
        });
    });

    after('Test MySQL: Stop js-controller', function (done) {
        this.timeout(6000);

        setup.stopController(function (normalTerminated) {
            console.log('MySQL: Adapter normal terminated: ' + normalTerminated);
            done();
        });
    });
});
