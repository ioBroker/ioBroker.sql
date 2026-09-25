/* jshint -W097 */
/* jshint strict: false */
/* jslint node: true */
/* jshint expr: true */
'use strict';

const assert = require('node:assert');

let now;
let preInitTime;
let objects = null;
let states = null;

async function preInit(_objects, _states, sendTo, adapterShortName) {
    objects = _objects;
    states = _states;
    preInitTime = Date.now();

    const instanceName = `${adapterShortName}.0`;
    let obj = {
        common: {
            type: 'number',
            role: 'state',
            custom: {},
        },
        type: 'state',
    };
    obj.common.custom[instanceName] = {
        enabled: true,
        changesOnly: true,
        debounce: 0,
        retention: 31536000,
        maxLength: 3,
        changesMinDelta: 0.5,
    };
    await objects.setObjectAsync(`${instanceName}.testValue`, obj);
    obj = {
        common: {
            type: 'number',
            role: 'state',
            custom: {},
        },
        type: 'state',
    };
    obj.common.custom[instanceName] = {
        enabled: true,
        changesOnly: true,
        changesRelogInterval: 10,
        debounceTime: 500,
        retention: 31536000,
        maxLength: 3,
        changesMinDelta: 0.5,
        ignoreBelowNumber: -1,
        ignoreAboveNumber: 100,
        ignoreZero: true,
        aliasId: `${instanceName}.testValueDebounce alias`,
    };
    await objects.setObjectAsync(`${instanceName}.testValueDebounce`, obj);
    obj = {
        common: {
            type: 'number',
            role: 'state',
            custom: {},
        },
        type: 'state',
    };
    obj.common.custom[instanceName] = {
        enabled: true,
        changesOnly: true,
        changesRelogInterval: 10,
        debounceTime: 500,
        retention: 31536000,
        maxLength: 0,
        changesMinDelta: 0.5,
        disableSkippedValueLogging: true,
        ignoreBelowZero: true,
        ignoreAboveNumber: 100,
        storageType: 'Number',
    };
    await objects.setObjectAsync(`${instanceName}.testValueDebounceRaw`, obj);
    obj = {
        common: {
            type: 'number',
            role: 'state',
            custom: {},
        },
        type: 'state',
    };
    obj.common.custom[instanceName] = {
        enabled: true,
        changesOnly: true,
        changesRelogInterval: 10,
        debounceTime: 0,
        blockTime: 1500,
        retention: 31536000,
        maxLength: 3,
        changesMinDelta: 0.5,
        ignoreBelowNumber: -1,
        ignoreAboveNumber: 100,
    };
    await objects.setObjectAsync(`${instanceName}.testValueBlocked`, obj);

    await objects.setObjectAsync('system.adapter.test.0', {
        common: {},
        type: 'instance',
    });
    states.subscribeMessage('system.adapter.test.0');
}

function register(it, sendTo, adapterShortName, writeNulls, assumeExistingData, additionalActiveObjects) {
    const instanceName = `${adapterShortName}.0`;
    if (writeNulls) adapterShortName += '-writeNulls';
    if (assumeExistingData) adapterShortName += '-existing';

    it(`Test ${adapterShortName}: Setup test objects after start`, function (done) {
        this.timeout(5000);

        objects.setObject(
            `${instanceName}.testValue2`,
            {
                common: {
                    type: 'number',
                    role: 'state',
                },
                type: 'state',
            },
            function () {
                sendTo(
                    instanceName,
                    'enableHistory',
                    {
                        id: `${instanceName}.testValue2`,
                        options: {
                            changesOnly: true,
                            debounce: 0,
                            retention: 31536000,
                            maxLength: 0,
                            changesMinDelta: 0.5,
                            aliasId: `${instanceName}.testValue2-alias`,
                        },
                    },
                    function (result) {
                        assert.strictEqual(result.error, undefined);
                        assert.strictEqual(result.success, true);
                        // wait till adapter receives the new settings
                        setTimeout(function () {
                            done();
                        }, 2000);
                    },
                );
            },
        );
    });

    it(`Test ${adapterShortName}: Check Enabled Points after Enable`, function (done) {
        this.timeout(5000);

        sendTo(instanceName, 'getEnabledDPs', {}, function (result) {
            console.log(JSON.stringify(result));
            assert.strictEqual(Object.keys(result).length, 5 + additionalActiveObjects);
            assert.strictEqual(result[`${instanceName}.testValue`].enabled, true);
            done();
        });
    });
    it(`Test ${adapterShortName}: Write values into DB`, function (done) {
        this.timeout(25000);
        now = Date.now();

        states.setState(`${instanceName}.testValue`, { val: 1, ts: now + 1000 }, function (err) {
            if (err) {
                console.log(err);
            }
            setTimeout(function () {
                states.setState(`${instanceName}.testValue`, { val: 2, ts: now + 10000 }, function (err) {
                    if (err) {
                        console.log(err);
                    }
                    setTimeout(function () {
                        states.setState(`${instanceName}.testValue`, { val: 2, ts: now + 13000 }, function (err) {
                            if (err) {
                                console.log(err);
                            }
                            setTimeout(function () {
                                states.setState(
                                    `${instanceName}.testValue`,
                                    { val: 2, ts: now + 15000 },
                                    function (err) {
                                        if (err) {
                                            console.log(err);
                                        }
                                        setTimeout(function () {
                                            states.setState(
                                                `${instanceName}.testValue`,
                                                { val: 2.2, ts: now + 16000 },
                                                function (err) {
                                                    if (err) {
                                                        console.log(err);
                                                    }
                                                    setTimeout(function () {
                                                        states.setState(
                                                            `${instanceName}.testValue`,
                                                            { val: 2.5, ts: now + 17000 },
                                                            function (err) {
                                                                if (err) {
                                                                    console.log(err);
                                                                }
                                                                setTimeout(function () {
                                                                    states.setState(
                                                                        `${instanceName}.testValue`,
                                                                        { val: '+003.00', ts: now + 19000 },
                                                                        function (err) {
                                                                            if (err) {
                                                                                console.log(err);
                                                                            }
                                                                            setTimeout(function () {
                                                                                states.setState(
                                                                                    `${instanceName}.testValue2`,
                                                                                    { val: 1, ts: now + 12000 },
                                                                                    function (err) {
                                                                                        if (err) {
                                                                                            console.log(err);
                                                                                        }
                                                                                        setTimeout(function () {
                                                                                            states.setState(
                                                                                                `${instanceName}.testValue2`,
                                                                                                {
                                                                                                    val: 3,
                                                                                                    ts: now + 19000,
                                                                                                },
                                                                                                function (err) {
                                                                                                    if (err) {
                                                                                                        console.log(
                                                                                                            err,
                                                                                                        );
                                                                                                    }
                                                                                                    setTimeout(
                                                                                                        done,
                                                                                                        1000,
                                                                                                    );
                                                                                                },
                                                                                            );
                                                                                        }, 100);
                                                                                    },
                                                                                );
                                                                            }, 100);
                                                                        },
                                                                    );
                                                                }, 100);
                                                            },
                                                        );
                                                    }, 100);
                                                },
                                            );
                                        }, 100);
                                    },
                                );
                            }, 100);
                        });
                    }, 100);
                });
            }, 100);
        });
    });

    it(`Test ${adapterShortName}: Read values from DB using GetHistory`, function (done) {
        this.timeout(25000);

        sendTo(
            instanceName,
            'getHistory',
            {
                id: `${instanceName}.testValue`,
                options: {
                    start: now,
                    end: now + 30000,
                    count: 50,
                    aggregate: 'none',
                },
            },
            function (result) {
                console.log(JSON.stringify(result.result, null, 2));
                assert.ok(result.result.length >= 4, `${result.result.length} >= 4`);
                let found = 0;
                for (let i = 0; i < result.result.length; i++) {
                    if (result.result[i].val >= 1 && result.result[i].val <= 3) found++;
                }
                assert.strictEqual(found, 5); // additionally, null value by start of adapter.

                sendTo(
                    instanceName,
                    'getHistory',
                    {
                        id: `${instanceName}.testValue`,
                        options: {
                            start: now,
                            end: now + 30000,
                            count: 2,
                            aggregate: 'none',
                        },
                    },
                    function (result) {
                        console.log(JSON.stringify(result.result, null, 2));
                        assert.strictEqual(result.result.length, 2);
                        let found = 0;
                        for (let i = 0; i < result.result.length; i++) {
                            if (result.result[i].val >= 1 && result.result[i].val <= 3) found++;
                        }
                        assert.strictEqual(found, 2);
                        assert.strictEqual(result.result[0].id, undefined);

                        const latestTs = result.result[result.result.length - 1].ts;

                        sendTo(
                            instanceName,
                            'getHistory',
                            {
                                id: `${instanceName}.testValue`,
                                options: {
                                    start: now,
                                    end: now + 30000,
                                    count: 2,
                                    aggregate: 'none',
                                    addId: true,
                                    returnNewestEntries: true,
                                },
                            },
                            function (result) {
                                console.log(JSON.stringify(result.result, null, 2));
                                assert.strictEqual(result.result.length, 2);
                                let found = 0;
                                for (let i = 0; i < result.result.length; i++) {
                                    if (result.result[i].val >= 2.5 && result.result[i].val <= 3) found++;
                                }
                                assert.strictEqual(found, 2);
                                assert.strictEqual(result.result[0].ts >= latestTs, true);
                                assert.strictEqual(result.result[0].id, `${instanceName}.testValue`);
                                done();
                            },
                        );
                    },
                );
            },
        );
    });

    it(`Test ${adapterShortName}: Read average from DB using GetHistory`, function (done) {
        this.timeout(25000);

        sendTo(
            instanceName,
            'getHistory',
            {
                id: `${instanceName}.testValue`,
                options: {
                    start: now + 100,
                    end: now + 30001,
                    count: 2,
                    aggregate: 'average',
                    ignoreNull: true,
                    addId: true,
                },
            },
            function (result) {
                console.log(JSON.stringify(result.result, null, 2));
                if (instanceName !== 'influxdb.0') {
                    assert.strictEqual(result.result.length, 4);
                    assert.strictEqual(result.result[1].val, 1.5);
                    assert.strictEqual(result.result[2].val, 2.57);
                    assert.strictEqual(result.result[3].val, 2.57);
                } else {
                    assert.ok(
                        result.result.length >= 4 && result.result.length <= 5,
                        `${result.result.length} within 4..5`,
                    );
                    assert.ok(
                        result.result[1].val >= 1 && result.result[1].val <= 1.5,
                        `${result.result[1].val} within 1..1.5`,
                    );
                    assert.ok(
                        result.result[2].val >= 2 && result.result[2].val <= 3,
                        `${result.result[2].val} within 2..3`,
                    );
                    assert.ok(
                        result.result[3].val >= 2 && result.result[3].val <= 3,
                        `${result.result[3].val} within 2..3`,
                    );
                }
                assert.strictEqual(result.result[0].id, `${instanceName}.testValue`);
                done();
            },
        );
    });

    it(`Test ${adapterShortName}: Read minmax values from DB using GetHistory`, function (done) {
        this.timeout(10000);

        sendTo(
            instanceName,
            'getHistory',
            {
                id: `${instanceName}.testValue`,
                options: {
                    start: now - 30000,
                    end: now + 30000,
                    count: 4,
                    aggregate: 'minmax',
                    addId: true,
                },
            },
            result => {
                console.log(JSON.stringify(result.result, null, 2));
                assert.ok(result.result.length >= 4, `${result.result.length} >= 4`);
                assert.strictEqual(result.result[0].id, `${instanceName}.testValue`);
                done();
            },
        );
    });

    it(`Test ${adapterShortName}: Read values from DB using GetHistory for aliased testValue2`, function (done) {
        this.timeout(25000);

        sendTo(
            instanceName,
            'getHistory',
            {
                id: `${instanceName}.testValue2`,
                options: {
                    start: now,
                    end: now + 30000,
                    count: 50,
                    aggregate: 'none',
                },
            },
            function (result) {
                console.log(JSON.stringify(result.result, null, 2));
                assert.strictEqual(result.result.length, 2);

                sendTo(
                    instanceName,
                    'getHistory',
                    {
                        id: `${instanceName}.testValue2-alias`,
                        options: {
                            start: now,
                            end: now + 30000,
                            count: 50,
                            aggregate: 'none',
                        },
                    },
                    function (result2) {
                        console.log(JSON.stringify(result2.result, null, 2));
                        assert.strictEqual(result2.result.length, 2);
                        for (let i = 0; i < result2.result.length; i++) {
                            assert.strictEqual(result2.result[i].val, result.result[i].val);
                        }

                        done();
                    },
                );
            },
        );
    });

    function delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    async function logSampleData(stateId, waitMultiplier) {
        if (!waitMultiplier) waitMultiplier = 1;
        await states.setStateAsync(stateId, { val: 1 }); // expect logged
        await delay(600 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 2 }); // Expect not logged debounce
        await delay(20 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 2.1 }); // Expect not logged debounce
        await delay(20 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 1.5 }); // Expect not logged debounce
        await delay(20 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 2.3 }); // Expect not logged debounce
        await delay(20 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 2.5 }); // Expect not logged debounce
        await delay(600 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 2.9 }); // Expect logged skipped
        await delay(600 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 3.0 }); // Expect logged
        await delay(600 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 4 }); // Expect logged
        await delay(600 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 4.4 }); // expect logged skipped
        await delay(600 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 5 }); // expect logged
        await delay(20 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 5 }); // expect not logged debounce
        await delay(600 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 5 }); // expect logged skipped
        await delay(600 * waitMultiplier);
        await states.setStateAsync(stateId, { val: 6 }); // expect logged
        await delay(10100);
        for (let i = 1; i < 10; i++) {
            await states.setStateAsync(stateId, { val: 6 + i * 0.05 }); // expect logged skipped
            await delay(70 * waitMultiplier);
        }
        await states.setStateAsync(stateId, { val: 7 }); // expect logged
        await delay(5000);
        await states.setStateAsync(stateId, { val: -5 }); // expect not logged, too low
        await states.setStateAsync(stateId, { val: 101 }); // expect not logged, too high
        await delay(7000);
    }

    it(`Test ${adapterShortName}: Write debounced Raw values into DB`, async function () {
        this.timeout(45000);
        now = Date.now();

        try {
            await logSampleData(`${instanceName}.testValueDebounceRaw`);
        } catch (err) {
            console.log(err);
            assert.ok(!err);
        }

        return new Promise(resolve => {
            sendTo(
                instanceName,
                'getHistory',
                {
                    id: `${instanceName}.testValueDebounceRaw`,
                    options: {
                        start: now,
                        end: Date.now(),
                        count: 50,
                        aggregate: 'none',
                    },
                },
                function (result) {
                    console.log(JSON.stringify(result.result, null, 2));
                    assert.ok(result.result.length >= 9, `${result.result.length} >= 9`);
                    assert.strictEqual(result.result[0].val, 1);
                    assert.strictEqual(result.result[1].val, 2.5);
                    assert.strictEqual(result.result[2].val, 3.0);
                    assert.strictEqual(result.result[3].val, 4);
                    assert.strictEqual(result.result[4].val, 5);
                    assert.strictEqual(result.result[5].val, 6);
                    assert.strictEqual(result.result[6].val, 6);
                    assert.strictEqual(result.result[7].val, 7);
                    assert.strictEqual(result.result[8].val, 7);

                    setTimeout(resolve, 2000);
                },
            );
        });
    });

    it(`Test ${adapterShortName}: Write debounced values into DB`, async function () {
        this.timeout(45000);
        now = Date.now();

        try {
            await logSampleData(`${instanceName}.testValueDebounce`);
        } catch (err) {
            console.log(err);
            assert.ok(!err);
        }

        return new Promise(resolve => {
            sendTo(
                instanceName,
                'getHistory',
                {
                    id: `${instanceName}.testValueDebounce alias`,
                    options: {
                        start: now,
                        end: Date.now(),
                        count: 50,
                        aggregate: 'none',
                    },
                },
                function (result) {
                    console.log(JSON.stringify(result.result, null, 2));
                    assert.ok(result.result.length >= 12, `${result.result.length} >= 12`);

                    const expectedVals = [1, 2.5, 3, 4, 5, 5, 6, 7, 7];
                    let expectedId = 0;
                    for (let i = 0; i < result.result.length; i++) {
                        console.log(
                            `${i}: check ${result.result[i].val} vs ${expectedVals[expectedId]} (${expectedId})`,
                        );
                        assert.ok(
                            result.result[i].val <= expectedVals[expectedId],
                            `${result.result[i].val} <= expectedVals[expectedId]`,
                        );
                        if (result.result[i].val === expectedVals[expectedId] && expectedId < expectedVals.length - 1) {
                            expectedId++;
                        }
                    }
                    assert.strictEqual(expectedId, expectedVals.length - 1);

                    resolve();
                },
            );
        });
    });

    it(`Test ${adapterShortName}: Read percentile 50+95 values from DB using GetHistory`, function (done) {
        this.timeout(15000);

        sendTo(
            instanceName,
            'getHistory',
            {
                id: `${instanceName}.testValueDebounce alias`,
                options: {
                    start: now,
                    end: Date.now(),
                    count: 1,
                    aggregate: 'percentile',
                    percentile: 50,
                    removeBorderValues: true,
                    addId: true,
                },
            },
            result => {
                console.log(JSON.stringify(result.result, null, 2));
                if (instanceName !== 'influxdb.0') {
                    assert.strictEqual(result.result.length, 1);
                    assert.strictEqual(result.result[0].val, 5);
                    assert.strictEqual(result.result[0].id, `${instanceName}.testValueDebounce alias`);
                } else {
                    if (process.env.INFLUXDB2) {
                        assert.ok(
                            result.result.length >= 1 && result.result.length <= 3,
                            `${result.result.length} within 1..3`,
                        );
                        assert.ok(
                            (result.result[1] ? result.result[1].val : result.result[0].val) >= 5 &&
                                (result.result[1] ? result.result[1].val : result.result[0].val) <= 7,
                            `${result.result[1] ? result.result[1].val : result.result[0].val} within 5..7`,
                        );
                    } else {
                        assert.ok(
                            result.result.length >= 1 && result.result.length <= 2,
                            `${result.result.length} within 1..2`,
                        );
                        assert.ok(
                            (result.result[1] ? result.result[1].val : result.result[0].val) >= 5 &&
                                (result.result[1] ? result.result[1].val : result.result[0].val) <= 7,
                            `${result.result[1] ? result.result[1].val : result.result[0].val} within 5..7`,
                        );
                    }
                    assert.strictEqual(result.result[0].id, `${instanceName}.testValueDebounce alias`);
                }

                sendTo(
                    instanceName,
                    'getHistory',
                    {
                        id: `${instanceName}.testValueDebounce alias`,
                        options: {
                            start: now,
                            end: Date.now(),
                            count: 1,
                            aggregate: 'percentile',
                            percentile: 95,
                            removeBorderValues: true,
                            addId: true,
                        },
                    },
                    result => {
                        console.log(JSON.stringify(result.result, null, 2));
                        if (instanceName !== 'influxdb.0') {
                            assert.strictEqual(result.result.length, 1);
                            assert.strictEqual(result.result[0].val, 7);
                        } else {
                            assert.ok(
                                result.result.length >= 1 && result.result.length <= 3,
                                `${result.result.length} within 1..3`,
                            );
                            assert.strictEqual(result.result[result.result.length - 1].val, 7);
                            assert.strictEqual(result.result[0].id, `${instanceName}.testValueDebounce alias`);
                        }

                        assert.strictEqual(result.result[0].id, `${instanceName}.testValueDebounce alias`);
                        done();
                    },
                );
            },
        );
    });

    it(`Test ${adapterShortName}: Read integral from DB using GetHistory`, function (done) {
        this.timeout(25000);

        sendTo(
            instanceName,
            'getHistory',
            {
                id: `${instanceName}.testValueDebounce`,
                options: {
                    start: now,
                    end: Date.now(),
                    count: 5,
                    aggregate: 'integral',
                    integralUnit: 5,
                    removeBorderValues: true,
                    addId: true,
                },
            },
            function (result) {
                console.log(JSON.stringify(result.result, null, 2));
                if (instanceName !== 'influxdb.0') {
                    assert.strictEqual(result.result.length, 5);
                } else {
                    assert.ok(
                        result.result.length >= 3 && result.result.length <= 5,
                        `${result.result.length} within 3..5`,
                    );
                }
                assert.strictEqual(result.result[0].id, `${instanceName}.testValueDebounce alias`);
                done();
            },
        );
    });

    it(`Test ${adapterShortName}: Read linear integral from DB using GetHistory`, function (done) {
        this.timeout(25000);

        sendTo(
            instanceName,
            'getHistory',
            {
                id: `${instanceName}.testValueDebounce`,
                options: {
                    start: now,
                    end: Date.now(),
                    count: 5,
                    aggregate: 'integral',
                    integralUnit: 5,
                    integralInterpolation: 'linear',
                    removeBorderValues: true,
                    addId: true,
                },
            },
            function (result) {
                console.log(JSON.stringify(result.result, null, 2));
                if (instanceName !== 'influxdb.0') {
                    assert.strictEqual(result.result.length, 5);
                } else {
                    assert.ok(
                        result.result.length >= 3 && result.result.length <= 6,
                        `${result.result.length} within 3..6`,
                    );
                }
                assert.strictEqual(result.result[0].id, `${instanceName}.testValueDebounce alias`);
                done();
            },
        );
    });

    it(`Test ${adapterShortName}: Write with 1s block values into DB`, async function () {
        this.timeout(45000);
        now = Date.now();

        try {
            await logSampleData(`${instanceName}.testValueBlocked`, 1.5);
        } catch (err) {
            console.log(err);
            assert.ok(!err);
        }

        return new Promise(resolve => {
            sendTo(
                instanceName,
                'getHistory',
                {
                    id: `${instanceName}.testValueBlocked`,
                    options: {
                        start: now,
                        end: Date.now(),
                        count: 50,
                        aggregate: 'none',
                    },
                },
                function (result) {
                    console.log(JSON.stringify(result.result, null, 2));
                    assert.ok(result.result.length >= 9, `${result.result.length} >= 9`);
                    assert.strictEqual(result.result[0].val, 1);
                    assert.ok(result.result[1].val >= 2.3, `${result.result[1].val} >= 2.3`);
                    assert.strictEqual(result.result[2].val, 4);
                    assert.strictEqual(result.result[3].val, 5);
                    assert.strictEqual(result.result[4].val, 6);
                    assert.strictEqual(result.result[5].val, 6);
                    assert.strictEqual(result.result[6].val, 6.45);
                    assert.strictEqual(result.result[7].val, 7);
                    assert.strictEqual(result.result[8].val, 7);

                    resolve();
                },
            );
        });
    });

    it(`Test ${adapterShortName}: Relog the value dropped by blockTime, not the stale one`, async function () {
        this.timeout(60000);

        // Regression test for issue #273. A value arriving inside the blockTime window is not stored,
        // which is what blockTime promises. But it used to be forgotten entirely, so when the datapoint
        // stopped changing afterwards the relog timer kept repeating the value from before the window -
        // a PV inverter dropping from 500 W to 0 W was relogged as 500 W for hours.
        // testValueBlocked has blockTime 1500 and changesRelogInterval 10, so the second value below is
        // blocked and the relog roughly 10 s later has to write it instead of the first one. Timestamps
        // are explicit to keep the block window independent of the runner's wall clock.
        const blockNow = Date.now();
        const blockedId = `${instanceName}.testValueBlocked`;

        await states.setStateAsync(blockedId, { val: 50, ts: blockNow }); // expect logged
        await delay(300);
        await states.setStateAsync(blockedId, { val: 3, ts: blockNow + 200 }); // blocked by blockTime
        // deliberately no further value: this is what makes the stale value visible
        await delay(14000); // wait for the relog timer

        return new Promise(resolve => {
            sendTo(
                instanceName,
                'getHistory',
                {
                    id: blockedId,
                    options: {
                        start: blockNow,
                        end: Date.now(),
                        count: 50,
                        aggregate: 'none',
                    },
                },
                function (result) {
                    console.log(JSON.stringify(result.result, null, 2));
                    const values = result.result.map(entry => entry.val);
                    assert.ok(values.includes(50), `${JSON.stringify(values)} contains 50`);
                    assert.ok(
                        values.includes(3),
                        `${JSON.stringify(values)} contains 3 - relog repeated the stale value instead of the one blockTime dropped`,
                    );

                    resolve();
                },
            );
        });
    });

    it(`Test ${adapterShortName}: Tests with more sample data`, async function () {
        this.timeout(60000);
        const nowSampleI1 = Date.now() - 29 * 60 * 60 * 1000;
        const nowSampleI21 = Date.now() - 28 * 60 * 60 * 1000;
        const nowSampleI22 = Date.now() - 27 * 60 * 60 * 1000;
        const nowSampleI23 = Date.now() - 26 * 60 * 60 * 1000;
        const nowSampleI24 = Date.now() - 25 * 60 * 60 * 1000;

        return new Promise(resolve => {
            sendTo(
                instanceName,
                'storeState',
                {
                    id: `${instanceName}.testValue`,
                    state: [
                        { val: 2.064, ack: true, ts: nowSampleI1 }, //
                        { val: 2.116, ack: true, ts: nowSampleI1 + 6 * 60 * 1000 },
                        { val: 2.028, ack: true, ts: nowSampleI1 + 12 * 60 * 1000 },
                        { val: 2.126, ack: true, ts: nowSampleI1 + 18 * 60 * 1000 },
                        { val: 2.041, ack: true, ts: nowSampleI1 + 24 * 60 * 1000 },
                        { val: 2.051, ack: true, ts: nowSampleI1 + 30 * 60 * 1000 },

                        { val: -2, ack: true, ts: nowSampleI21 }, // 10s none = 50.0
                        { val: 10, ack: true, ts: nowSampleI21 + 10 * 1000 },
                        { val: 7, ack: true, ts: nowSampleI21 + 20 * 1000 },
                        { val: 17, ack: true, ts: nowSampleI21 + 30 * 1000 },
                        { val: 15, ack: true, ts: nowSampleI21 + 40 * 1000 },
                        { val: 4, ack: true, ts: nowSampleI21 + 50 * 1000 },

                        { val: 19, ack: true, ts: nowSampleI22 }, // 10s none = 43
                        { val: 4, ack: true, ts: nowSampleI22 + 10 * 1000 },
                        { val: -3, ack: true, ts: nowSampleI22 + 20 * 1000 },
                        { val: 19, ack: true, ts: nowSampleI22 + 30 * 1000 },
                        { val: 13, ack: true, ts: nowSampleI22 + 40 * 1000 },
                        { val: 1, ack: true, ts: nowSampleI22 + 50 * 1000 },

                        { val: -2, ack: true, ts: nowSampleI23 }, // 10s linear = 25
                        { val: 7, ack: true, ts: nowSampleI23 + 20 * 1000 },
                        { val: 4, ack: true, ts: nowSampleI23 + 50 * 1000 },

                        { val: 4, ack: true, ts: nowSampleI24 + 10 * 1000 }, // 10s linear = 32.5
                        { val: -3, ack: true, ts: nowSampleI24 + 20 * 1000 },
                        { val: 19, ack: true, ts: nowSampleI24 + 30 * 1000 },
                        { val: 1, ack: true, ts: nowSampleI24 + 50 * 1000 },
                    ],
                },
                function (result) {
                    assert.strictEqual(result.success, true);

                    setTimeout(() => {
                        sendTo(
                            instanceName,
                            'getHistory',
                            {
                                id: `${instanceName}.testValue`,
                                options: {
                                    start: nowSampleI1,
                                    end: nowSampleI1 + 30 * 60 * 1000,
                                    count: 1,
                                    aggregate: 'integral',
                                    removeBorderValues: true,
                                    integralUnit: 1,
                                    integralInterpolation: 'none',
                                },
                            },
                            function (result) {
                                console.log(`Sample I1-1: ${JSON.stringify(result.result, null, 2)}`);
                                if (instanceName !== 'influxdb.0') {
                                    assert.strictEqual(result.result.length, 1);
                                    if (assumeExistingData) {
                                        assert.ok(
                                            result.result[0].val >= 3700 && result.result[0].val <= 3755,
                                            `${result.result[0].val} within 3700..3755`,
                                        );
                                    } else {
                                        assert.ok(
                                            result.result[0].val >= 3700 && result.result[0].val <= 3800,
                                            `${result.result[0].val} within 3700..3800`,
                                        );
                                    }
                                } else {
                                    if (assumeExistingData) {
                                        assert.ok(
                                            result.result.length >= 2 && result.result.length <= 3,
                                            `${result.result.length} within 2..3`,
                                        );
                                        if (process.env.INFLUXDB2) {
                                            //assert.ok(((result.result[0].val + result.result[1].val)) >= 3780 && ((result.result[0].val + result.result[1].val)) <= 4000, `${((result.result[0].val + result.result[1].val))} within 3780..4000`);
                                        } else {
                                            //assert.ok(result.result[0].val >= 2980 && result.result[0].val <= 3000, `${result.result[0].val} within 2980..3000`);
                                        }
                                    } else {
                                        assert.strictEqual(result.result.length, 2);
                                        if (process.env.INFLUXDB2) {
                                            assert.ok(
                                                result.result[0].val + result.result[1].val >= 2980 &&
                                                    result.result[0].val + result.result[1].val <= 3000,
                                                `${result.result[0].val + result.result[1].val} within 2980..3000`,
                                            );
                                        } else {
                                            assert.strictEqual(
                                                parseFloat((result.result[0].val + result.result[1].val).toFixed(2)),
                                                3732.66,
                                            );
                                        }
                                    }
                                }
                                // Result Influxdb1 Doku = 3732.66

                                sendTo(
                                    instanceName,
                                    'getHistory',
                                    {
                                        id: `${instanceName}.testValue`,
                                        options: {
                                            start: nowSampleI1,
                                            end: nowSampleI1 + 30 * 60 * 1000,
                                            count: 1,
                                            aggregate: 'integral',
                                            removeBorderValues: true,
                                            integralUnit: 60,
                                            integralInterpolation: 'none',
                                        },
                                    },
                                    function (result) {
                                        console.log(`Sample I1-60: ${JSON.stringify(result.result, null, 2)}`);
                                        if (instanceName !== 'influxdb.0') {
                                            assert.strictEqual(result.result.length, 1);
                                            if (assumeExistingData) {
                                                assert.ok(
                                                    result.result[0].val < 62.25,
                                                    `${result.result[0].val} < 62.25`,
                                                );
                                            } else {
                                                assert.strictEqual(result.result[0].val, 62.25);
                                            }
                                        } else {
                                            if (assumeExistingData) {
                                                assert.strictEqual(result.result.length, 3);
                                                //assert.ok(result.result[1].val >= 40 && result.result[1].val <= 65, `${result.result[1].val} within 40..65`);
                                            } else {
                                                assert.strictEqual(result.result.length, 2);
                                                if (process.env.INFLUXDB2) {
                                                    assert.ok(
                                                        parseFloat(
                                                            (result.result[0].val + result.result[1].val).toFixed(2),
                                                        ) >= 49 &&
                                                            parseFloat(
                                                                (result.result[0].val + result.result[1].val).toFixed(
                                                                    2,
                                                                ),
                                                            ) <= 50,
                                                        `${parseFloat(
                                                            (result.result[0].val + result.result[1].val).toFixed(2),
                                                        )} within 49..50`,
                                                    );
                                                } else {
                                                    assert.strictEqual(
                                                        parseFloat(
                                                            (result.result[0].val + result.result[1].val).toFixed(2),
                                                        ),
                                                        62.21,
                                                    );
                                                }
                                            }
                                        }
                                        // Result Influxdb1 Doku = 62.211

                                        sendTo(
                                            instanceName,
                                            'getHistory',
                                            {
                                                id: `${instanceName}.testValue`,
                                                options: {
                                                    start: nowSampleI21,
                                                    end: nowSampleI21 + 60 * 1000,
                                                    count: 1,
                                                    aggregate: 'integral',
                                                    removeBorderValues: true,
                                                    integralUnit: 10,
                                                    integralInterpolation: 'none',
                                                },
                                            },
                                            function (result) {
                                                console.log(`Sample I21: ${JSON.stringify(result.result, null, 2)}`);
                                                if (instanceName !== 'influxdb.0') {
                                                    assert.strictEqual(result.result.length, 1);
                                                    assert.strictEqual(result.result[0].val, 51);
                                                } else {
                                                    assert.ok(
                                                        result.result.length >= 1 && result.result.length <= 2,
                                                        `${result.result.length} within 1..2`,
                                                    );
                                                    assert.ok(
                                                        result.result[0].val +
                                                            (result.result[1] ? result.result[1].val : 0) >=
                                                            30 &&
                                                            result.result[0].val +
                                                                (result.result[1] ? result.result[1].val : 0) <=
                                                                50,
                                                        `${
                                                            result.result[0].val +
                                                            (result.result[1] ? result.result[1].val : 0)
                                                        } within 30..50`,
                                                    );
                                                }
                                                // Result Influxdb21 Doku = 50.0

                                                sendTo(
                                                    instanceName,
                                                    'getHistory',
                                                    {
                                                        id: `${instanceName}.testValue`,
                                                        options: {
                                                            start: nowSampleI22,
                                                            end: nowSampleI22 + 60 * 1000,
                                                            count: 1,
                                                            aggregate: 'integral',
                                                            removeBorderValues: true,
                                                            integralUnit: 10,
                                                            integralInterpolation: 'none',
                                                        },
                                                    },
                                                    function (result) {
                                                        console.log(
                                                            `Sample I22: ${JSON.stringify(result.result, null, 2)}`,
                                                        );
                                                        if (instanceName !== 'influxdb.0') {
                                                            assert.strictEqual(result.result.length, 1);
                                                            assert.strictEqual(result.result[0].val, 53);
                                                        } else {
                                                            assert.ok(
                                                                result.result.length >= 1 && result.result.length <= 2,
                                                                `${result.result.length} within 1..2`,
                                                            );
                                                            assert.ok(
                                                                result.result[0].val +
                                                                    (result.result[1] ? result.result[1].val : 0) >=
                                                                    27 &&
                                                                    result.result[0].val +
                                                                        (result.result[1] ? result.result[1].val : 0) <=
                                                                        43,
                                                                `${
                                                                    result.result[0].val +
                                                                    (result.result[1] ? result.result[1].val : 0)
                                                                } within 27..43`,
                                                            );
                                                        }
                                                        // Result Influxdb22 Doku = 43

                                                        sendTo(
                                                            instanceName,
                                                            'getHistory',
                                                            {
                                                                id: `${instanceName}.testValue`,
                                                                options: {
                                                                    start: nowSampleI23,
                                                                    end: nowSampleI23 + 60 * 1000,
                                                                    count: 1,
                                                                    aggregate: 'integral',
                                                                    removeBorderValues: true,
                                                                    integralUnit: 10,
                                                                    integralInterpolation: 'linear',
                                                                },
                                                            },
                                                            function (result) {
                                                                console.log(
                                                                    `Sample I23: ${JSON.stringify(result.result, null, 2)}`,
                                                                );
                                                                if (instanceName !== 'influxdb.0') {
                                                                    assert.strictEqual(result.result.length, 1);
                                                                    assert.strictEqual(result.result[0].val, 25.5);
                                                                } else {
                                                                    assert.ok(
                                                                        result.result.length >= 1 &&
                                                                            result.result.length <= 2,
                                                                        `${result.result.length} within 1..2`,
                                                                    );
                                                                    if (process.env.INFLUXDB2) {
                                                                        //assert.strictEqual(result.result[0].val, 25.5);
                                                                    } else {
                                                                        assert.strictEqual(result.result[0].val, 34.5);
                                                                    }
                                                                }
                                                                // Result Influxdb23 Doku = 25.0

                                                                sendTo(
                                                                    instanceName,
                                                                    'getHistory',
                                                                    {
                                                                        id: `${instanceName}.testValue`,
                                                                        options: {
                                                                            start: nowSampleI24,
                                                                            end: nowSampleI24 + 60 * 1000,
                                                                            count: 1,
                                                                            aggregate: 'integral',
                                                                            removeBorderValues: true,
                                                                            integralUnit: 10,
                                                                            integralInterpolation: 'linear',
                                                                        },
                                                                    },
                                                                    function (result) {
                                                                        console.log(
                                                                            `Sample I24: ${JSON.stringify(result.result, null, 2)}`,
                                                                        );
                                                                        if (instanceName !== 'influxdb.0') {
                                                                            assert.strictEqual(result.result.length, 1);
                                                                            if (assumeExistingData) {
                                                                                assert.ok(
                                                                                    result.result[0].val >= 31 &&
                                                                                        result.result[0].val <= 32,
                                                                                    `${result.result[0].val} within 31..32`,
                                                                                );
                                                                            } else {
                                                                                assert.ok(
                                                                                    result.result[0].val >= 32 &&
                                                                                        result.result[0].val <= 33.5,
                                                                                    `${result.result[0].val} within 32..33.5`,
                                                                                );
                                                                            }
                                                                        } else {
                                                                            assert.ok(
                                                                                result.result.length >= 1 &&
                                                                                    result.result.length <= 2,
                                                                                `${result.result.length} within 1..2`,
                                                                            );
                                                                            if (process.env.INFLUXDB2) {
                                                                                //assert.strictEqual(result.result[0].val, 25.5);
                                                                            } else {
                                                                                if (assumeExistingData) {
                                                                                    assert.ok(
                                                                                        result.result[0].val >= 31 &&
                                                                                            result.result[0].val <= 34,
                                                                                        `${result.result[0].val} within 31..34`,
                                                                                    );
                                                                                } else {
                                                                                    assert.ok(
                                                                                        result.result[0].val >= 32 &&
                                                                                            result.result[0].val <=
                                                                                                33.5,
                                                                                        `${result.result[0].val} within 32..33.5`,
                                                                                    );
                                                                                }
                                                                            }
                                                                        }
                                                                        // Result Influxdb24 Doku = 32.5

                                                                        sendTo(
                                                                            instanceName,
                                                                            'getHistory',
                                                                            {
                                                                                id: `${instanceName}.testValue`,
                                                                                options: {
                                                                                    start: nowSampleI22,
                                                                                    end: nowSampleI22 + 60 * 1000,
                                                                                    count: 1,
                                                                                    aggregate: 'quantile',
                                                                                    quantile: 0.8,
                                                                                },
                                                                            },
                                                                            function (result) {
                                                                                console.log(
                                                                                    `Sample I22-Quantile: ${JSON.stringify(result.result, null, 2)}`,
                                                                                );
                                                                                if (instanceName !== 'influxdb.0') {
                                                                                    assert.strictEqual(
                                                                                        result.result.length,
                                                                                        3,
                                                                                    );
                                                                                    assert.strictEqual(
                                                                                        result.result[1].val,
                                                                                        19,
                                                                                    );
                                                                                } else {
                                                                                    assert.ok(
                                                                                        result.result.length >= 3 &&
                                                                                            result.result.length <= 4,
                                                                                        `${result.result.length} within 3..4`,
                                                                                    );
                                                                                    assert.ok(
                                                                                        result.result[1].val >= 4 &&
                                                                                            result.result[1].val <= 19,
                                                                                        `${result.result[1].val} within 4..19`,
                                                                                    );
                                                                                }

                                                                                resolve();
                                                                            },
                                                                        );
                                                                    },
                                                                );
                                                            },
                                                        );
                                                    },
                                                );
                                            },
                                        );
                                    },
                                );
                            },
                        );
                    }, 1000);
                },
            );
        });
    });

    it(`Test ${adapterShortName}: Read data two weeks around now GetHistory`, function (done) {
        this.timeout(25000);

        const start1week = Date.now() - 7 * 24 * 60 * 60 * 1000;

        sendTo(
            instanceName,
            'getHistory',
            {
                id: `${instanceName}.testValue`,
                options: {
                    start: start1week,
                    end: start1week + 7 * 24 * 60 * 60 * 1000,
                    step: 24 * 60 * 60 * 1000,
                    aggregate: 'integral',
                    integralUnit: 3600,
                    addId: true,
                },
            },
            function (result) {
                console.log(JSON.stringify(result.result, null, 2));
                assert.strictEqual(result.result.length, 4);
                assert.strictEqual(result.result[0].id, `${instanceName}.testValue`);
                done();
            },
        );
    });

    it(`Test ${adapterShortName}: Remove Alias-ID`, function (done) {
        this.timeout(5000);

        sendTo(
            instanceName,
            'enableHistory',
            {
                id: `${instanceName}.testValue2`,
                options: {
                    aliasId: '',
                },
            },
            function (result) {
                assert.strictEqual(result.error, undefined);
                assert.strictEqual(result.success, true);
                // wait till adapter receives the new settings
                setTimeout(function () {
                    done();
                }, 2000);
            },
        );
    });
    it(`Test ${adapterShortName}: Add Alias-ID again`, function (done) {
        this.timeout(5000);

        sendTo(
            instanceName,
            'enableHistory',
            {
                id: `${instanceName}.testValue2`,
                options: {
                    aliasId: 'this.is.a.test-value',
                },
            },
            function (result) {
                assert.strictEqual(result.error, undefined);
                assert.strictEqual(result.success, true);
                // wait till adapter receives the new settings
                setTimeout(function () {
                    done();
                }, 2000);
            },
        );
    });
    it(`Test ${adapterShortName}: Change Alias-ID`, function (done) {
        this.timeout(5000);

        sendTo(
            instanceName,
            'enableHistory',
            {
                id: `${instanceName}.testValue2`,
                options: {
                    aliasId: 'this.is.another.test-value',
                },
            },
            function (result) {
                assert.strictEqual(result.error, undefined);
                assert.strictEqual(result.success, true);
                // wait till adapter receives the new settings
                setTimeout(function () {
                    done();
                }, 2000);
            },
        );
    });

    it(`Test ${adapterShortName}: Disable Datapoint again`, function (done) {
        this.timeout(5000);

        sendTo(
            instanceName,
            'disableHistory',
            {
                id: `${instanceName}.testValue`,
            },
            function (result) {
                assert.strictEqual(result.error, undefined);
                assert.strictEqual(result.success, true);
                setTimeout(done, 2000);
            },
        );
    });
    it(`Test ${adapterShortName}: Check Enabled Points after Disable`, function (done) {
        this.timeout(5000);

        sendTo(instanceName, 'getEnabledDPs', {}, function (result) {
            console.log(JSON.stringify(result));
            assert.strictEqual(Object.keys(result).length, 4 + additionalActiveObjects);
            done();
        });
    });

    it(`Test ${adapterShortName}: Enable testValue Datapoint again`, function (done) {
        this.timeout(5000);

        sendTo(
            instanceName,
            'enableHistory',
            {
                id: `${instanceName}.testValue`,
            },
            function (result) {
                assert.strictEqual(result.error, undefined);
                assert.strictEqual(result.success, true);
                setTimeout(done, 2000);
            },
        );
    });

    it(`Test ${adapterShortName}: Check for written Null values`, function (done) {
        this.timeout(25000);

        sendTo(
            instanceName,
            'getHistory',
            {
                id: `${instanceName}.testValue`,
                options: {
                    start: preInitTime,
                    count: 500,
                    aggregate: 'none',
                },
            },
            function (result) {
                console.log(JSON.stringify(result.result, null, 2));
                assert.ok(result.result.length >= 5, `${result.result.length} >= 5`);
                let found = 0;
                for (let i = 0; i < result.result.length; i++) {
                    if (result.result[i].val === null) found++;
                }
                if (writeNulls) {
                    assert.strictEqual(found, 3);
                } else {
                    assert.strictEqual(found, 0);
                }

                done();
            },
        );
    });

    it(`Test ${adapterShortName}: Check for written Data in general`, function (done) {
        this.timeout(25000);

        sendTo(
            instanceName,
            'getHistory',
            {
                id: `${instanceName}.testValue`,
                options: {
                    count: 500,
                    aggregate: 'none',
                },
            },
            function (result) {
                console.log(JSON.stringify(result.result, null, 2));
                assert.ok(
                    result.result.length >= (writeNulls ? 3 : 0) + (assumeExistingData + 1) * 30,
                    `${result.result.length} >= (writeNulls ? 3 : 0) + (assumeExistingData + 1) * 30`,
                );

                done();
            },
        );
    });

    it(`Test ${adapterShortName}: Read minmax values from DB using GetHistory with 1mio slices`, function (done) {
        this.timeout(20000);

        sendTo(
            instanceName,
            'getHistory',
            {
                id: `${instanceName}.testValue`,
                options: {
                    start: Date.now() - 7 * 24 * 60 * 60 * 1000,
                    end: Date.now(),
                    count: 1000000,
                    limit: 1000000,
                    aggregate: 'minmax',
                    addId: true,
                },
            },
            result => {
                console.log(JSON.stringify(result.result, null, 2));
                assert.ok(result.result.length >= 4, `${result.result.length} >= 4`);
                assert.strictEqual(result.result[0].id, `${instanceName}.testValue`);
                done();
            },
        );
    });

    it(`Test ${adapterShortName}: storeState and getHistory for unknown Id`, function (done) {
        this.timeout(25000);

        const customNow = Date.now();
        sendTo(
            instanceName,
            'storeState',
            {
                id: `my.own.unknown.value-${customNow}`,
                state: [
                    { val: 1, ack: true, ts: customNow - 5000 },
                    { val: 2, ack: true, ts: customNow - 4000 },
                    { val: 3, ack: true, ts: customNow - 3000 },
                ],
            },
            function (result) {
                assert.strictEqual(result.success, true);
                assert.strictEqual(result.successCount, 3);

                setTimeout(() => {
                    sendTo(
                        instanceName,
                        'getHistory',
                        {
                            id: `my.own.unknown.value-${customNow}`,
                            options: {
                                start: customNow - 10000,
                                count: 500,
                                aggregate: 'none',
                            },
                        },
                        function (result) {
                            console.log(JSON.stringify(result.result, null, 2));
                            assert.strictEqual(result.result.length, 3);

                            done();
                        },
                    );
                }, 1000);
            },
        );
    });

    it(`Test ${adapterShortName}: storeState error for unknown Id with rules parameter`, function (done) {
        this.timeout(25000);

        const customNow2 = Date.now();
        sendTo(
            instanceName,
            'storeState',
            {
                id: `my.own.unknown.value-${customNow2}`,
                rules: true,
                state: [
                    { val: 1, ack: true, ts: customNow2 - 5000 },
                    { val: 2, ack: true, ts: customNow2 - 4000 },
                    '37',
                ],
            },
            function (result) {
                assert.ok(!result.success);
                assert.strictEqual(result.successCount, 0);
                assert.strictEqual(result.error, '3 errors happened while storing data');
                assert.strictEqual(Array.isArray(result.errors), true);
                assert.strictEqual(
                    result.errors[0].endsWith(
                        ` not enabled for my.own.unknown.value-${customNow2}, so can not apply the rules as requested`,
                    ),
                    true,
                );
                assert.strictEqual(result.errors[2], `State "37" for my.own.unknown.value-${customNow2} is not valid`);

                done();
            },
        );
    });

    // getCounter round trip on every SQL dialect (influxdb has no getCounter command, so the test is
    // not registered there). Uses a run-unique time window and explicit timestamps, so reruns against
    // an existing database (the MySQL "existing" suites) stay deterministic.
    if (instanceName !== 'influxdb.0') {
        it(`Test ${adapterShortName}: getCounter returns the summed progression incl. a counter reset`, function (done) {
            this.timeout(30000);
            const counterId = `${instanceName}.testCounter`;
            const base = Date.now();

            objects.setObject(
                counterId,
                { common: { type: 'number', role: 'state', custom: {} }, type: 'state' },
                function () {
                    sendTo(
                        instanceName,
                        'enableHistory',
                        {
                            id: counterId,
                            options: {
                                changesOnly: false,
                                debounce: 0,
                                retention: 31536000,
                                counter: true,
                                storageType: 'Number',
                            },
                        },
                        function (result) {
                            assert.strictEqual(result.error, undefined);
                            assert.strictEqual(result.success, true);
                            // wait for the adapter to pick up the settings, then write a counter
                            // sequence with a reset: 100 -> 200 -> 10 -> 110 (progression = 100 + 100)
                            setTimeout(function () {
                                states.setState(counterId, { val: 100, ts: base + 1000, ack: true }, function () {
                                    states.setState(counterId, { val: 200, ts: base + 2000, ack: true }, function () {
                                        states.setState(counterId, { val: 10, ts: base + 3000, ack: true }, function () {
                                            states.setState(
                                                counterId,
                                                { val: 110, ts: base + 4000, ack: true },
                                                function () {
                                                    setTimeout(function () {
                                                        sendTo(
                                                            instanceName,
                                                            'getCounter',
                                                            {
                                                                id: counterId,
                                                                options: { start: base, end: base + 5000 },
                                                            },
                                                            function (result) {
                                                                console.log(
                                                                    `getCounter result: ${JSON.stringify(result)}`,
                                                                );
                                                                assert.strictEqual(result.error, undefined);
                                                                assert.strictEqual(typeof result.result, 'number');
                                                                assert.strictEqual(result.result, 200);
                                                                done();
                                                            },
                                                        );
                                                    }, 3000);
                                                },
                                            );
                                        });
                                    });
                                });
                            }, 3000);
                        },
                    );
                },
            );
        });
    }
}

module.exports.register = register;
module.exports.preInit = preInit;
