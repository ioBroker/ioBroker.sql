/* jshint -W097 */
/* jshint strict: false */
/* jslint node: true */
/* jshint expr: true */
'use strict';

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
            custom: {}
        },
        type: 'state'
    };
    obj.common.custom[instanceName] = {
        enabled: true,
        changesOnly:  true,
        debounce:     0,
        retention:    31536000,
        maxLength:    3,
        changesMinDelta: 0.5
    };
    await objects.setObjectAsync(`${instanceName}.testValue`, obj);
    obj = {
        common: {
            type: 'number',
            role: 'state',
            custom: {}
        },
        type: 'state'
    };
    obj.common.custom[instanceName] = {
        enabled: true,
        changesOnly:  true,
        changesRelogInterval: 10,
        debounceTime:     500,
        retention:    31536000,
        maxLength:    3,
        changesMinDelta: 0.5,
        ignoreBelowNumber: -1,
        ignoreAboveNumber: 100,
        ignoreZero:   true,
        aliasId: `${instanceName}.testValueDebounce alias`
    };
    await objects.setObjectAsync(`${instanceName}.testValueDebounce`, obj);
    obj = {
        common: {
            type: 'number',
            role: 'state',
            custom: {}
        },
        type: 'state'
    };
    obj.common.custom[instanceName] = {
        enabled: true,
        changesOnly:  true,
        changesRelogInterval: 10,
        debounceTime:     500,
        retention:    31536000,
        maxLength:    0,
        changesMinDelta: 0.5,
        disableSkippedValueLogging: true,
        ignoreBelowZero: true,
        ignoreAboveNumber: 100,
        storageType: 'Number'
    };
    await objects.setObjectAsync(`${instanceName}.testValueDebounceRaw`, obj);
    obj = {
        common: {
            type: 'number',
            role: 'state',
            custom: {}
        },
        type: 'state'
    };
    obj.common.custom[instanceName] = {
        enabled: true,
        changesOnly:  true,
        changesRelogInterval: 10,
        debounceTime:     0,
        blockTime:        1500,
        retention:        31536000,
        maxLength:        3,
        changesMinDelta:  0.5,
        ignoreBelowNumber: -1,
        ignoreAboveNumber: 100
    };
    await objects.setObjectAsync(`${instanceName}.testValueBlocked`, obj);

    await objects.setObjectAsync('system.adapter.test.0', {
        common: {

        },
        type: 'instance'
    });
    states.subscribeMessage('system.adapter.test.0');
}

function register(it, expect, sendTo, adapterShortName, writeNulls, assumeExistingData, additionalActiveObjects) {
    const instanceName = `${adapterShortName}.0`;
    if (writeNulls) adapterShortName += '-writeNulls';
    if (assumeExistingData) adapterShortName += '-existing';

    it(`Test ${adapterShortName}: Setup test objects after start`, function(done) {
        this.timeout(3000);

        objects.setObject(`${instanceName}.testValue2`, {
                common: {
                    type: 'number',
                    role: 'state'
                },
                type: 'state'
            },
            function () {
                sendTo(instanceName, 'enableHistory', {
                    id: `${instanceName}.testValue2`,
                    options: {
                        changesOnly:  true,
                        debounce:     0,
                        retention:    31536000,
                        maxLength:    0,
                        changesMinDelta: 0.5,
                        aliasId: `${instanceName}.testValue2-alias`
                    }
                }, function (result) {
                    expect(result.error).to.be.undefined;
                    expect(result.success).to.be.true;
                    // wait till adapter receives the new settings
                    setTimeout(function () {
                        done();
                    }, 2000);
                });
            });
    });

    it(`Test ${adapterShortName}: Check Enabled Points after Enable`, function (done) {
        this.timeout(5000);

        sendTo(instanceName, 'getEnabledDPs', {}, function (result) {
            console.log(JSON.stringify(result));
            expect(Object.keys(result).length).to.be.equal(5 + additionalActiveObjects);
            expect(result[`${instanceName}.testValue`].enabled).to.be.true;
            done();
        });
    });
    it(`Test ${adapterShortName}: Write values into DB`, function (done) {
        this.timeout(25000);
        now = Date.now();

        states.setState(`${instanceName}.testValue`, {val: 1, ts: now + 1000}, function (err) {
            if (err) {
                console.log(err);
            }
            setTimeout(function () {
                states.setState(`${instanceName}.testValue`, {val: 2, ts: now + 10000}, function (err) {
                    if (err) {
                        console.log(err);
                    }
                    setTimeout(function () {
                        states.setState(`${instanceName}.testValue`, {val: 2, ts: now + 13000}, function (err) {
                            if (err) {
                                console.log(err);
                            }
                            setTimeout(function () {
                                states.setState(`${instanceName}.testValue`, {val: 2, ts: now + 15000}, function (err) {
                                    if (err) {
                                        console.log(err);
                                    }
                                    setTimeout(function () {
                                        states.setState(`${instanceName}.testValue`, {val: 2.2, ts: now + 16000}, function (err) {
                                            if (err) {
                                                console.log(err);
                                            }
                                            setTimeout(function () {
                                                states.setState(`${instanceName}.testValue`, {val: 2.5, ts: now + 17000}, function (err) {
                                                    if (err) {
                                                        console.log(err);
                                                    }
                                                    setTimeout(function () {
                                                        states.setState(`${instanceName}.testValue`, {val: '+003.00', ts: now + 19000}, function (err) {
                                                            if (err) {
                                                                console.log(err);
                                                            }
                                                            setTimeout(function () {
                                                                states.setState(`${instanceName}.testValue2`, {val: 1, ts: now + 12000}, function (err) {
                                                                    if (err) {
                                                                        console.log(err);
                                                                    }
                                                                    setTimeout(function () {
                                                                        states.setState(`${instanceName}.testValue2`, {val: 3, ts: now + 19000}, function (err) {
                                                                            if (err) {
                                                                                console.log(err);
                                                                            }
                                                                            setTimeout(done, 1000);
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
                    }, 100);
                });
            }, 100);
        });
    });

    it(`Test ${adapterShortName}: Read values from DB using GetHistory`, function (done) {
        this.timeout(25000);

        sendTo(instanceName, 'getHistory', {
            id: `${instanceName}.testValue`,
            options: {
                start:     now,
                end:       now + 30000,
                count:     50,
                aggregate: 'none'
            }
        }, function (result) {
            console.log(JSON.stringify(result.result, null, 2));
            expect(result.result.length).to.be.at.least(4);
            var found = 0;
            for (var i = 0; i < result.result.length; i++) {
                if (result.result[i].val >= 1 && result.result[i].val <= 3) found ++;
            }
            expect(found).to.be.equal(5); // additionally null value by start of adapter.

            sendTo(instanceName, 'getHistory', {
                id: `${instanceName}.testValue`,
                options: {
                    start:     now + 15000,
                    end:       now + 30000,
                    count:     2,
                    aggregate: 'none'
                }
            }, function (result) {
                console.log(JSON.stringify(result.result, null, 2));
                expect(result.result.length).to.be.equal(2);
                var found = 0;
                for (var i = 0; i < result.result.length; i++) {
                    if (result.result[i].val >= 1 && result.result[i].val <= 3) found ++;
                }
                expect(found).to.be.equal(2);
                expect(result.result[0].id).to.be.undefined;

                const latestTs = result.result[result.result.length - 1].ts;

                sendTo(instanceName, 'getHistory', {
                    id: `${instanceName}.testValue`,
                    options: {
                        start:     now + 15000,
                        end:       now + 30000,
                        count:     2,
                        aggregate: 'none',
                        addId: true,
                        returnNewestEntries: true
                    }
                }, function (result) {
                    console.log(JSON.stringify(result.result, null, 2));
                    expect(result.result.length).to.be.equal(2);
                    var found = 0;
                    for (var i = 0; i < result.result.length; i++) {
                        if (result.result[i].val >= 2.5 && result.result[i].val <= 3) found ++;
                    }
                    expect(found).to.be.equal(2);
                    expect(result.result[0].ts >= latestTs).to.be.true;
                    expect(result.result[0].id).to.be.equal(`${instanceName}.testValue`);
                    done();
                });
            });
        });
    });

    it(`Test ${adapterShortName}: Read average from DB using GetHistory`, function (done) {
        this.timeout(25000);

        sendTo(instanceName, 'getHistory', {
            id: `${instanceName}.testValue`,
            options: {
                start:     now + 100,
                end:       now + 30001,
                count:     2,
                aggregate: 'average',
                ignoreNull: true,
                addId: true
            }
        }, function (result) {
            console.log(JSON.stringify(result.result, null, 2));
            expect(result.result.length).to.be.equal(4);
            expect(result.result[1].val).to.be.equal(1.5);
            expect(result.result[2].val).to.be.equal(2.57);
            expect(result.result[3].val).to.be.equal(2.57);
            expect(result.result[0].id).to.be.equal(`${instanceName}.testValue`);
            done();
        });
    });

    it(`Test ${adapterShortName}: Read minmax values from DB using GetHistory`, function (done) {
        this.timeout(10000);

        sendTo(instanceName, 'getHistory', {
            id: `${instanceName}.testValue`,
            options: {
                start:     now - 30000,
                end:       now + 30000,
                count:     4,
                aggregate: 'minmax',
                addId: true
            }
        }, result => {
            console.log(JSON.stringify(result.result, null, 2));
            expect(result.result.length).to.be.at.least(4);
            expect(result.result[0].id).to.be.equal(`${instanceName}.testValue`);
            done();
        });
    });

    it(`Test ${adapterShortName}: Read values from DB using GetHistory for aliased testValue2`, function (done) {
        this.timeout(25000);

        sendTo(instanceName, 'getHistory', {
            id: `${instanceName}.testValue2`,
            options: {
                start:     now,
                end:       now + 30000,
                count:     50,
                aggregate: 'none'
            }
        }, function (result) {
            console.log(JSON.stringify(result.result, null, 2));
            expect(result.result.length).to.be.equal(2);

            sendTo(instanceName, 'getHistory', {
                id: `${instanceName}.testValue2-alias`,
                options: {
                    start:     now,
                    end:       now + 30000,
                    count:     50,
                    aggregate: 'none'
                }
            }, function (result2) {
                console.log(JSON.stringify(result2.result, null, 2));
                expect(result2.result.length).to.be.equal(2);
                for (var i = 0; i < result2.result.length; i++) {
                    expect(result2.result[i].val).to.be.equal(result.result[i].val);
                }

                done();
            });
        });
    });

    function delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    async function logSampleData(stateId, waitMultiplier) {
        if (!waitMultiplier) waitMultiplier = 1;
        await states.setStateAsync(stateId, {val: 1}); // expect logged
        await delay(600 * waitMultiplier);
        await states.setStateAsync(stateId, {val: 2}); // Expect not logged debounce
        await delay(20 * waitMultiplier);
        await states.setStateAsync(stateId, {val: 2.1}); // Expect not logged debounce
        await delay(20 * waitMultiplier);
        await states.setStateAsync(stateId, {val: 1.5}); // Expect not logged debounce
        await delay(20 * waitMultiplier);
        await states.setStateAsync(stateId, {val: 2.3}); // Expect not logged debounce
        await delay(20 * waitMultiplier);
        await states.setStateAsync(stateId, {val: 2.5}); // Expect not logged debounce
        await delay(600 * waitMultiplier);
        await states.setStateAsync(stateId, {val: 2.9}); // Expect logged skipped
        await delay(600 * waitMultiplier);
        await states.setStateAsync(stateId, {val: 3.0}); // Expect logged
        await delay(600 * waitMultiplier);
        await states.setStateAsync(stateId, {val: 4}); // Expect logged
        await delay(600 * waitMultiplier);
        await states.setStateAsync(stateId, {val: 4.4}); // expect logged skipped
        await delay(600 * waitMultiplier);
        await states.setStateAsync(stateId, {val: 5});  // expect logged
        await delay(20 * waitMultiplier);
        await states.setStateAsync(stateId, {val: 5});  // expect not logged debounce
        await delay(600 * waitMultiplier);
        await states.setStateAsync(stateId, {val: 5});  // expect logged skipped
        await delay(600 * waitMultiplier);
        await states.setStateAsync(stateId, {val: 6});  // expect logged
        await delay(10100);
        for (let i = 1; i < 10; i++) {
            await states.setStateAsync(stateId, {val: 6 + i * 0.05});  // expect logged skipped
            await delay(70 * waitMultiplier);
        }
        await states.setStateAsync(stateId, {val: 7});  // expect logged
        await delay(5000);
        await states.setStateAsync(stateId, {val: -5});  // expect not logged, too low
        await states.setStateAsync(stateId, {val: 101}); // expect not logged, too high
        await delay(7000);
    }

    it(`Test ${adapterShortName}: Write debounced Raw values into DB`, async function () {
        this.timeout(45000);
        now = Date.now();

        try {
            await logSampleData(`${instanceName}.testValueDebounceRaw`);
        } catch (err) {
            console.log(err);
            expect(err).to.be.not.ok;
        }

        return new Promise(resolve => {
            sendTo(instanceName, 'getHistory', {
                id: `${instanceName}.testValueDebounceRaw`,
                options: {
                    start:     now,
                    end:       Date.now(),
                    count:     50,
                    aggregate: 'none'
                }
            }, function (result) {
                console.log(JSON.stringify(result.result, null, 2));
                expect(result.result.length).to.be.at.least(9);
                expect(result.result[0].val).to.be.equal(1);
                expect(result.result[1].val).to.be.equal(2.5);
                expect(result.result[2].val).to.be.equal(3.0);
                expect(result.result[3].val).to.be.equal(4);
                expect(result.result[4].val).to.be.equal(5);
                expect(result.result[5].val).to.be.equal(6);
                expect(result.result[6].val).to.be.equal(6);
                expect(result.result[7].val).to.be.equal(7);
                expect(result.result[8].val).to.be.equal(7);

                setTimeout(resolve, 2000);
            });
        });
    });

    it(`Test ${adapterShortName}: Write debounced values into DB`, async function () {
        this.timeout(45000);
        now = Date.now();

        try {
            await logSampleData(`${instanceName}.testValueDebounce`);
        } catch (err) {
            console.log(err);
            expect(err).to.be.not.ok;
        }

        return new Promise(resolve => {

            sendTo(instanceName, 'getHistory', {
                id: `${instanceName}.testValueDebounce alias`,
                options: {
                    start:     now,
                    end:       Date.now(),
                    count:     50,
                    aggregate: 'none'
                }
            }, function (result) {
                console.log(JSON.stringify(result.result, null, 2));
                expect(result.result.length).to.be.at.least(13);

                const expectedVals = [1, 2.5, 3, 4, 5, 5, 6, 7, 7, 7];
                let expectedId = 0;
                for (let i = 0; i < result.result.length; i++) {
                    console.log(`${i}: check ${result.result[i].val} vs ${expectedVals[expectedId]} (${expectedId})`);
                    expect(result.result[i].val).to.be.lessThanOrEqual(expectedVals[expectedId]);
                    if (result.result[i].val === expectedVals[expectedId]) {
                        expectedId++;
                    }
                }
                expect(expectedId).to.be.equal(expectedVals.length - 1);

                resolve();
            });
        });
    });

    it(`Test ${adapterShortName}: Read percentile 50+95 values from DB using GetHistory`, function (done) {
        this.timeout(10000);

        sendTo(instanceName, 'getHistory', {
            id: `${instanceName}.testValueDebounce alias`,
            options: {
                start:     now,
                end:       Date.now(),
                count:     1,
                aggregate: 'percentile',
                percentile: 50,
                removeBorderValues: true,
                addId: true
            }
        }, result => {
            console.log(JSON.stringify(result.result, null, 2));
            expect(result.result.length).to.be.equal(1);
            expect(result.result[0].id).to.be.equal(`${instanceName}.testValueDebounce alias`);
            expect(result.result[0].val).to.be.equal(5);

            sendTo(instanceName, 'getHistory', {
                id: `${instanceName}.testValueDebounce alias`,
                options: {
                    start:     now,
                    end:       Date.now(),
                    count:     1,
                    aggregate: 'percentile',
                    percentile: 95,
                    removeBorderValues: true,
                    addId: true
                }
            }, result => {
                console.log(JSON.stringify(result.result, null, 2));
                expect(result.result.length).to.be.equal(1);
                expect(result.result[0].id).to.be.equal(`${instanceName}.testValueDebounce alias`);
                expect(result.result[0].val).to.be.equal(7);
                done();
            });
        });
    });

    it(`Test ${adapterShortName}: Read integral from DB using GetHistory`, function (done) {
        this.timeout(25000);

        sendTo(instanceName, 'getHistory', {
            id: `${instanceName}.testValueDebounce`,
            options: {
                start:     now,
                end:       Date.now(),
                count:     5,
                aggregate: 'integral',
                integralUnit: 5,
                removeBorderValues: true,
                addId: true
            }
        }, function (result) {
            console.log(JSON.stringify(result.result, null, 2));
            expect(result.result.length).to.be.equal(5);
            expect(result.result[0].id).to.be.equal(`${instanceName}.testValueDebounce alias`);
            done();
        });
    });

    it(`Test ${adapterShortName}: Read linear integral from DB using GetHistory`, function (done) {
        this.timeout(25000);

        sendTo(instanceName, 'getHistory', {
            id: `${instanceName}.testValueDebounce`,
            options: {
                start:     now,
                end:       Date.now(),
                count:     5,
                aggregate: 'integral',
                integralUnit: 5,
                integralInterpolation: 'linear',
                removeBorderValues: true,
                addId: true
            }
        }, function (result) {
            console.log(JSON.stringify(result.result, null, 2));
            expect(result.result.length).to.be.equal(5);
            expect(result.result[0].id).to.be.equal(`${instanceName}.testValueDebounce alias`);
            done();
        });
    });

    it(`Test ${adapterShortName}: Write with 1s block values into DB`, async function () {
        this.timeout(45000);
        now = Date.now();

        try {
            await logSampleData(`${instanceName}.testValueBlocked`, 1.5);
        } catch (err) {
            console.log(err);
            expect(err).to.be.not.ok;
        }

        return new Promise(resolve => {

            sendTo(instanceName, 'getHistory', {
                id: `${instanceName}.testValueBlocked`,
                options: {
                    start:     now,
                    end:       Date.now(),
                    count:     50,
                    aggregate: 'none'
                }
            }, function (result) {
                console.log(JSON.stringify(result.result, null, 2));
                expect(result.result.length).to.be.at.least(9);
                expect(result.result[0].val).to.be.equal(1);
                expect(result.result[1].val).to.be.at.least(2.3);
                expect(result.result[2].val).to.be.equal(4);
                expect(result.result[3].val).to.be.equal(5);
                expect(result.result[4].val).to.be.equal(6);
                expect(result.result[5].val).to.be.equal(6);
                expect(result.result[6].val).to.be.equal(6.45);
                expect(result.result[7].val).to.be.equal(7);
                expect(result.result[8].val).to.be.equal(7);

                resolve();
            });
        });
    });

    it(`Test ${adapterShortName}: Tests with more sample data`, async function () {
        this.timeout(45000);
        const nowSampleI1 = Date.now() - 29 * 60 * 60 * 1000;
        const nowSampleI21 = Date.now() - 28 * 60 * 60 * 1000;
        const nowSampleI22 = Date.now() - 27 * 60 * 60 * 1000;
        const nowSampleI23 = Date.now() - 26 * 60 * 60 * 1000;
        const nowSampleI24 = Date.now() - 25 * 60 * 60 * 1000;

        return new Promise(resolve => {

            sendTo(instanceName, 'storeState', {
                id: `${instanceName}.testValue`,
                state: [
                    {val: 2.064, ack: true, ts: nowSampleI1}, //
                    {val: 2.116, ack: true, ts: nowSampleI1 + 6 * 60 * 1000},
                    {val: 2.028, ack: true, ts: nowSampleI1 + 12 * 60 * 1000},
                    {val: 2.126, ack: true, ts: nowSampleI1 + 18 * 60 * 1000},
                    {val: 2.041, ack: true, ts: nowSampleI1 + 24 * 60 * 1000},
                    {val: 2.051, ack: true, ts: nowSampleI1 + 30 * 60 * 1000},

                    {val: -2, ack: true, ts: nowSampleI21}, // 10s none = 50.0
                    {val: 10, ack: true, ts: nowSampleI21 + 10 * 1000},
                    {val: 7, ack: true, ts: nowSampleI21 + 20 * 1000},
                    {val: 17, ack: true, ts: nowSampleI21 + 30 * 1000},
                    {val: 15, ack: true, ts: nowSampleI21 + 40 * 1000},
                    {val: 4, ack: true, ts: nowSampleI21 + 50 * 1000},

                    {val: 19, ack: true, ts: nowSampleI22}, // 10s none = 43
                    {val: 4, ack: true, ts: nowSampleI22 + 10 * 1000},
                    {val: -3, ack: true, ts: nowSampleI22 + 20 * 1000},
                    {val: 19, ack: true, ts: nowSampleI22 + 30 * 1000},
                    {val: 13, ack: true, ts: nowSampleI22 + 40 * 1000},
                    {val: 1, ack: true, ts: nowSampleI22 + 50 * 1000},

                    {val: -2, ack: true, ts: nowSampleI23}, // 10s linear = 25
                    {val: 7, ack: true, ts: nowSampleI23 + 20 * 1000},
                    {val: 4, ack: true, ts: nowSampleI23 + 50 * 1000},

                    {val: 4, ack: true, ts: nowSampleI24 + 10 * 1000}, // 10s linear = 32.5
                    {val: -3, ack: true, ts: nowSampleI24 + 20 * 1000},
                    {val: 19, ack: true, ts: nowSampleI24 + 30 * 1000},
                    {val: 1, ack: true, ts: nowSampleI24 + 50 * 1000},
                ]
            }, function (result) {
                expect(result.success).to.be.true;

                sendTo(instanceName, 'getHistory', {
                    id: `${instanceName}.testValue`,
                    options: {
                        start:     nowSampleI1,
                        end:       nowSampleI1 + 30 * 60 * 1000,
                        count:     1,
                        aggregate: 'integral',
                        removeBorderValues: true,
                        integralUnit: 1,
                        integralInterpolation: 'none'
                    }
                }, function (result) {
                    console.log(`Sample I1-1: ${JSON.stringify(result.result, null, 2)}`);
                    expect(result.result.length).to.be.equal(1);
                    if (assumeExistingData) {
                        expect(result.result[0].val).to.be.within(3700, 3755);
                    } else {
                        expect(result.result[0].val).to.be.within(3700, 3800);
                    }
                    // Result Influxdb1 Doku = 3732.66

                    sendTo(instanceName, 'getHistory', {
                        id: `${instanceName}.testValue`,
                        options: {
                            start:     nowSampleI1,
                            end:       nowSampleI1 + 30 * 60 * 1000,
                            count:     1,
                            aggregate: 'integral',
                            removeBorderValues: true,
                            integralUnit: 60,
                            integralInterpolation: 'none'
                        }
                    }, function (result) {
                        console.log(`Sample I1-60: ${JSON.stringify(result.result, null, 2)}`);
                        expect(result.result.length).to.be.equal(1);
                        if (assumeExistingData) {
                            expect(result.result[0].val).to.be.lessThan(62.25);
                        } else {
                            expect(result.result[0].val).to.be.equal(62.25);
                        }
                        // Result Influxdb1 Doku = 62.211

                        sendTo(instanceName, 'getHistory', {
                            id: `${instanceName}.testValue`,
                            options: {
                                start:     nowSampleI21,
                                end:       nowSampleI21 + 60 * 1000,
                                count:     1,
                                aggregate: 'integral',
                                removeBorderValues: true,
                                integralUnit: 10,
                                integralInterpolation: 'none'
                            }
                        }, function (result) {
                            console.log(`Sample I21: ${JSON.stringify(result.result, null, 2)}`);
                            expect(result.result.length).to.be.equal(1);
                            expect(result.result[0].val).to.be.equal(51);
                            // Result Influxdb21 Doku = 50.0

                            sendTo(instanceName, 'getHistory', {
                                id: `${instanceName}.testValue`,
                                options: {
                                    start:     nowSampleI22,
                                    end:       nowSampleI22 + 60 * 1000,
                                    count:     1,
                                    aggregate: 'integral',
                                    removeBorderValues: true,
                                    integralUnit: 10,
                                    integralInterpolation: 'none'
                                }
                            }, function (result) {
                                console.log(`Sample I22: ${JSON.stringify(result.result, null, 2)}`);
                                expect(result.result.length).to.be.equal(1);
                                expect(result.result[0].val).to.be.equal(53);
                                // Result Influxdb22 Doku = 43

                                sendTo(instanceName, 'getHistory', {
                                    id: `${instanceName}.testValue`,
                                    options: {
                                        start:     nowSampleI23,
                                        end:       nowSampleI23 + 60 * 1000,
                                        count:     1,
                                        aggregate: 'integral',
                                        removeBorderValues: true,
                                        integralUnit: 10,
                                        integralInterpolation: 'linear'
                                    }
                                }, function (result) {
                                    console.log(`Sample I23: ${JSON.stringify(result.result, null, 2)}`);
                                    expect(result.result.length).to.be.equal(1);
                                    expect(result.result[0].val).to.be.equal(25.5);
                                    // Result Influxdb23 Doku = 25.0

                                    sendTo(instanceName, 'getHistory', {
                                        id: `${instanceName}.testValue`,
                                        options: {
                                            start:     nowSampleI24,
                                            end:       nowSampleI24 + 60 * 1000,
                                            count:     1,
                                            aggregate: 'integral',
                                            removeBorderValues: true,
                                            integralUnit: 10,
                                            integralInterpolation: 'linear'
                                        }
                                    }, function (result) {
                                        console.log(`Sample I24: ${JSON.stringify(result.result, null, 2)}`);
                                        expect(result.result.length).to.be.equal(1);
                                        if (assumeExistingData) {
                                            expect(result.result[0].val).to.be.within(31, 32);
                                        } else {
                                            expect(result.result[0].val).to.be.within(32, 33.5);
                                        }
                                        // Result Influxdb24 Doku = 32.5

                                        sendTo(instanceName, 'getHistory', {
                                            id: `${instanceName}.testValue`,
                                            options: {
                                                start:     nowSampleI22,
                                                end:       nowSampleI22 + 60 * 1000,
                                                count:     1,
                                                aggregate: 'quantile',
                                                quantile: 0.8
                                            }
                                        }, function (result) {
                                            console.log(`Sample I22-Quantile: ${JSON.stringify(result.result, null, 2)}`);
                                            expect(result.result.length).to.be.equal(3);
                                            expect(result.result[1].val).to.be.equal(19);

                                            resolve();
                                        });
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });
    });

    it(`Test ${adapterShortName}: Read data two weeks around now GetHistory`, function (done) {
        this.timeout(25000);

        const start1week = Date.now() - 7 * 24 * 60 * 60 * 1000;

        sendTo(instanceName, 'getHistory', {
            id: `${instanceName}.testValue`,
            options: {
                start:     start1week,
                end:       start1week + 7 * 24 * 60 * 60 * 1000,
                step:      24 * 60 * 60 * 1000,
                aggregate: 'integral',
                integralUnit: 3600,
                addId: true
            }
        }, function (result) {
            console.log(JSON.stringify(result.result, null, 2));
            expect(result.result.length).to.be.equal(4);
            expect(result.result[0].id).to.be.equal(`${instanceName}.testValue`);
            done();
        });
    });

    it(`Test ${adapterShortName}: Remove Alias-ID`, function (done) {
        this.timeout(5000);

        sendTo(instanceName, 'enableHistory', {
            id: `${instanceName}.testValue2`,
            options: {
                aliasId: ''
            }
        }, function (result) {
            expect(result.error).to.be.undefined;
            expect(result.success).to.be.true;
            // wait till adapter receives the new settings
            setTimeout(function () {
                done();
            }, 2000);
        });
    });
    it(`Test ${adapterShortName}: Add Alias-ID again`, function (done) {
        this.timeout(5000);

        sendTo(instanceName, 'enableHistory', {
            id: `${instanceName}.testValue2`,
            options: {
                aliasId: 'this.is.a.test-value'
            }
        }, function (result) {
            expect(result.error).to.be.undefined;
            expect(result.success).to.be.true;
            // wait till adapter receives the new settings
            setTimeout(function () {
                done();
            }, 2000);
        });
    });
    it(`Test ${adapterShortName}: Change Alias-ID`, function (done) {
        this.timeout(5000);

        sendTo(instanceName, 'enableHistory', {
            id: `${instanceName}.testValue2`,
            options: {
                aliasId: 'this.is.another.test-value'
            }
        }, function (result) {
            expect(result.error).to.be.undefined;
            expect(result.success).to.be.true;
            // wait till adapter receives the new settings
            setTimeout(function () {
                done();
            }, 2000);
        });
    });

    it(`Test ${adapterShortName}: Disable Datapoint again`, function (done) {
        this.timeout(5000);

        sendTo(instanceName, 'disableHistory', {
            id: `${instanceName}.testValue`
        }, function (result) {
            expect(result.error).to.be.undefined;
            expect(result.success).to.be.true;
            setTimeout(done, 2000);
        });
    });
    it(`Test ${adapterShortName}: Check Enabled Points after Disable`, function (done) {
        this.timeout(5000);

        sendTo(instanceName, 'getEnabledDPs', {}, function (result) {
            console.log(JSON.stringify(result));
            expect(Object.keys(result).length).to.be.equal(4 + additionalActiveObjects);
            done();
        });
    });

    it(`Test ${adapterShortName}: Check for written Null values`, function (done) {
        this.timeout(25000);

        sendTo(instanceName, 'getHistory', {
            id: `${instanceName}.testValue`,
            options: {
                start:     preInitTime - 5000,
                count:     500,
                aggregate: 'none'
            }
        }, function (result) {
            console.log(JSON.stringify(result.result, null, 2));
            expect(result.result.length).to.be.at.least(5);
            var found = 0;
            for (var i = 0; i < result.result.length; i++) {
                if (result.result[i].val === null) found++;
            }
            if (writeNulls) {
                expect(found).to.be.equal(2);
            } else {
                expect(found).to.be.equal(0);
            }

            done();
        });
    });

    it(`Test ${adapterShortName}: Check for written Data in general`, function (done) {
        this.timeout(25000);

        sendTo(instanceName, 'getHistory', {
            id: `${instanceName}.testValue`,
            options: {
                count:     500,
                aggregate: 'none'
            }
        }, function (result) {
            console.log(JSON.stringify(result.result, null, 2));
            expect(result.result.length).to.be.at.least((writeNulls? 3 : 0) + ((assumeExistingData + 1) * 30));

            done();
        });
    });
}

module.exports.register = register;
module.exports.preInit = preInit;
