/* jshint -W097 */
/* jshint strict: false */
/* jslint node: true */
'use strict';
// THIS file should be identical with sql and history adapter's one

function initAggregate(options) {
    //step; // 1 Step is 1 second
    if (options.step === null) {
        options.step = (options.end - options.start) / options.count;
    }

    // Limit 2000
    if ((options.end - options.start) / options.step > options.limit) {
        options.step = (options.end - options.start) / options.limit;
    }

    options.maxIndex       = Math.ceil(((options.end - options.start) / options.step) - 1);
    options.result         = [];
    options.averageCount   = [];
    options.aggregate      = options.aggregate || 'minmax';
    options.overallLength  = 0;

    // pre-fill the result with timestamps (add one before start and one after end)
    for (let i = 0; i <= options.maxIndex + 2; i++){
        options.result[i] = {
            val:    {ts: null, val: null},
            max:    {ts: null, val: null},
            min:    {ts: null, val: null},
            start:  {ts: null, val: null},
            end:    {ts: null, val: null}
        };

        if (options.aggregate === 'average') {
            options.averageCount[i] = 0;
        }
    }
    return options;
}

function aggregation(options, data) {
    let index;
    let preIndex;
    for (let i = 0; i < data.length; i++) {
        if (!data[i]) {
            continue;
        }
        if (typeof data[i].ts !== 'number') {
            data[i].ts = parseInt(data[i].ts, 10);
        }

        preIndex = Math.floor((data[i].ts - options.start) / options.step);

        // store all border values
        if (preIndex < 0) {
            index = 0;
        } else if (preIndex > options.maxIndex) {
            index = options.maxIndex + 2;
        } else {
            index = preIndex + 1;
        }
        options.overallLength++;

        if (!options.result[index]) {
            console.error('Cannot find index ' +  index);
            continue;
        }

        if (options.aggregate === 'max') {
            if (!options.result[index].val.ts) {
                options.result[index].val.ts = Math.round(options.start + (((index - 1) + 0.5) * options.step));
            }
            if (options.result[index].val.val === null || options.result[index].val.val < data[i].val) {
                options.result[index].val.val = data[i].val;
            }
        } else if (options.aggregate === 'min') {
            if (!options.result[index].val.ts) {
                options.result[index].val.ts = Math.round(options.start + (((index - 1) + 0.5) * options.step));
            }
            if (options.result[index].val.val === null || options.result[index].val.val > data[i].val) {
                options.result[index].val.val = data[i].val;
            }
        } else if (options.aggregate === 'average') {
            if (!options.result[index].val.ts) {
                options.result[index].val.ts = Math.round(options.start + (((index - 1) + 0.5) * options.step));
            }
            options.result[index].val.val += parseFloat(data[i].val);
            options.averageCount[index]++;
        } else if (options.aggregate === 'total') {
            if (!options.result[index].val.ts) {
                options.result[index].val.ts = Math.round(options.start + (((index - 1) + 0.5) * options.step));
            }
            options.result[index].val.val += parseFloat(data[i].val);
        } else if (options.aggregate === 'minmax') {
            if (options.result[index].min.ts === null) {
                options.result[index].min.ts    = data[i].ts;
                options.result[index].min.val   = data[i].val;

                options.result[index].max.ts    = data[i].ts;
                options.result[index].max.val   = data[i].val;

                options.result[index].start.ts  = data[i].ts;
                options.result[index].start.val = data[i].val;

                options.result[index].end.ts    = data[i].ts;
                options.result[index].end.val   = data[i].val;
            } else {
                if (data[i].val !== null) {
                    if (data[i].val > options.result[index].max.val) {
                        options.result[index].max.ts    = data[i].ts;
                        options.result[index].max.val   = data[i].val;
                    } else if (data[i].val < options.result[index].min.val) {
                        options.result[index].min.ts    = data[i].ts;
                        options.result[index].min.val   = data[i].val;
                    }
                    if (data[i].ts > options.result[index].end.ts) {
                        options.result[index].end.ts    = data[i].ts;
                        options.result[index].end.val   = data[i].val;
                    }
                } else {
                    if (data[i].ts > options.result[index].end.ts) {
                        options.result[index].end.ts    = data[i].ts;
                        options.result[index].end.val   = null;
                    }
                }
            }
        }
    }

    return  {result: options.result, step: options.step, sourceLength: data.length} ;
}

function finishAggregation(options) {
    if (options.aggregate === 'minmax') {
        for (let ii = options.result.length - 1; ii >= 0; ii--) {
            // no one value in this period
            if (options.result[ii].start.ts === null) {
                options.result.splice(ii, 1);
            } else {
                // just one value in this period: max == min == start == end
                if (options.result[ii].start.ts === options.result[ii].end.ts) {
                    options.result[ii] = {
                        ts:  options.result[ii].start.ts,
                        val: options.result[ii].start.val
                    };
                } else
                if (options.result[ii].min.ts   === options.result[ii].max.ts) {
                    // if just 2 values: start == min == max, end
                    if (options.result[ii].start.ts === options.result[ii].min.ts ||
                        options.result[ii].end.ts   === options.result[ii].min.ts) {
                        options.result.splice(ii + 1, 0, {
                            ts:  options.result[ii].end.ts,
                            val: options.result[ii].end.val
                        });
                        options.result[ii] = {
                            ts:  options.result[ii].start.ts,
                            val: options.result[ii].start.val
                        };
                    } // if just 3 values: start, min == max, end
                    else {
                        options.result.splice(ii + 1, 0, {
                            ts:  options.result[ii].max.ts,
                            val: options.result[ii].max.val
                        });
                        options.result.splice(ii + 2, 0, {
                            ts:  options.result[ii].end.ts,
                            val: options.result[ii].end.val
                        });
                        options.result[ii] = {
                            ts:  options.result[ii].start.ts,
                            val: options.result[ii].start.val
                        };
                    }
                } else
                if (options.result[ii].start.ts === options.result[ii].max.ts) {
                    // just one value in this period: start == max, min == end
                    if (options.result[ii].min.ts === options.result[ii].end.ts) {
                        options.result.splice(ii + 1, 0, {
                            ts:  options.result[ii].end.ts,
                            val: options.result[ii].end.val
                        });
                        options.result[ii] = {
                            ts:  options.result[ii].start.ts,
                            val: options.result[ii].start.val
                        };
                    } // start == max, min, end
                    else {
                        options.result.splice(ii + 1, 0, {
                            ts:  options.result[ii].min.ts,
                            val: options.result[ii].min.val
                        });
                        options.result.splice(ii + 2, 0, {
                            ts:  options.result[ii].end.ts,
                            val: options.result[ii].end.val
                        });
                        options.result[ii] = {
                            ts:  options.result[ii].start.ts,
                            val: options.result[ii].start.val
                        };
                    }
                } else
                if (options.result[ii].end.ts   === options.result[ii].max.ts) {
                    // just one value in this period: start == min, max == end
                    if (options.result[ii].min.ts === options.result[ii].start.ts) {
                        options.result.splice(ii + 1, 0, {
                            ts:  options.result[ii].end.ts,
                            val: options.result[ii].end.val
                        });
                        options.result[ii] = {
                            ts:  options.result[ii].start.ts,
                            val: options.result[ii].start.val
                        };
                    } // start, min, max == end
                    else {
                        options.result.splice(ii + 1, 0, {
                            ts:  options.result[ii].min.ts,
                            val: options.result[ii].min.val
                        });
                        options.result.splice(ii + 2, 0, {
                            ts:  options.result[ii].end.ts,
                            val: options.result[ii].end.val
                        });
                        options.result[ii] = {
                            ts:  options.result[ii].start.ts,
                            val: options.result[ii].start.val
                        };
                    }
                } else
                if (options.result[ii].start.ts === options.result[ii].min.ts ||
                    options.result[ii].end.ts   === options.result[ii].min.ts) {
                    // just one value in this period: start == min, max, end
                    options.result.splice(ii + 1, 0, {
                        ts:  options.result[ii].max.ts,
                        val: options.result[ii].max.val
                    });
                    options.result.splice(ii + 2, 0, {
                        ts:  options.result[ii].end.ts,
                        val: options.result[ii].end.val
                    });
                    options.result[ii] = {
                        ts:  options.result[ii].start.ts,
                        val: options.result[ii].start.val
                    };
                } else {
                    // just one value in this period: start == min, max, end
                    if (options.result[ii].max.ts > options.result[ii].min.ts) {
                        options.result.splice(ii + 1, 0, {
                            ts:  options.result[ii].min.ts,
                            val: options.result[ii].min.val
                        });
                        options.result.splice(ii + 2, 0, {
                            ts:  options.result[ii].max.ts,
                            val: options.result[ii].max.val
                        });
                    } else {
                        options.result.splice(ii + 1, 0, {
                            ts:  options.result[ii].max.ts,
                            val: options.result[ii].max.val
                        });
                        options.result.splice(ii + 2, 0, {
                            ts:  options.result[ii].min.ts,
                            val: options.result[ii].min.val
                        });
                    }
                    options.result.splice(ii + 3, 0, {
                        ts:  options.result[ii].end.ts,
                        val: options.result[ii].end.val
                    });
                    options.result[ii] = {
                        ts:  options.result[ii].start.ts,
                        val: options.result[ii].start.val
                    };
                }
            }
        }
    } else if (options.aggregate === 'average') {
        for (let k = options.result.length - 1; k >= 0; k--) {
            if (options.result[k].val.ts) {
                options.result[k] = {
                    ts:  options.result[k].val.ts,
                    val: options.result[k].val.val !== null ? Math.round(options.result[k].val.val / options.averageCount[k] * 100) / 100 : null
                };
            } else {
                // no one value in this interval
                options.result.splice(k, 1);
            }
        }
    } else {
        for (let j = options.result.length - 1; j >= 0; j--) {
            if (options.result[j].val.ts) {
                options.result[j] = {
                    ts:  options.result[j].val.ts,
                    val: options.result[j].val.val
                };
            } else {
                // no one value in this interval
                options.result.splice(j, 1);
            }
        }
    }

    beautify(options);
}

function beautify(options) {
    let preFirstValue = null;
    let postLastValue = null;

    if (options.ignoreNull === 'true')  { // include nulls and replace them with last value
        options.ignoreNull = true;
    } else
    if (options.ignoreNull === 'false') { // include nulls
        options.ignoreNull = false;
    } else
    if (options.ignoreNull === '0') { // include nulls and replace them with 0
        options.ignoreNull = 0;
    } else
    if (options.ignoreNull !== true && options.ignoreNull !== false && options.ignoreNull !== 0) {
        options.ignoreNull = false;
    }

    // process null values, remove points outside the span and find first points after end and before start
    for (let i = 0; i < options.result.length; i++) {
        if (options.ignoreNull !== false) {
            // if value is null
            if (options.result[i].val === null) {
                // null value must be replaced with last not null value
                if (options.ignoreNull === true) {
                    // remove value
                    options.result.splice(i, 1);
                    i--;
                    continue;
                } else {
                    // null value must be replaced with 0
                    options.result[i].val = options.ignoreNull;
                }
            }
        }

        // remove all not requested points
        if (options.result[i].ts < options.start) {
            preFirstValue = options.result[i].val !== null ? options.result[i] : null;
            options.result.splice(i, 1);
            i--;
            continue;
        }

        postLastValue = options.result[i].val !== null ? options.result[i] : null;

        if (options.result[i].ts > options.end) {
            options.result.splice(i, options.result.length - i);
            break;
        }
    }

    // check start and stop
    if (options.result.length && options.aggregate !== 'none') {
        const firstTS = options.result[0].ts;

        if (firstTS > options.start) {
            if (preFirstValue) {
                const firstY = options.result[0].val;
                // if steps
                if (options.aggregate === 'onchange' || !options.aggregate) {
                    if (preFirstValue.ts !== firstTS) {
                        options.result.unshift({ts: options.start, val: preFirstValue.val});
                    } else {
                        if (options.ignoreNull) {
                            options.result.unshift({ts: options.start, val: firstY});
                        }
                    }
                } else {
                    if (preFirstValue.ts !== firstTS) {
                        if (firstY !== null) {
                            // interpolate
                            const y = preFirstValue.val + (firstY - preFirstValue.val) * (options.start - preFirstValue.ts) / (firstTS - preFirstValue.ts);
                            options.result.unshift({ts: options.start, val: y, i: true});
                        } else {
                            options.result.unshift({ts: options.start, val: null});
                        }
                    } else {
                        if (options.ignoreNull) {
                            options.result.unshift({ts: options.start, val: firstY});
                        }
                    }
                }
            } else {
                if (options.ignoreNull) {
                    options.result.unshift({ts: options.start, val: options.result[0].val});
                } else {
                    options.result.unshift({ts: options.start, val: null});
                }
            }
        }

        const lastTS = options.result[options.result.length - 1].ts;
        if (lastTS < options.end) {
            if (postLastValue) {
                // if steps
                if (options.aggregate === 'onchange' || !options.aggregate) {
                    // if more data following, draw line to the end of chart
                    if (postLastValue.ts !== lastTS) {
                        options.result.push({ts: options.end, val: postLastValue.val});
                    } else {
                        if (options.ignoreNull) {
                            options.result.push({ts: options.end, val: postLastValue.val});
                        }
                    }
                } else {
                    if (postLastValue.ts !== lastTS) {
                        const lastY = options.result[options.result.length - 1].val;
                        if (lastY !== null) {
                            // make interpolation
                            const _y = lastY + (postLastValue.val - lastY) * (options.end - lastTS) / (postLastValue.ts - lastTS);
                            options.result.push({ts: options.end, val: _y, i: true});
                        } else {
                            options.result.push({ts: options.end, val: null});
                        }
                    } else {
                        if (options.ignoreNull) {
                            options.result.push({ts: options.end, val: postLastValue.val});
                        }
                    }
                }
            } else {
                if (options.ignoreNull) {
                    const lastY = options.result[options.result.length - 1].val;
                    // if no more data, that means do not draw line
                    options.result.push({ts: options.end, val: lastY});
                } else {
                    // if no more data, that means do not draw line
                    options.result.push({ts: options.end, val: null});
                }
            }
        }
    } else if (options.aggregate === 'none') {
        if (options.count && options.result.length > options.count) {
            options.result = options.result.slice(0, options.count);
        }
    }
}

function sendResponse(adapter, msg, options, data, startTime) {
    let aggregateData;
    if (typeof data === 'string') {
        adapter.log.error(data);
        return adapter.sendTo(msg.from, msg.command, {
            result:     [],
            step:       0,
            error:      data,
            sessionId:  options.sessionId
        }, msg.callback);
    }

    if (options.count && !options.start && data.length > options.count) {
        data.splice(0, data.length - options.count);
    }
    if (data[0]) {
        options.start = options.start || data[0].ts;

        if (!options.aggregate || options.aggregate === 'onchange' || options.aggregate === 'none') {
            aggregateData = {result: data, step: 0, sourceLength: data.length};

            // convert ack from 0/1 to false/true
            if (options.ack && aggregateData.result) {
                for (let i = 0; i < aggregateData.result.length; i++) {
                    aggregateData.result[i].ack = !!aggregateData.result[i].ack;
                }
            }
            options.result = aggregateData.result;

            beautify(options);

            if (options.aggregate === 'none' && options.count && options.result.length > options.count) {
                options.result = options.result.slice(0, options.count);
            }
            aggregateData.result = options.result;
        } else {
            initAggregate(options);
            aggregateData = aggregation(options, data);
            finishAggregation(options);
            aggregateData.result = options.result;
        }

        adapter.log.debug(`Send: ${aggregateData.result.length} of: ${aggregateData.sourceLength} in: ${Date.now() - startTime}ms`);

        adapter.sendTo(msg.from, msg.command, {
            result:     aggregateData.result,
            step:       aggregateData.step,
            error:      null,
            sessionId:  options.sessionId
        }, msg.callback);
    } else {
        adapter.log.info('No Data');
        adapter.sendTo(msg.from, msg.command, {result: [], step: null, error: null, sessionId: options.sessionId}, msg.callback);
    }
}

function sendResponseCounter(adapter, msg, options, data, startTime) {
    // data
    // 1586713810000	100
    // 1586713810010	200
    // 1586713810040	500
    // 1586713810050	0
    // 1586713810090	400
    // 1586713810100	0
    // 1586713810110	100
    if (typeof data === 'string') {
        adapter.log.error(data);
        return adapter.sendTo(msg.from, msg.command, {
            result:     [],
            error:      data,
            sessionId:  options.sessionId
        }, msg.callback);
    }

    if (data[0] && data[1]) {
        // first | start          | afterFirst | ...... | last | end            | afterLast
        // 5     |                | 8          |  9 | 1 | 3    |                | 5
        //       | 5+(8-5/tsDiff) |            |  9 | 1 |      | 3+(5-3/tsDiff) |
        //       (9 - 6.5) + (4 - 1)

        if (data[1].ts === options.start) {
            data.splice(0, 1);
        }

        if (data[0].ts < options.start && data[0].val > data[1].val) {
            data.splice(0, 1);
        }

        // interpolate from first to start time
        if (data[0].ts < options.start) {
            const val = data[0].val + (data[1].val - data[0].val) * ((options.start - data[0].ts) / (data[1].ts - data[0].ts));
            data.splice(0, 1);
            data.unshift({ts: options.start, val, i: true});
        }

        if (data[data.length - 2].ts === options.end) {
            data.splice(-1, 1);
        }

        const veryLast   = data[data.length - 1];
        const beforeLast = data[data.length - 2];

        // interpolate from first to start time
        if (options.end < veryLast.ts) {
            const val = beforeLast.val + (veryLast.val - beforeLast.val) * ((options.end - beforeLast.ts) / (veryLast.ts - beforeLast.ts));
            data.splice(-1, 1);
            data.push({ts: options.end, val, i: true});
        }

        // at this point we expect [6.5, 9, 1, 4]
        // at this point we expect [150, 200, 500, 0, 400, 0, 50]
        let val = data[data.length - 1].val;
        let sum = 0;
        for (let i = data.length - 2; i >= 0; i--) {
            if (data[i].val < val) {
                sum += val - data[i].val;
            }
            val = data[i].val;
        }

        adapter.sendTo(msg.from, msg.command, {
            result:     sum,
            error:      null,
            sessionId:  options.sessionId
        }, msg.callback);
    } else {
        adapter.log.info('No Data');
        adapter.sendTo(msg.from, msg.command, {result: 0, step: null, error: null, sessionId: options.sessionId}, msg.callback);
    }
}

module.exports.sendResponseCounter = sendResponseCounter;
module.exports.sendResponse        = sendResponse;
module.exports.initAggregate       = initAggregate;
module.exports.aggregation         = aggregation;
module.exports.beautify            = beautify;
module.exports.finishAggregation   = finishAggregation;
