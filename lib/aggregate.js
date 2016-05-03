"use strict";

//function aggregate(data, options) {
//    if (options && options.aggregate == 'onchange') {
//        return onChange(data, options);
//    }
//
//    if (data && data.length) {
//        if (typeof data[0].val !== 'number') {
//            return {result: data, step: 0, sourceLength: data.length};
//        }
//        var start = new Date(options.start);
//        var end   = new Date(options.end);
//
//        var step = 1; // 1 Step is 1 second
//        if (options.step) {
//            step = options.step;
//        } else {
//            step = Math.round((options.end - options.start) / options.count) ;
//        }
//
//        // Limit 2000
//        if ((options.end - options.start) / step > options.limit) {
//            step = Math.round((options.end - options.start) / options.limit);
//        }
//
//        var stepEnd;
//        var i = 0;
//        var result = [];
//        var iStep = 0;
//        options.aggregate = options.aggregate || 'max';
//
//        while (i < data.length && new Date(data[i].ts) < end) {
//            stepEnd = new Date(start);
//            var x = stepEnd.getSeconds();
//            stepEnd.setSeconds(x + step);
//
//            if (stepEnd < start) {
//                // Summer time
//                stepEnd.setHours(start.getHours() + 2);
//            }
//
//            // find all entries in this time period
//            var value = null;
//            var count = 0;
//
//            while (i < data.length && new Date(data[i].ts) < stepEnd) {
//                if (options.aggregate == 'max') {
//                    // Find max
//                    if (value === null || data[i].val > value) value = data[i].val;
//                } else if (options.aggregate == 'min') {
//                    // Find min
//                    if (value === null || data[i].val < value) value = data[i].val;
//                } else if (options.aggregate == 'average') {
//                    if (value === null) value = 0;
//                    value += data[i].val;
//                    count++;
//                } else if (options.aggregate == 'total') {
//                    // Find sum
//                    if (value === null) value = 0;
//                    value += parseFloat(data[i].val);
//                }
//                i++;
//            }
//
//            if (options.aggregate == 'average') {
//                if (!count) {
//                    value = null;
//                } else {
//                    value /= count;
//                    value = Math.round(value * 100) / 100;
//                }
//            }
//            if (value !== null || !options.ignoreNull) {
//                result[iStep] = {ts: stepEnd.getTime()};
//                result[iStep].val = value;
//                iStep++;
//            }
//
//            start = stepEnd;
//        }
//
//        return {result: result, step: step, sourceLength: data.length};
//    } else {
//        return {result: [], step: 0, sourceLength: 0};
//    }
//}

/*function onChange(data, options) {
    if (data && data.length) {
        //var start = new Date(options.start);
        var end   = new Date(options.end);

        var value = null;
        var i = 0;
        var result = [];

        while (i < data.length && new Date(data[i].ts) < end) {
            if (value === null || value != data[i].val) {
                result.push(data[i]);
                value = data[i].val;
                if (result.length > 2000) {
                    break;
                }
            }
        }

        return {result: result, step: step, sourceLength: data.length};
    } else {
        return {result: [], step: 0, sourceLength: 0};
    }
}*/

function initAggregate(options) {

    //step; // 1 Step is 1 second
    if (options.step === null) {
        options.step = (options.end - options.start) / options.count;
    }

    // Limit 2000
    if ((options.end - options.start) / options.step > options.limit) {
        options.step = (options.end - options.start) / options.limit;
    }

    options.maxIndex       = ((options.end - options.start) / options.step) - 1;
    options.result         = [];
    options.averageCount   = [];
    options.aggregate      = options.aggregate || 'm4';
    options.overall_length = 0;

    // pre-fill the result with timestamps
    for (var i = 0; i <= options.maxIndex; i++){

        options.result[i] = {
            val:    {ts: Math.round(options.start + ((i + 0.5) * options.step)), val: null},
            max:    {ts: null, val: null},
            min:    {ts: null, val: null},
            start:  {ts: null, val: null},
            end:    {ts: null, val: null}
        };

        if (options.aggregate === 'average') options.averageCount[i] = 0;
    }
    return options;
}

function aggregation(options, data) {
    var index;
    for (var i = 0; i < data.length; i++) {
        if (data[i].ts < 946681200000) data[i].ts *= 1000;
        
        index = Math.round((data[i].ts - options.start) / options.step);

        if (index > -1 && index <= options.maxIndex) {
            options.overall_length++;

            if (options.aggregate === 'max') {
                if (options.result[index].val.val === null || options.result[index].val.val < data[i].val) options.result[index].val.val = data[i].val;
            } else if (options.aggregate === 'min') {
                if (options.result[index].val.val === null || options.result[index].val.val > data[i].val) options.result[index].val.val = data[i].val;
            } else if (options.aggregate === 'average') {
                //if (value === null) value = 0; //is value even used in this version?
                options.result[index].val.val += data[i].val;
                options.averageCount[index]++;
            } else if (options.aggregate === 'total') {
                //if (value === null) value = 0; //again
                options.result[index].val.val += parseFloat(data[i].val);
            } else if (options.aggregate === 'm4') {
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
    }

    return  {result: options.result, step: options.step, sourceLength: data.length} ;
}

function finishAggregation(options) {
    if (options.aggregate === 'm4') {
        for (var ii = options.result.length - 1; ii >= 0; ii--) {
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
                            val: options.result[ii].max.ts
                        });
                        options.result.splice(ii + 2, 0, {
                            ts:  options.result[ii].end.ts,
                            val: options.result[ii].end.ts
                        });
                        options.result[ii] = {
                            ts:  options.result[ii].start.ts,
                            val: options.result[ii].start.ts
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
    } else if (options.aggregate == 'average') {
        for (var ii = 0; ii < options.result.length; ii++) {
            options.result[ii] = {
                ts:   options.result[ii].val.ts,
                val: (options.result[ii].val.val !== null) ? Math.round(options.result[ii].val.val / options.averageCount[ii] * 100) / 100 : null
            };
        }
    } else {
        for (var ii = 0; ii < options.result.length; ii++) {
            options.result[ii] = {
                ts:  options.result[ii].val.ts,
                val: options.result[ii].val.val
            };
        }
    }
}

function sendResponse(adapter, msg, options, data, startTime) {
    var aggregateData;
    if (typeof data === 'string') {
        adapter.log.error(data);
        return adapter.sendTo(msg.from, msg.command, {
            result: [],
            step:   0,
            error:  data
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
                for (var i = 0; i < aggregateData.result.length; i++) {
                    aggregateData.result[i].ack = !!aggregateData.result[i].ack;
                }
            }
        } else {
            initAggregate(options);
            aggregateData = aggregation(options, data);
            finishAggregation(options);
        }

        adapter.log.info('Send: ' + aggregateData.result.length + ' of: ' + aggregateData.sourceLength + ' in: ' + (new Date().getTime() - startTime) + 'ms');
        adapter.sendTo(msg.from, msg.command, {
            result: aggregateData.result,
            step:   aggregateData.step,
            error:  null
        }, msg.callback);
    } else {
        adapter.log.info('No Data');
        adapter.sendTo(msg.from, msg.command, {result: [], step: null, error: null}, msg.callback);
    }
}

module.exports.sendResponse = sendResponse;
