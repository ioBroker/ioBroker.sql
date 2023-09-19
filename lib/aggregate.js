/* jshint -W097 */
/* jshint strict: false */
/* jslint node: true */
'use strict';
// THIS file should be identical with sql and history adapter's one

function initAggregate(options) {
    let log = () => {};
    if (options.debugLog) {
        log = options.log || console.log;
    }

    // step; // 1 Step is 1 second
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
    options.quantileDatapoints = [];
    options.integralDatapoints = [];
    options.aggregate      = options.aggregate || 'minmax';
    options.overallLength  = 0;

    if (options.aggregate === 'percentile') {
        if (typeof options.percentile !== 'number' || options.percentile < 0 || options.percentile > 100) {
            options.percentile = 50;
        }
        options.quantile = options.percentile / 100; // Internally we use quantile for percentile too
    }
    if (options.aggregate === 'quantile') {
        if (typeof options.quantile !== 'number' || options.quantile < 0 || options.quantile > 1) {
            options.quantile = 0.5;
        }
    }
    if (options.aggregate === 'integral') {
        if (typeof options.integralUnit !== 'number' || options.integralUnit <= 0) {
            options.integralUnit = 60;
        }
        options.integralUnit *= 1000; // Convert to milliseconds
    }

    log(`${options.logId} Initialize: maxIndex = ${options.maxIndex}, step = ${options.step}, start = ${options.start}, end = ${options.end}`);
    // pre-fill the result with timestamps (add one before start and one after end)
    try {
        options.result.length = options.maxIndex + 2;
    } catch (err) {
        err.message += `: ${options.maxIndex + 2}`;
        throw err;
    }
    // We define the array length but do not prefill values, we do that on runtime when needed
    options.result[0] = {
        val:   {ts: null, val: null},
        max:   {ts: null, val: null},
        min:   {ts: null, val: null},
        start: {ts: null, val: null},
        end:   {ts: null, val: null}
    };
    options.result[options.maxIndex + 2] = {
        val:   {ts: null, val: null},
        max:   {ts: null, val: null},
        min:   {ts: null, val: null},
        start: {ts: null, val: null},
        end:   {ts: null, val: null}
    };

    if (options.aggregate === 'average') {
        options.averageCount[0] = 0;
        options.averageCount[options.maxIndex + 2] = 0;
    }

    if (options.aggregate === 'percentile' || options.aggregate === 'quantile') {
        options.quantileDatapoints[0] = [];
        options.quantileDatapoints[options.maxIndex + 2] = [];
    }
    if (options.aggregate === 'integral') {
        options.integralDatapoints[0] = [];
        options.integralDatapoints[options.maxIndex + 2] = [];
    }
    return options;
}

function aggregation(options, data) {
    let index;
    let preIndex;

    let collectedTooEarlyData = [];
    let collectedTooLateData = [];
    let preIndexValueFound = false;
    let postIndexValueFound = false;

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
            // if the ts is even earlier than the "pre-interval" ignore it, else we collect all data there
            if (preIndex < -1) {
                collectedTooEarlyData.push(data[i]);
                continue;
            }
            preIndexValueFound = true;
        } else if (preIndex > options.maxIndex) {
            index = options.maxIndex + 2;
            // if the ts is even later than the "post-interval" ignore it, else we collect all data there
            if (preIndex > options.maxIndex + 1) {
                collectedTooLateData.push(data[i]);
                continue;
            }
            postIndexValueFound = true;
        } else {
            index = preIndex + 1;
        }
        options.overallLength++;

        if (options.result[index] === undefined) { // lazy initialization of data structure
            options.result[index] = {
                val:   {ts: null, val: null},
                max:   {ts: null, val: null},
                min:   {ts: null, val: null},
                start: {ts: null, val: null},
                end:   {ts: null, val: null},
            };

            if (options.aggregate === 'average' || options.aggregate === 'count') {
                options.averageCount[index] = 0;
            }

            if (options.aggregate === 'percentile' || options.aggregate === 'quantile') {
                options.quantileDatapoints[index] = [];
            }
            if (options.aggregate === 'integral') {
                options.integralDatapoints[index] = [];
            }
        }

        aggregationLogic(data[i], index, options);
    }

    // If no data was found in the pre-interval, but we have earlier data, we put the latest of them in the pre-interval
    if (!preIndexValueFound && collectedTooEarlyData.length > 0) {
        collectedTooEarlyData = collectedTooEarlyData.sort(sortByTs);
        options.overallLength++;
        aggregationLogic(collectedTooEarlyData[collectedTooEarlyData.length - 1], 0, options);
    }
    // If no data was found in the post-interval, but we have later data, we put the earliest of them in the post-interval
    if (!postIndexValueFound && collectedTooLateData.length > 0) {
        collectedTooLateData = collectedTooLateData.sort(sortByTs);
        options.overallLength++;
        aggregationLogic(collectedTooLateData[0], options.maxIndex + 2, options);
    }

    return  { result: options.result, step: options.step, sourceLength: data.length };
}

function aggregationLogic(data, index, options) {
    let log = () => {};
    if (options.debugLog) {
        log = options.log || console.log;
    }

    if (!options.result[index]) {
        log(`${options.logId} Data index ${index} not initialized, ignore!`);
        return;
    }

    if (options.aggregate !== 'minmax' && !options.result[index].val.ts) {
        options.result[index].val.ts = Math.round(options.start + (((index - 1) + 0.5) * options.step));
    }

    if (options.aggregate === 'max') {
        if (options.result[index].val.val === null || options.result[index].val.val < data.val) {
            options.result[index].val.val = data.val;
        }
    } else if (options.aggregate === 'min') {
        if (options.result[index].val.val === null || options.result[index].val.val > data.val) {
            options.result[index].val.val = data.val;
        }
    } else if (options.aggregate === 'average') {
        options.result[index].val.val += parseFloat(data.val);
        options.averageCount[index]++;
    } else if (options.aggregate === 'count') {
        options.averageCount[index]++;
    } else if (options.aggregate === 'total') {
        options.result[index].val.val += parseFloat(data.val);
    } else if (options.aggregate === 'minmax') {
        if (options.result[index].min.ts === null) {
            options.result[index].min.ts    = data.ts;
            options.result[index].min.val   = data.val;

            options.result[index].max.ts    = data.ts;
            options.result[index].max.val   = data.val;

            options.result[index].start.ts  = data.ts;
            options.result[index].start.val = data.val;

            options.result[index].end.ts    = data.ts;
            options.result[index].end.val   = data.val;
        } else {
            if (data.val !== null) {
                if (data.val > options.result[index].max.val) {
                    options.result[index].max.ts    = data.ts;
                    options.result[index].max.val   = data.val;
                } else if (data.val < options.result[index].min.val) {
                    options.result[index].min.ts    = data.ts;
                    options.result[index].min.val   = data.val;
                }
                if (data.ts > options.result[index].end.ts) {
                    options.result[index].end.ts    = data.ts;
                    options.result[index].end.val   = data.val;
                }
            } else {
                if (data.ts > options.result[index].end.ts) {
                    options.result[index].end.ts    = data.ts;
                    options.result[index].end.val   = null;
                }
            }
        }
    } else if (options.aggregate === 'percentile' || options.aggregate === 'quantile') {
        options.quantileDatapoints[index].push(data.val);
        log(`${options.logId} Quantile ${index}: Add ts= ${data.ts} val=${data.val}`);
    } else if (options.aggregate === 'integral') {
        options.integralDatapoints[index].push(data);
        log(`${options.logId} Integral ${index}: Add ts= ${data.ts} val=${data.val}`);
    }
}

function finishAggregation(options) {
    let log = () => {};
    if (options.debugLog) {
        log = options.log || console.log;
    }

    if (options.aggregate === 'minmax') {
        let preBorderValueRemoved = false;
        let postBorderValueRemoved = false;
        const originalResultLength = options.result.length;

        let startIndex = 0;
        let endIndex = options.result.length;
        const finalResult = [];

        for (let ii = startIndex; ii < endIndex; ii++) {
            // no one value in this period
            if (options.result[ii] === undefined || options.result[ii].start.ts === null) {
                if (ii === 0) {
                    preBorderValueRemoved = true;
                } else if (ii === originalResultLength - 1) {
                    postBorderValueRemoved = true;
                }
                // options.result.splice(ii, 1);
                continue;
            }
            // just one value in this period: max == min == start == end
            if (options.result[ii].start.ts === options.result[ii].end.ts) {
                finalResult.push({
                    ts:  options.result[ii].start.ts,
                    val: options.result[ii].start.val
                });
            } else
            if (options.result[ii].min.ts   === options.result[ii].max.ts) {
                // if just 2 values: start == min == max, end
                if (options.result[ii].start.ts === options.result[ii].min.ts ||
                    options.result[ii].end.ts   === options.result[ii].min.ts) {
                    finalResult.push({
                        ts:  options.result[ii].start.ts,
                        val: options.result[ii].start.val
                    });
                    finalResult.push({
                        ts:  options.result[ii].end.ts,
                        val: options.result[ii].end.val
                    });
                } // if just 3 values: start, min == max, end
                else {
                    finalResult.push({
                        ts:  options.result[ii].start.ts,
                        val: options.result[ii].start.val
                    });
                    finalResult.push({
                        ts:  options.result[ii].max.ts,
                        val: options.result[ii].max.val
                    });
                    finalResult.push({
                        ts:  options.result[ii].end.ts,
                        val: options.result[ii].end.val
                    });
                }
            } else
            if (options.result[ii].start.ts === options.result[ii].max.ts) {
                // just one value in this period: start == max, min == end
                if (options.result[ii].min.ts === options.result[ii].end.ts) {
                    finalResult.push({
                        ts:  options.result[ii].start.ts,
                        val: options.result[ii].start.val
                    });
                    finalResult.push({
                        ts:  options.result[ii].end.ts,
                        val: options.result[ii].end.val
                    });
                } // start == max, min, end
                else {
                    finalResult.push({
                        ts:  options.result[ii].start.ts,
                        val: options.result[ii].start.val
                    });
                    finalResult.push({
                        ts:  options.result[ii].min.ts,
                        val: options.result[ii].min.val
                    });
                    finalResult.push({
                        ts:  options.result[ii].end.ts,
                        val: options.result[ii].end.val
                    });
                }
            } else
            if (options.result[ii].end.ts === options.result[ii].max.ts) {
                // just one value in this period: start == min, max == end
                if (options.result[ii].min.ts === options.result[ii].start.ts) {
                    finalResult.push({
                        ts:  options.result[ii].start.ts,
                        val: options.result[ii].start.val
                    });
                    finalResult.push({
                        ts:  options.result[ii].end.ts,
                        val: options.result[ii].end.val
                    });
                } // start, min, max == end
                else {
                    finalResult.push({
                        ts:  options.result[ii].start.ts,
                        val: options.result[ii].start.val
                    });
                    finalResult.push({
                        ts:  options.result[ii].min.ts,
                        val: options.result[ii].min.val
                    });
                    finalResult.push({
                        ts:  options.result[ii].end.ts,
                        val: options.result[ii].end.val
                    });
                }
            } else
            if (options.result[ii].start.ts === options.result[ii].min.ts ||
                options.result[ii].end.ts   === options.result[ii].min.ts) {
                // just one value in this period: start == min, max, end
                finalResult.push({
                    ts:  options.result[ii].start.ts,
                    val: options.result[ii].start.val
                });
                finalResult.push({
                    ts:  options.result[ii].max.ts,
                    val: options.result[ii].max.val
                });
                finalResult.push({
                    ts:  options.result[ii].end.ts,
                    val: options.result[ii].end.val
                });
            } else {
                finalResult.push({
                    ts:  options.result[ii].start.ts,
                    val: options.result[ii].start.val
                });
                // just one value in this period: start == min, max, end
                if (options.result[ii].max.ts > options.result[ii].min.ts) {
                    finalResult.push({
                        ts:  options.result[ii].min.ts,
                        val: options.result[ii].min.val
                    });
                    finalResult.push({
                        ts:  options.result[ii].max.ts,
                        val: options.result[ii].max.val
                    });
                } else {
                    finalResult.push({
                        ts:  options.result[ii].max.ts,
                        val: options.result[ii].max.val
                    });
                    finalResult.push({
                        ts:  options.result[ii].min.ts,
                        val: options.result[ii].min.val
                    });
                }
                finalResult.push({
                    ts:  options.result[ii].end.ts,
                    val: options.result[ii].end.val
                });
            }
        }
        if (options.removeBorderValues) { // we cut out the additional results
            if (!preBorderValueRemoved) {
                finalResult.splice(0, 1);
            }
            if (!postBorderValueRemoved) {
                finalResult.length--;
            }
        }
        options.result = finalResult;
    } else if (options.aggregate === 'average') {
        let startIndex = 0;
        let endIndex = options.result.length;
        const finalResult = [];
        if (options.removeBorderValues) { // we cut out the additional results
            // options.result.splice(0, 1);
            // options.averageCount.splice(0, 1);
            // options.result.length--;
            // options.averageCount.length--;
            startIndex++;
            endIndex--;
        }
        for (let k = startIndex; k < endIndex; k++) {
            if (options.result[k] !== undefined && options.result[k].val.ts) {
                finalResult.push({
                    ts:  options.result[k].val.ts,
                    val: options.result[k].val.val !== null ? Math.round(options.result[k].val.val / options.averageCount[k] * 100) / 100 : null
                });
            } else {
                // no one value in this interval
                // options.result.splice(k, 1);
                // options.averageCount.splice(k, 1); // not needed to clean up because not used anymore afterwards
            }
        }
        options.result = finalResult;
    }  else if (options.aggregate === 'count') {
        let startIndex = 0;
        let endIndex = options.result.length;
        const finalResult = [];
        if (options.removeBorderValues) { // we cut out the additional results
            // options.result.splice(0, 1);
            // options.averageCount.splice(0, 1);
            // options.result.length--;
            // options.averageCount.length--;
            startIndex++;
            endIndex--;
        }
        for (let k = startIndex; k < endIndex; k++) {
            if (options.result[k] !== undefined && options.result[k].val.ts) {
                finalResult.push({
                    ts:  options.result[k].val.ts,
                    val: options.averageCount[k],
                });
            } else {
                // no one value in this interval
                // options.result.splice(k, 1);
                // options.averageCount.splice(k, 1); // not needed to clean up because not used anymore afterward
            }
        }
        options.result = finalResult;
    } else if (options.aggregate === 'integral') {
        let preBorderValueRemoved = false;
        let postBorderValueRemoved = false;
        const originalResultLength = options.result.length;
        const finalResult = [];

        for (let k = 0; k < options.result.length; k++) {
            let indexStartTs = options.start + ((k - 1) * options.step);
            let indexEndTs = indexStartTs + options.step;
            if (options.integralDatapoints[k] && options.integralDatapoints[k].length) {
                // Sort data points by ts first
                options.integralDatapoints[k].sort(sortByTs);
            }
            // Make sure that we have entries that always start at the beginning of the interval
            if ((!options.integralDatapoints[k] || !options.integralDatapoints[k].length || options.integralDatapoints[k][0].ts > indexStartTs) && options.integralDatapoints[k - 1] && options.integralDatapoints[k -1][options.integralDatapoints[k - 1].length - 1]) {
                // if the first entry of this interval started somewhere in the start of the interval, add a start entry
                // same if there is no entry at all in the timeframe, use last entry from interval before
                options.integralDatapoints[k] = options.integralDatapoints[k] || [];
                options.integralDatapoints[k].unshift({
                    ts:  indexStartTs,
                    val: options.integralDatapoints[k - 1][options.integralDatapoints[k - 1].length - 1].val
                });
                log(`${options.logId} Integral: ${k}: Added start entry for interval with ts=${indexStartTs}, val=${options.integralDatapoints[k][0].val}`);
            } else if (options.integralDatapoints[k] && options.integralDatapoints[k].length && options.integralDatapoints[k][0].ts > indexStartTs) {
                options.integralDatapoints[k].unshift({
                    ts:  indexStartTs,
                    val: options.integralDatapoints[k][0].val
                });
                log(`${options.logId} Integral: ${k}: Added start entry for interval with ts=${indexStartTs}, val=${options.integralDatapoints[k][0].val} with same value as first point in interval because no former datapoint was found`);
            } else if (options.integralDatapoints[k] && options.integralDatapoints[k].length && options.integralDatapoints[k][0].ts < indexStartTs) {
                // if the first entry of this interval started before the start of the interval, search for the last value before the start of the interval, add as start entry
                let preFirstIndex = null;
                for (let kk = 0; kk < options.integralDatapoints[k].length; kk++) {
                    if (options.integralDatapoints[k][kk].ts >= indexStartTs) {
                        break;
                    }
                    preFirstIndex = kk;
                }
                if (preFirstIndex !== null) {
                    options.integralDatapoints[k].splice(0, preFirstIndex, {
                        ts:  indexStartTs,
                        val: options.integralDatapoints[k][preFirstIndex].val
                    });
                    log(`${options.logId} Integral: ${k}: Remove ${preFirstIndex + 1} entries and add start entry for interval with ts=${indexStartTs}, val=${options.integralDatapoints[k][0].val}`);
                }
            }

            const point = {
                ts: options.result[k] !== undefined && options.result[k].val.ts ? options.result[k].val.ts : Math.round(options.start + (((k - 1) + 0.5) * options.step)),
                val: null
            }

            const integralDatapoints = options.integralDatapoints[k] || [];
            const vals = integralDatapoints.map(dp => `[${dp.ts}, ${dp.val}]`);
            log(`${options.logId} Integral: ${k}: ${integralDatapoints.length} datapoints for interval  for ${indexStartTs} - ${indexEndTs}: ${vals.join(',')}`);

            // Calculate Intervals and always calculate till the interval end (start made sure above already)
            for (let kk = 0; kk < integralDatapoints.length; kk++) {
                const valEndTs = integralDatapoints[kk + 1] ? Math.min(integralDatapoints[kk + 1].ts, indexEndTs) : indexEndTs;
                let valDuration = valEndTs - integralDatapoints[kk].ts;
                if (valDuration < 0) {
                    log(`${options.logId} Integral: ${k}[${kk}] data do not belong to this interval, ignore ${JSON.stringify(integralDatapoints[kk])} (vs. ${valEndTs})`)
                    break;
                }
                if (valDuration === 0) {
                    log(`${options.logId} Integral: ${k}[${kk}] valDuration zero, ignore ${JSON.stringify(integralDatapoints[kk])}`)
                    continue;
                }
                let valStart = parseFloat(integralDatapoints[kk].val) || 0;
                // End value is the next value, or if none, assume "linearity
                let valEnd = parseFloat((integralDatapoints[kk + 1] ? integralDatapoints[kk + 1].val : (options.integralDatapoints[k + 1] && options.integralDatapoints[k + 1][0] ? options.integralDatapoints[k + 1][0].val : valStart))) || 0;
                if (options.integralInterpolation !== 'linear' || valStart === valEnd) {
                    const integralAdd = valStart * valDuration / options.integralUnit;
                    // simple rectangle linear interpolation
                    log(`${options.logId} Integral: ${k}[${kk}] : Add ${integralAdd} from val=${valStart} for ${valDuration}`);
                    point.val += integralAdd;
                } else if ((valStart >= 0 && valEnd >= 0) || (valStart <= 0 && valEnd <= 0)) {
                    // start and end are both positive or both negative, or one is 0
                    let multiplier = 1;
                    if (valStart <= 0 && valEnd <= 0) {
                        multiplier = -1; // correct the sign at the end
                        valStart = -valStart;
                        valEnd = -valEnd;
                    }
                    const minVal = Math.min(valStart, valEnd);
                    const maxVal = Math.max(valStart, valEnd);
                    const rectPart = minVal * valDuration / options.integralUnit;
                    const trianglePart = (maxVal - minVal) * valDuration * 0.5 / options.integralUnit;
                    const integralAdd = (rectPart + trianglePart) * multiplier;
                    log(`${options.logId} Integral: ${k}[${kk}] : Add R${rectPart} + T${trianglePart} => ${integralAdd} from val=${valStart} to ${valEnd} for ${valDuration}`);
                    point.val += integralAdd;
                } else {
                    // Values are on different sides of 0, so we need to find the 0 crossing
                    const zeroCrossing = Math.abs((valStart * valDuration) / (valEnd - valStart));
                    // Then calculate two linear segments, one from 0 to the crossing, and one from the crossing to the end
                    const trianglePart1 = valStart * zeroCrossing * 0.5 / options.integralUnit;
                    const trianglePart2 = valEnd * (valDuration - zeroCrossing) * 0.5 / options.integralUnit;
                    const integralAdd = trianglePart1 + trianglePart2;
                    log(`${options.logId} Integral: ${k}[${kk}] : Add T${trianglePart1} + T${trianglePart2} => ${integralAdd} from val=${valStart} to ${valEnd} for ${valDuration} (zero crossing ${zeroCrossing})`);
                    point.val += integralAdd;
                }
            }
            /*options.result[k] = {
                ts: options.result[k].val.ts,
                val: options.result[k].val.val
            }*/
            if (point.val !== null) {
                finalResult.push(point);
            } else {
                if (k === 0) {
                    preBorderValueRemoved = true;
                } else if (k === originalResultLength - 1) {
                    postBorderValueRemoved = true;
                }
            }
        }
        if (options.removeBorderValues) { // we cut out the additional results
            if (!preBorderValueRemoved) {
                finalResult.splice(0, 1);
            }
            if (!postBorderValueRemoved) {
                finalResult.length--;
            }
        }
        options.result = finalResult;
    } else if (options.aggregate === 'percentile' || options.aggregate === 'quantile') {
        let startIndex = 0;
        let endIndex = options.result.length;
        const finalResult = [];
        if (options.removeBorderValues) { // we cut out the additional results
            /*
            options.result.splice(0, 1);
            options.quantileDatapoints.splice(0, 1);
            options.result.length--
            options.quantileDatapoints.length--;
            */
            startIndex++;
            endIndex--;
        }
        for (let k = startIndex; k < endIndex; k++) {
            if (options.result[k] !== undefined && options.result[k].val.ts) {
                const point = {
                    ts:  options.result[k].val.ts,
                    val: quantile(options.quantile, options.quantileDatapoints[k])
                };
                log(`${options.logId} Quantile ${k} ${point.ts}: ${options.quantileDatapoints[k].join(', ')} -> ${point.val}`);
                finalResult.push(point);
            } else {
                // no one value in this interval
                // options.result.splice(k, 1);
                // options.quantileDatapoints.splice(k, 1); // not needed to clean up because not used anymore afterward
            }
        }
        options.result = finalResult;
    } else {
        let startIndex = 0;
        let endIndex = options.result.length;
        const finalResult = [];
        if (options.removeBorderValues) { // we cut out the additional results
            // options.result.splice(0, 1);
            // options.result.length--;
            startIndex++;
            endIndex--;
        }
        for (let j = startIndex; j < endIndex; j++) {
            if (options.result[j] !== undefined && options.result[j].val.ts) {
                finalResult.push({
                    ts:  options.result[j].val.ts,
                    val: options.result[j].val.val
                });
            } else {
                // no one value in this interval
                //options.result.splice(j, 1);
            }
        }
        options.result = finalResult;
    }

    beautify(options);
}

function beautify(options) {
    let log = () => {};
    if (options.debugLog) {
        log = options.log || console.log;
    }

    log(`${options.logId} Beautify: ${options.result.length} results`);
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
    if (options.result.length && options.aggregate !== 'none' && !options.removeBorderValues) {
        const firstTS = options.result[0].ts;

        if (firstTS > options.start && !options.removeBorderValues) {
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
                            log(`${options.logId} interpolate ${y} from ${preFirstValue.val} to ${firstY} as first return value`);
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
        if (lastTS < options.end && !options.removeBorderValues) {
            if (postLastValue) {
                // if steps
                if (options.aggregate === 'onchange' || !options.aggregate) {
                    // if more data following, draw line to the end of the chart
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
                            log(`${options.logId} interpolate ${_y} from ${lastY} to ${postLastValue.val} as last return value`);
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
            options.result.splice(0, options.result.length - options.count);
        }
    }

    if (options.addId) {
        for (let i = 0; i < options.result.length; i++) {
            if (!options.result[i].id && options.id) {
                options.result[i].id = options.index || options.id;
            }
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

        if (!options.aggregate || options.aggregate === 'onchange' || options.aggregate === 'none' || options.preAggregated) {
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
                options.result.splice(0, options.result.length - options.count);
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

        if (data[data.length - 2] !== undefined && data[data.length - 2].ts === options.end) {
            data.length--;
        }

        const veryLast   = data[data.length - 1];
        const beforeLast = data[data.length - 2];

        // interpolate from end time to last
        if (veryLast !== undefined && beforeLast !== undefined && options.end < veryLast.ts) {
            const val = beforeLast.val + (veryLast.val - beforeLast.val) * ((options.end - beforeLast.ts) / (veryLast.ts - beforeLast.ts));
            data.length--;
            data.push({ts: options.end, val, i: true});
        }

        // at this point we expect [6.5, 9, 1, 4]
        // at this point we expect [150, 200, 500, 0, 400, 0, 50]
        let sum = 0;
        if (data.length > 1) {
            let val = data[data.length - 1].val;
            for (let i = data.length - 2; i >= 0; i--) {
                if (data[i].val < val) {
                    sum += val - data[i].val;
                }
                val = data[i].val;
            }
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

/**
 * Get quantile value from an array.
 *
 * @param {Number} q - quantile
 * @param {Array|TypedArray} list - list of sorted values (ascending)
 *
 * @return {number} Quantile value
 */
function getQuantileValue(q, list) {
    if (q === 0) return list[0];

    const index = list.length * q;
    if (Number.isInteger(index)) {
        // mean of two middle numbers
        return (list[index - 1] + list[index]) / 2;
    }
    return list[Math.ceil(index - 1)];
}

/**
 * Calculate quantile for given array of values.
 *
 * @template T
 * @param {Number|Array<Number>} qOrPs - quantile or a list of quantiles
 * @param {Array<T>|Array<Number>|TypedArray} list - array of values
 * @param {function(T): Number} [fn] - optional function to extract a value from an array item
 *
 * @return {Number|T|Array<Number>|Array<T>}
 */
function quantile(qOrPs, list, fn) {
    const q = Array.isArray(qOrPs) ? qOrPs : [qOrPs];

    list = list.slice().sort(function (a, b) {
        if (fn) {
            a = fn(a);
            b = fn(b);
        }

        a = Number.isNaN(a) ? Number.NEGATIVE_INFINITY : a;
        b = Number.isNaN(b) ? Number.NEGATIVE_INFINITY : b;

        if (a > b) {
            return 1;
        }
        if (a < b) {
            return -1;
        }

        return 0;
    });

    if (q.length === 1) {
        return getQuantileValue(q[0], list);
    }

    return q.map(function (q) {
        return getQuantileValue(q, list);
    });
}

function sortByTs(a, b) {
    return a.ts - b.ts;
}

module.exports.sendResponseCounter = sendResponseCounter;
module.exports.sendResponse        = sendResponse;
module.exports.initAggregate       = initAggregate;
module.exports.aggregation         = aggregation;
module.exports.beautify            = beautify;
module.exports.finishAggregation   = finishAggregation;
module.exports.sortByTs            = sortByTs;
