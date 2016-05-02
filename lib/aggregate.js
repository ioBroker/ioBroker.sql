var overall_length = 0;
var averageCount;
var aggregate;
var maxIndex;
var start;
var end;
var step;
var result;
//var value;

//var finish = false;

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

    start = options.start;
    end   = options.end;

    //step; // 1 Step is 1 second
    if (options.step != null) {
        step = options.step;
    } else {
        step = (options.end - options.start) / options.count;
    }

    // Limit 2000
    if ((options.end - options.start) / step > options.limit) {
        step = (options.end - options.start) / options.limit;
    }


    maxIndex     = ((end - start) / step) -1 ;
    result       = [];
    averageCount = [];
    aggregate    = options.aggregate || 'average';


    //pre-fill the result with timestamps
    for (var i = 0; i <= maxIndex; i++){

        result[i] = {
            ts:  Math.round(start + ((i + 0.5) * step)),
            val: null
        };

        if (aggregate == 'average') averageCount[i] = 0;
    }
}

function aggregation(data) {
    var index;
    for (var i in data) {
        if (data[i].ts < 946681200000) data[i].ts *= 1000;
        
        index = Math.round((data[i].ts - start) / step) ;

        if (index > -1 && index <= maxIndex) {
            overall_length++;
            if (aggregate == 'max') {
                if (result[index].val == null || result[index].val < data[i].val) result[index].val = data[i].val;
            } else if (aggregate == 'min') {
                if (result[index].val == null || result[index].val > data[i].val) result[index].val = data[i].val;
            } else if (aggregate == 'average') {
                //if (value === null) value = 0; //is value even used in this version?
                result[index].val += data[i].val;
                averageCount[index]++;
            } else if (aggregate == 'total') {
                //if (value === null) value = 0; //again
                result[index].val += parseFloat(data[i].val);
            }
        }
    }

    if (aggregate == 'average') {
        for (var ii = 0; ii < result.length; ii++) {
            result[ii].val = result[ii].val !== null ? Math.round(result[ii].val / averageCount[ii] * 100) / 100 : null;
        }
    }

    return  {result: result, step: step, sourceLength: data.length} ;
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

        if (!options.aggregate || options.aggregate === 'none') {
            aggregateData = {result: data, step: 0, sourceLength: data.length};
        } else {
            initAggregate(options);
            aggregateData = aggregation(data, options);
        }
        // convert ack from 0/1 to false/true
        if (options.ack && aggregateData.result) {
            for (var i = 0; i < aggregateData.result.length; i++) {
                aggregateData.result[i].ack = !!aggregateData.result[i].ack;
            }
        }

        adapter.log.info('Send: ' + aggregateData.result.length + ' of: ' + aggregateData.sourceLength + ' in: ' + (new Date().getTime() - startTime) + 'ms');
        adapter.sendTo(msg.from, msg.command, {
            result: aggregateData.result,
            step:   aggregateData.step,
            error:  null
        }, msg.callback);
    } else {
        adapter.log.info('No Data');
        adapter.sendTo(msg.from, msg.command, {result: [].result, step: null, error: null}, msg.callback);
    }
}

module.exports.sendResponse = sendResponse;
