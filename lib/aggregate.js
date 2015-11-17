function aggregate(data, options) {
    if (data && data.length) {
        if (typeof data[0].val !== 'number') {
            return {result: data, step: 0, sourceLength: data.length};
        }
        var start = new Date(options.start * 1000);
        var end   = new Date(options.end   * 1000);

        var step = 1; // 1 Step is 1 second
        if (options.step) {
            step = options.step;
        } else {
            step = Math.round((options.end - options.start) / options.count) ;
        }

        // Limit 2000
        if ((options.end - options.start) / step > options.limit) {
            step = Math.round((options.end - options.start) / options.limit);
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
            aggregateData = aggregate(data, options);
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