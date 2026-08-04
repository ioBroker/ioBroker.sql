export type SmartDate = {
    // From 1 to 31
    date: number;
    // From 1 to 12
    month: number;
    // from 1970 to 2300
    year: number;
    /** If "end" is true, the time will be calculated as "start of this day/month/hour" - 1ms */
    end?: boolean;
    /** Time zone (default is 1 - germany)  */
    timeZone?: number;
};

export interface DataEntry {
    ts: number;

    [valueName: string]: number;
}

export interface IobDataEntry {
    val: number | null;
    ts: number;
    time?: string;
    id?: string;
    ack?: boolean;
    from?: string;
    q?: number;
    /** Interpolated value */
    i?: boolean;
}

interface GetHistoryOptions {
    instance?: string;

    /** Start time in ms */
    start?: number;
    /** End time in ms. If not defined, it is "now" */
    end?: number;
    /** Step in ms of intervals. Used in aggregate (max, min, average, total, ...)  */
    step?: number;

    /** Start of smart intervals. It calculates the statistics according to real month length */
    smartStart?: SmartDate;
    /** Type of smart intervals  */
    smartType?: 'hour' | 'day' | 'month';
    /** End of smart intervals. It calculates the statistics according to real month length. If not defined, the end is "now" */
    smartEnd?: SmartDate;

    /** number of values if aggregate is 'onchange' or number of intervals if other aggregate method. Count will be ignored if step is set, else default is 500 if not set */
    count?: number;
    /** if `from` field should be included in answer */
    from?: boolean;
    /** if `ack` field should be included in answer */
    ack?: boolean;
    /** if `q` field should be included in answer */
    q?: boolean;
    /** if `id` field should be included in answer */
    addId?: boolean;
    /** do not return more entries than limit */
    limit?: number;
    /** round result to number of digits after decimal point */
    round?: number;
    /** if null values should be included (false), replaced by last not null value (true) or replaced with 0 (0) */
    ignoreNull?: boolean | 0;
    /** This number will be returned in answer, so the client can assign the request for it */
    sessionId?: number;
    /** aggregate method (Default: 'average') */
    aggregate?:
        | 'onchange'
        | 'minmax'
        | 'min'
        | 'max'
        | 'average'
        | 'total'
        | 'count'
        | 'none'
        | 'percentile'
        | 'quantile'
        | 'integral'
        | 'integralTotal';
    /** Returned data is normally sorted ascending by date, this option lets you return the newest instead of the oldest values if the number of returned points is limited */
    returnNewestEntries?: boolean;
    /** By default, the additional border values are returned to optimize charting. Set this option to true if this is not wanted (e.g. for script data processing) */
    removeBorderValues?: boolean;
    /** when using aggregate method `percentile` defines the percentile level (0..100)(defaults to 50) */
    percentile?: number;
    /** when using aggregate method `quantile` defines the quantile level (0..1)(defaults to 0.5) */
    quantile?: number;
    /** when using aggregate method `integral` defines the unit in seconds (defaults to 60s). e.g. to get integral in hours for Wh or such, set to 3600. */
    integralUnit?: number;
    /** when using aggregate method `integral` defines the interpolation method (defaults to `none`). */
    integralInterpolation?: 'none' | 'linear';
    /** This means, that the data was pre-aggregated when stored (e.g. by influxdb) */
    preAggregated?: boolean;
}

export interface TimeInterval {
    start: number;
    end: number;
    startS?: string;
    endS?: string;
}

export interface GetStatistics {
    timeType: 'hour' | 'day' | 'month';
    startDate: SmartDate;
    endDate?: SmartDate;
    // Object ID
    id?: string;
    // Winter TimeZone in hours. For Germany, it is +1;
    timeZone?: number;
}

export interface GetHistoryOptionsExtended extends GetHistoryOptions {
    uuid?: string;
    id?: string;
    path?: string;

    time?: boolean;
    humanTime?: boolean;

    endS?: string;
    startS?: string;

    pretty?: boolean;
    preAggregated?: boolean;

    logDebug?: boolean;
}

export interface InternalHistoryOptions extends GetHistoryOptions {
    id?: string;
    logDebug?: boolean;
    log?: typeof console.log;
    processing?: {
        val: { ts: number | null; val: number | null };
        max: { ts: number | null; val: number | null };
        min: { ts: number | null; val: number | null };
        start: { ts: number | null; val: number | null };
        end: { ts: number | null; val: number | null };
    }[];

    result?: IobDataEntry[];

    overallLength?: number;
    maxIndex?: number;
    averageCount?: number[];
    quantileDataPoints?: number[][];
    integralDataPoints?: IobDataEntry[][];
    totalIntegralDataPoints?: IobDataEntry[];

    timeIntervals?: TimeInterval[];
    currentTimeInterval?: number;
}
