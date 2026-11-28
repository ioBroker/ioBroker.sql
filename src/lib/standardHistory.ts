/**
 * The commands `aggregate` and `lastChange` of the shared catalog of sendTo commands (family "history", see HELP.md
 * of ioBroker.assistant): one number for a period, and the time of the last change of a state. Both are computed
 * from the stored values as the adapter reads them anyway (for `getHistory` or `getRawEntries`), so they need no
 * database query of their own.
 *
 * The same file is used by ioBroker.history, ioBroker.sql, ioBroker.influxdb and ioBroker.recorder: keep it identical.
 */

/** One stored value */
export interface StoredValue {
    ts: number;
    val: unknown;
}

/** Where the values come from */
export interface ValueSource {
    /**
     * The stored values of a state, not aggregated: with `start` the oldest `count` values in [start, end], without
     * it the newest `count` values up to `end`. Values outside the range and nulls (gaps) are ignored.
     */
    read(id: string, query: { start?: number; end: number; count: number }): Promise<StoredValue[]>;
    /** The unit of the state (common.unit), if it has one */
    unit?(id: string): Promise<string | undefined>;
}

/** An answer of the catalog: the result, or `{error, reason}` */
export type StandardAnswer = Record<string, unknown>;

/** The values of `fn` of `aggregate` */
export const AGGREGATE_FUNCTIONS = [
    'min',
    'max',
    'avg',
    'sum',
    'count',
    'delta',
    'first',
    'last',
    'integral',
    'duration',
];

/** `aggregate` reads at most so many values: a longer period gives the error TOO_MANY_VALUES */
export const MAX_VALUES = 50_000;
/** `lastChange` reads the values backwards in pages of this size ... */
const PAGE = 1_000;
/** ... and stops after so many values */
export const MAX_SCAN = 10_000;
const HOUR = 3_600_000;

function fail(error: string, reason: string): StandardAnswer {
    return { error, reason };
}

/** ms since 1970 from a number, a numeric text or a date text (a text without a zone is local time) */
function toTimestamp(value: unknown): number | undefined {
    if (typeof value === 'number') {
        return Number.isFinite(value) ? value : undefined;
    }
    if (typeof value !== 'string' || !value.trim()) {
        return undefined;
    }
    const text = value.trim();
    if (/^\d+$/.test(text)) {
        return parseInt(text, 10);
    }
    // Date reads a date without a time as UTC, the catalog means the local midnight
    const ts = new Date(/^\d{4}-\d{2}-\d{2}$/.test(text) ? `${text}T00:00` : text).getTime();
    return Number.isFinite(ts) ? ts : undefined;
}

function isGiven(value: unknown): boolean {
    return value !== undefined && value !== null && value !== '';
}

/** A value as number (true = 1, false = 0, a numeric text), null if it is none */
function toNumber(val: unknown): number | null {
    if (typeof val === 'boolean') {
        return val ? 1 : 0;
    }
    const n = typeof val === 'number' ? val : typeof val === 'string' && val.trim() ? Number(val) : NaN;
    return Number.isFinite(n) ? n : null;
}

/** A value to compare: true, 1 and "true" are the same, as are 5 and "5"; texts without case */
function comparable(val: unknown): unknown {
    if (typeof val === 'boolean') {
        return val ? 1 : 0;
    }
    if (typeof val === 'string') {
        const text = val.trim().toLowerCase();
        if (text === 'true' || text === 'false') {
            return text === 'true' ? 1 : 0;
        }
        return text && Number.isFinite(Number(text)) ? Number(text) : text;
    }
    return val;
}

function same(a: unknown, b: unknown): boolean {
    return comparable(a) === comparable(b);
}

function round(value: number): number {
    return Math.round(value * 1000) / 1000;
}

/** The values in [start, end] without the gaps (null), sorted by ts */
function inRange(values: StoredValue[], start: number, end: number): StoredValue[] {
    return values
        .filter(v => v && v.ts >= start && v.ts <= end && v.val !== null && v.val !== undefined)
        .sort((a, b) => a.ts - b.ts);
}

function errorText(e: unknown): string {
    return e instanceof Error ? e.message : String(e);
}

/** The value of one function; `before` = the last value before start, `end` = the end, but not after now */
function compute(
    fn: string,
    values: StoredValue[],
    before: StoredValue | undefined,
    wanted: unknown,
    start: number,
    end: number,
): { value: unknown; ts?: number } {
    const numbers = values
        .map(v => ({ ts: v.ts, n: toNumber(v.val) }))
        .filter((v): v is { ts: number; n: number } => v.n !== null);
    const sum = numbers.reduce((total, v) => total + v.n, 0);
    switch (fn) {
        case 'first':
        case 'last': {
            const v = fn === 'first' ? values[0] : values[values.length - 1];
            return v ? { value: v.val, ts: v.ts } : { value: null };
        }
        case 'min':
        case 'max': {
            let best: { ts: number; n: number } | undefined;
            for (const v of numbers) {
                if (!best || (fn === 'min' ? v.n < best.n : v.n > best.n)) {
                    best = v;
                }
            }
            return best ? { value: best.n, ts: best.ts } : { value: null };
        }
        case 'avg':
            return { value: numbers.length ? round(sum / numbers.length) : null };
        case 'sum':
            return { value: round(sum) };
        case 'count': {
            if (!isGiven(wanted)) {
                return { value: values.length };
            }
            // how often the state became the value: the values before it do not count again
            let count = 0;
            let previous = before?.val;
            for (const v of values) {
                if (same(v.val, wanted) && (previous === undefined || !same(previous, wanted))) {
                    count++;
                }
                previous = v.val;
            }
            return { value: count };
        }
        case 'delta': {
            // how much a counter rose: a value below the one before is a reset, it rose from 0 to this value
            const series = (before ? [before, ...values] : values)
                .map(v => toNumber(v.val))
                .filter((n): n is number => n !== null);
            let rise = 0;
            for (let i = 1; i < series.length; i++) {
                rise += series[i] >= series[i - 1] ? series[i] - series[i - 1] : series[i];
            }
            return { value: series.length ? round(rise) : null };
        }
        default: {
            // integral and duration: every value holds until the next one, the last one until the end
            const line = before ? [{ ts: start, val: before.val }, ...values] : values;
            let total = 0;
            for (let i = 0; i < line.length; i++) {
                const from = Math.max(line[i].ts, start);
                const to = Math.min(i + 1 < line.length ? line[i + 1].ts : end, end);
                if (to > from) {
                    if (fn === 'duration') {
                        total += same(line[i].val, wanted) ? to - from : 0;
                    } else {
                        total += (toNumber(line[i].val) ?? 0) * (to - from);
                    }
                }
            }
            if (!line.length) {
                return { value: null };
            }
            return { value: fn === 'duration' ? Math.round(total / 1000) : round(total / HOUR) };
        }
    }
}

/**
 * `aggregate {id, start, end?, fn, value?}`: one number for a period, `{value, ts?, unit?, count}`.
 * fn: min, max (ts = when), avg, sum, count (with value: how often the state became it), delta (how much a counter
 * rose, resets included), first, last (ts = when), integral (value × hours, e.g. W → Wh), duration (with value:
 * seconds the state had it). count = the number of stored values in the period.
 *
 * @param message the message of sendTo
 * @param source the stored values
 * @param now the current time
 */
export async function aggregate(
    message: unknown,
    source: ValueSource,
    now: number = Date.now(),
): Promise<StandardAnswer> {
    const m = (message && typeof message === 'object' ? message : {}) as Record<string, unknown>;
    const id = m.id;
    const fn = m.fn;
    if (typeof id !== 'string' || !id) {
        return fail('id is missing', 'BAD_REQUEST');
    }
    if (typeof fn !== 'string' || !AGGREGATE_FUNCTIONS.includes(fn)) {
        return fail(`fn must be one of ${AGGREGATE_FUNCTIONS.join(', ')}`, 'BAD_REQUEST');
    }
    const start = toTimestamp(m.start);
    const end = isGiven(m.end) ? toTimestamp(m.end) : now;
    if (start === undefined || end === undefined || start > end) {
        return fail('start and end must be times (ms or a date text) and start not after end', 'BAD_REQUEST');
    }
    if (fn === 'duration' && !isGiven(m.value)) {
        return fail('fn duration needs the value', 'BAD_REQUEST');
    }

    try {
        const read = await source.read(id, { start, end, count: MAX_VALUES + 1 });
        if (read.length > MAX_VALUES) {
            return fail(
                `More than ${MAX_VALUES} stored values in this period: ask for a shorter one`,
                'TOO_MANY_VALUES',
            );
        }
        const values = inRange(read, start, end);
        // what the state was at start, for the changes, the counter and the time line
        let before: StoredValue | undefined;
        if ((fn === 'count' && isGiven(m.value)) || fn === 'delta' || fn === 'integral' || fn === 'duration') {
            before = inRange(await source.read(id, { end: start - 1, count: 10 }), -Infinity, start - 1).pop();
        }
        const result = compute(fn, values, before, m.value, start, Math.min(end, now));
        const answer: StandardAnswer = { value: result.value, count: values.length };
        if (result.ts !== undefined) {
            answer.ts = result.ts;
        }
        const unit = fn === 'duration' ? 's' : fn === 'count' ? undefined : await source.unit?.(id);
        if (unit) {
            answer.unit = fn === 'integral' ? `${unit}h` : unit;
        }
        return answer;
    } catch (e) {
        return { error: errorText(e) };
    }
}

/**
 * `lastChange {id, value?, before?}`: when the state last changed (to value), `{ts, val}`: the first of the newest
 * stored values that are the same as the newest one (or as value). before: only the values before this time.
 * At most MAX_SCAN values are read: if the state had the value longer, ts is the oldest one read and limited true.
 *
 * @param message the message of sendTo
 * @param source the stored values
 * @param now the current time
 */
export async function lastChange(
    message: unknown,
    source: ValueSource,
    now: number = Date.now(),
): Promise<StandardAnswer> {
    const m = (message && typeof message === 'object' ? message : {}) as Record<string, unknown>;
    const id = m.id;
    if (typeof id !== 'string' || !id) {
        return fail('id is missing', 'BAD_REQUEST');
    }
    let end = isGiven(m.before) ? toTimestamp(m.before) : now;
    if (end === undefined) {
        return fail('before must be a time (ms or a date text)', 'BAD_REQUEST');
    }
    let wanted = m.value;
    // the oldest value read of the newest run of values that are the same as the wanted one
    let run: StoredValue | undefined;
    try {
        for (let scanned = 0; scanned < MAX_SCAN;) {
            const page = await source.read(id, { end, count: PAGE });
            const values = inRange(page, -Infinity, end);
            for (let i = values.length - 1; i >= 0; i--) {
                if (!isGiven(wanted)) {
                    wanted = values[i].val;
                }
                if (same(values[i].val, wanted)) {
                    run = values[i];
                } else if (run) {
                    return { ts: run.ts, val: run.val };
                }
            }
            if (page.length < PAGE) {
                // no older values: the state has had it since the first stored value
                return run ? { ts: run.ts, val: run.val } : fail(`${id} has no such stored value`, 'NOT_FOUND');
            }
            scanned += page.length;
            end = Math.min(...page.map(v => v.ts)) - 1;
        }
    } catch (e) {
        return { error: errorText(e) };
    }
    return run
        ? { ts: run.ts, val: run.val, limited: true }
        : fail(`${id} has no such value in its newest ${MAX_SCAN} stored values`, 'NOT_FOUND');
}

/**
 * The `read` of a `ValueSource` through the adapter's own `getHistory` (aggregate none, without border values).
 *
 * @param ask sends `getHistory` with this message to the own instance and resolves with the answer
 */
export function readThroughGetHistory(
    ask: (message: { id: string; options: Record<string, unknown> }) => Promise<unknown>,
): ValueSource['read'] {
    return async (id, query) => {
        const answer = (await ask({
            id,
            options: {
                start: query.start,
                end: query.end,
                count: query.count,
                limit: query.count,
                aggregate: 'none',
                returnNewestEntries: query.start === undefined,
                removeBorderValues: true,
            },
        })) as { result?: StoredValue[]; error?: unknown } | Error | null | undefined;
        if (answer instanceof Error) {
            throw answer;
        }
        if (answer?.error) {
            throw new Error(typeof answer.error === 'string' ? answer.error : JSON.stringify(answer.error));
        }
        return Array.isArray(answer?.result) ? answer.result : [];
    };
}
