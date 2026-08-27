// Only the first few sub-errors of an AggregateError are rendered - a DNS name with many A/AAAA
// records would otherwise produce a log line of arbitrary length.
const MAX_NESTED_ERRORS = 5;
const MAX_DEPTH = 3;

/** Fields drivers put on their errors that are worth showing when the message alone says nothing */
const DETAIL_FIELDS = ['code', 'errno', 'syscall', 'address', 'port', 'sqlMessage'] as const;

/**
 * Render an error as one readable line.
 *
 * `String(err)` is not good enough for connection errors: Node's happy-eyeballs connect (and with it
 * every driver that opens a TCP socket) rejects with an `AggregateError` whose own `message` is empty,
 * so `toString()` degrades to a bare "AggregateError" and the actual reason - "connect ECONNREFUSED
 * 127.0.0.1:3306" for each address that was tried - stays hidden in `err.errors`. Unwrap that array,
 * unwrap `err.cause`, and append the driver's error code when the message does not contain it already.
 *
 * @param err whatever was caught or handed to a callback
 * @param depth recursion depth, used internally for nested errors
 * @returns a non-empty, single-line description
 */
export function formatError(err: unknown, depth = 0): string {
    if (err === null || err === undefined) {
        return 'Unknown error';
    }
    if (typeof err === 'string') {
        return err || 'Unknown error';
    }
    if (typeof err === 'number' || typeof err === 'boolean' || typeof err === 'bigint') {
        return String(err);
    }
    if (typeof err !== 'object') {
        // symbol or function - nothing sensible to print
        return Object.prototype.toString.call(err);
    }

    const error = err as Record<string, any>;
    const message = typeof error.message === 'string' ? oneLine(error.message) : '';
    const name = typeof error.name === 'string' && error.name ? error.name : 'Error';
    let text = message ? `${name}: ${message}` : name;

    // AggregateError: the reason is in `errors`, not in `message`
    if (Array.isArray(error.errors) && error.errors.length && depth < MAX_DEPTH) {
        const details: string[] = [];
        for (const nested of error.errors.slice(0, MAX_NESTED_ERRORS)) {
            const nestedText = describeNested(nested, depth + 1);
            // both stacks of a happy-eyeballs connect fail the same way more often than not
            if (nestedText && !details.includes(nestedText)) {
                details.push(nestedText);
            }
        }
        if (error.errors.length > MAX_NESTED_ERRORS) {
            details.push(`and ${error.errors.length - MAX_NESTED_ERRORS} more`);
        }
        if (details.length) {
            text += `: ${details.join('; ')}`;
        }
    }

    for (const field of DETAIL_FIELDS) {
        const value = error[field];
        if ((typeof value === 'string' && value) || typeof value === 'number') {
            if (!text.includes(String(value))) {
                text += ` (${field}: ${value})`;
            }
        }
    }

    if (error.cause !== undefined && error.cause !== null && depth < MAX_DEPTH) {
        const cause = formatError(error.cause, depth + 1);
        if (cause && !text.includes(cause)) {
            text += `; caused by ${cause}`;
        }
    }

    if (text === 'Error') {
        // a plain object without message/name - show what it actually holds
        try {
            const json = JSON.stringify(error);
            if (json && json !== '{}') {
                return json;
            }
        } catch {
            // circular or non-serializable - fall through
        }
        return Object.prototype.toString.call(error);
    }

    return text;
}

/**
 * Squeeze all whitespace into single spaces, so a multi-line driver message stays one log line
 *
 * @param text the text to normalize
 * @returns the text without line breaks
 */
function oneLine(text: string): string {
    return text.replace(/\s+/g, ' ').trim();
}

/**
 * Describe one sub-error of an AggregateError: the message alone if there is one, because it usually
 * already carries the code ("connect ECONNREFUSED 127.0.0.1:3306").
 *
 * @param err the nested error
 * @param depth current recursion depth
 * @returns a short description of the nested error
 */
function describeNested(err: unknown, depth: number): string {
    if (err && typeof err === 'object') {
        const nested = err as Record<string, any>;
        const message = typeof nested.message === 'string' ? oneLine(nested.message) : '';
        if (message) {
            const code = typeof nested.code === 'string' ? nested.code : '';
            return code && !message.includes(code) ? `${message} (${code})` : message;
        }
    }
    return formatError(err, depth);
}
