/**
 * Value comparison for the `changesOnly` filter.
 *
 * Kept out of `main.ts` so it can be unit tested: importing `main.ts` pulls in
 * `@iobroker/adapter-core`, which calls `process.exit(10)` at module load time when js-controller
 * cannot be resolved, and js-controller is not a dependency of this repository.
 */

/**
 * Whether a new value is the same as the one that was stored last.
 *
 * `pushHistory()` used to decide this from `state.ts !== state.lc`, i.e. from the controller's own
 * "last change" timestamp. That is right for an ordinary state - js-controller only moves `lc` when
 * the value really changes - but wrong for an alias: an alias has no value of its own, so it carries
 * the `lc` of its source. A source that changes from 12.34 to 12.31 moves `lc`, and the alias
 * arrives with `ts === lc` although its read converter maps both to 12.3. `changesOnly` therefore
 * stored a row for every source change. See https://github.com/ioBroker/ioBroker.sql/issues/295
 *
 * Objects are compared by their JSON because that is how they are stored: `ts_string` holds
 * `JSON.stringify`ed values, so two structurally equal objects are the same row. A strict `!==`
 * would compare references, call every update a change and defeat `changesOnly` for those
 * datapoints - the opposite of the bug being fixed here.
 *
 * @param lastStored the value of the last state that was written, `sqlDPs[id].state.val`
 * @param next the value of the state that just arrived
 */
export function isSameValue(lastStored: unknown, next: unknown): boolean {
    if (lastStored === next) {
        return true;
    }

    // NaN === NaN is false, but two NaN readings are not a change worth storing
    if (typeof lastStored === 'number' && typeof next === 'number') {
        return Number.isNaN(lastStored) && Number.isNaN(next);
    }

    if (lastStored && next && typeof lastStored === 'object' && typeof next === 'object') {
        try {
            return JSON.stringify(lastStored) === JSON.stringify(next);
        } catch {
            // circular structures cannot be stored either, so treat them as different and let the
            // write path report the real problem
            return false;
        }
    }

    // Different types are a change. `'12.3'` and `12.3` are stored differently - string values go
    // into ts_string, numbers into ts_number - so collapsing them here would lose a real transition.
    return false;
}
