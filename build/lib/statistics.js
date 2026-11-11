"use strict";
/**
 * Pure helpers behind the `getDpStatistics` and `cleanupOrphaned` messages.
 *
 * Kept out of `main.ts` so they can be unit tested: importing `main.ts` pulls in
 * `@iobroker/adapter-core`, which calls `process.exit(10)` at module load time when js-controller
 * cannot be resolved, and js-controller is not a dependency of this repository.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.resolveStatus = resolveStatus;
exports.classifyDatapoint = classifyDatapoint;
exports.estimateBytes = estimateBytes;
exports.summarize = summarize;
exports.selectForCleanup = selectForCleanup;
/**
 * Determine the status of one datapoint of the `datapoints` table.
 *
 * The object database is only asked for names that are neither logged nor configured, so a logged
 * alias never depends on an object with its name. An error of the object database is not caught:
 * read as "object missing" it would make a confirmed cleanup delete the history of every datapoint
 * that is not logged, so the whole request fails instead and nothing is deleted. When in doubt, the
 * data is kept.
 *
 * @param name the name of the datapoint in the `datapoints` table
 * @param sources where logging, custom settings and objects are looked up
 */
async function resolveStatus(name, sources) {
    const loggingEnabled = sources.isLogged(name);
    const exists = loggingEnabled || sources.configured.has(name) || (await sources.objectExists(name));
    return classifyDatapoint(exists, loggingEnabled);
}
/**
 * Decide how a datapoint should be classified.
 *
 * A datapoint this instance logs right now is `active`, whether an object with its ID exists or
 * not: with an alias (`aliasId` in the custom settings) the values are stored under the alias name,
 * which has no object of its own. Checking the object first called every logged alias
 * `objectMissing`, and a confirmed cleanup deleted its history (#587).
 *
 * @param objectExists whether an ioBroker object or a custom setting for this ID still exists
 * @param loggingEnabled whether this instance currently logs the ID
 */
function classifyDatapoint(objectExists, loggingEnabled) {
    if (loggingEnabled) {
        return 'active';
    }
    return objectExists ? 'loggingDisabled' : 'objectMissing';
}
/**
 * Multiply a row count by the average row width the database reported.
 *
 * @param count number of rows belonging to the datapoint
 * @param avgRowLength average bytes per row of the whole table, or null/0 when unknown
 * @returns the estimate, or null when no width is available - never a fabricated number
 */
function estimateBytes(count, avgRowLength) {
    if (!avgRowLength || avgRowLength <= 0 || !Number.isFinite(avgRowLength)) {
        return null;
    }
    return Math.round(count * avgRowLength);
}
/**
 * Totals for the statistics table.
 *
 * `estimatedBytes` is null as soon as one datapoint has no estimate: a partial sum presented as a
 * total would understate the real footprint.
 *
 * @param stats the per-datapoint statistics
 */
function summarize(stats) {
    const byStatus = {
        active: { datapoints: 0, rows: 0 },
        loggingDisabled: { datapoints: 0, rows: 0 },
        objectMissing: { datapoints: 0, rows: 0 },
    };
    let rows = 0;
    let bytes = 0;
    let bytesKnown = true;
    for (const stat of stats) {
        rows += stat.count;
        byStatus[stat.status].datapoints++;
        byStatus[stat.status].rows += stat.count;
        if (stat.estimatedBytes === null) {
            bytesKnown = false;
        }
        else {
            bytes += stat.estimatedBytes;
        }
    }
    return {
        datapoints: stats.length,
        rows,
        estimatedBytes: bytesKnown ? bytes : null,
        byStatus,
    };
}
/**
 * Pick the datapoints a cleanup run with this scope would remove.
 *
 * An empty scope selects `objectMissing` only. Defaulting to the safe half means a caller that
 * forgets to pass a scope deletes the data nobody can reach any more, not data someone may be
 * keeping on purpose. `active` datapoints are never selectable.
 *
 * @param stats the per-datapoint statistics
 * @param scope which statuses to include
 */
function selectForCleanup(stats, scope) {
    const includeMissing = scope?.objectMissing !== false;
    const includeDisabled = scope?.loggingDisabled === true;
    return stats.filter(stat => (stat.status === 'objectMissing' && includeMissing) ||
        (stat.status === 'loggingDisabled' && includeDisabled));
}
//# sourceMappingURL=statistics.js.map