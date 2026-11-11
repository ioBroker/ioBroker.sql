/**
 * Pure helpers behind the `getDpStatistics` and `cleanupOrphaned` messages.
 *
 * Kept out of `main.ts` so they can be unit tested: importing `main.ts` pulls in
 * `@iobroker/adapter-core`, which calls `process.exit(10)` at module load time when js-controller
 * cannot be resolved, and js-controller is not a dependency of this repository.
 */

export type TableName = 'ts_number' | 'ts_string' | 'ts_bool' | 'ts_counter';

/**
 * Why a datapoint is or is not a candidate for cleanup.
 *
 * `objectMissing` and `loggingDisabled` are deliberately kept apart. A state that no longer exists
 * in ioBroker cannot produce new values and nobody can chart it, so removing its history is safe.
 * A state that still exists but has logging switched off is a different matter: the history is
 * still reachable and may well be wanted, which is why cleanup must not treat the two as one.
 */
export type DatapointStatus = 'active' | 'loggingDisabled' | 'objectMissing';

export type DatapointStat = {
    /** The state ID as stored in the `datapoints` table */
    id: string;
    /** The integer key used by the `ts_*` tables */
    index: number;
    /** Storage type, or null when the `datapoints` row carries none */
    type: 'Number' | 'String' | 'Boolean' | null;
    /** Which time series table holds the values */
    table: TableName | null;
    /** Exact number of stored values */
    count: number;
    /** Timestamp of the oldest value, or null when there is none */
    firstTs: number | null;
    /** Timestamp of the newest value, or null when there is none */
    lastTs: number | null;
    /** count * average row width, or null when the database could not report a width */
    estimatedBytes: number | null;
    status: DatapointStatus;
};

/** Where the status of a datapoint comes from (see `resolveStatus`) */
export interface StatusSources {
    /** whether this instance logs the name right now (`sqlDPs` is keyed by the logging name) */
    isLogged: (name: string) => boolean;
    /** names with custom settings of this instance: aliasId or ID, logging on or off */
    configured: ReadonlySet<string>;
    /** whether an object with this ID exists; rejects on an error of the object database */
    objectExists: (name: string) => Promise<boolean>;
}

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
export async function resolveStatus(name: string, sources: StatusSources): Promise<DatapointStatus> {
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
export function classifyDatapoint(objectExists: boolean, loggingEnabled: boolean): DatapointStatus {
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
export function estimateBytes(count: number, avgRowLength: number | null | undefined): number | null {
    if (!avgRowLength || avgRowLength <= 0 || !Number.isFinite(avgRowLength)) {
        return null;
    }
    return Math.round(count * avgRowLength);
}

export type StatisticsSummary = {
    datapoints: number;
    rows: number;
    /** null when the width of at least one involved table was unknown, so the sum would mislead */
    estimatedBytes: number | null;
    byStatus: Record<DatapointStatus, { datapoints: number; rows: number }>;
};

/**
 * Totals for the statistics table.
 *
 * `estimatedBytes` is null as soon as one datapoint has no estimate: a partial sum presented as a
 * total would understate the real footprint.
 *
 * @param stats the per-datapoint statistics
 */
export function summarize(stats: DatapointStat[]): StatisticsSummary {
    const byStatus: Record<DatapointStatus, { datapoints: number; rows: number }> = {
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
        } else {
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

/** Which statuses a cleanup run should remove */
export type CleanupScope = {
    /** States that no longer exist in ioBroker. Safe, and the default. */
    objectMissing?: boolean;
    /** States that still exist but are not logged. Their history may still be wanted. */
    loggingDisabled?: boolean;
};

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
export function selectForCleanup(stats: DatapointStat[], scope?: CleanupScope): DatapointStat[] {
    const includeMissing = scope?.objectMissing !== false;
    const includeDisabled = scope?.loggingDisabled === true;

    return stats.filter(
        stat =>
            (stat.status === 'objectMissing' && includeMissing) ||
            (stat.status === 'loggingDisabled' && includeDisabled),
    );
}
