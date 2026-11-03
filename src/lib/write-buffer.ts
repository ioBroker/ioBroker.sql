import type { TableName } from '../types';

/** Rows per INSERT: every dialect chunks its statements at this size */
export const ROWS_PER_STATEMENT = 500;

/**
 * Rows of one datapoint that are kept for a retry after the database was not reachable.
 *
 * At one value every 10 s this covers more than 5 hours of outage; the oldest rows are dropped first.
 * A cap per datapoint keeps the memory bounded: a row is about 200 bytes, so 1000 datapoints cost
 * at most about 400 MB in the worst case.
 */
export const MAX_REQUEUED_ROWS = 2000;

/**
 * Split a batch into groups of one table with at most {@link ROWS_PER_STATEMENT} rows.
 *
 * Each group becomes exactly one INSERT, so when a statement fails, it is known which rows were
 * written and which were not.
 *
 * @param list the rows of one datapoint, in the order they were logged
 * @param size rows per group
 */
export function splitBatch<T extends { table: TableName }>(list: T[], size = ROWS_PER_STATEMENT): T[][] {
    const byTable = new Map<TableName, T[]>();
    for (const row of list) {
        let rows = byTable.get(row.table);
        if (!rows) {
            rows = [];
            byTable.set(row.table, rows);
        }
        rows.push(row);
    }
    const groups: T[][] = [];
    for (const rows of byTable.values()) {
        for (let i = 0; i < rows.length; i += size) {
            groups.push(rows.slice(i, i + size));
        }
    }
    return groups;
}

/**
 * Put rows that could not be written back in front of the rows logged since.
 *
 * Only re-queued rows are dropped to stay within the cap, the oldest first: the rows logged in the
 * meantime are buffered as usual.
 *
 * @param current the rows that were buffered in the meantime
 * @param unwritten the rows of the failed write, older than `current`
 * @param cap the maximum number of rows to keep
 * @returns the new buffer and the number of dropped rows
 */
export function requeue<T>(
    current: T[] | undefined,
    unwritten: T[],
    cap = MAX_REQUEUED_ROWS,
): { list: T[]; dropped: number } {
    const rest = current || [];
    const dropped = Math.min(unwritten.length, Math.max(0, unwritten.length + rest.length - cap));
    return { list: [...unwritten.slice(dropped), ...rest], dropped };
}
