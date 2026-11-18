import type { SqlParam } from '../types';

export type SQLConnection = any;

export abstract class ConnectionFactory {
    /**
     * Whether `execute()` binds `params`. Only the MySQL factory does so far; for the other drivers a
     * statement with values is rejected instead of running it without them.
     */
    readonly supportsParams: boolean = false;

    abstract openConnection(options: any, callback: (err: Error | null, connection?: SQLConnection) => void): void;
    abstract closeConnection(connection: SQLConnection, callback?: (err?: Error | null) => void): void;
    abstract execute<T>(
        connection: SQLConnection,
        sql: string,
        callback: (err: Error | null | undefined, results?: Array<T>) => void,
        params?: SqlParam[],
    ): void;

    /**
     * Read the rows of a query in batches instead of buffering the whole result in RAM.
     *
     * `streamed` in the callback says whether streaming took place. `false` means this driver cannot
     * stream (or the connection could not be used for it), and the caller has to read the buffered
     * way with `execute()`. `onRows` is only called while streaming really takes place, so a caller
     * can start any per-request work lazily on the first batch.
     *
     * The default reports "not supported"; only a driver that overrides it streams.
     *
     * @param _connection the open connection
     * @param _sql plain SQL without bound values
     * @param _batchSize maximum number of rows per `onRows` call
     * @param _onRows receives the rows batch by batch, in the order of the query
     * @param callback called once at the end
     */
    executeStreamed<T>(
        _connection: SQLConnection,
        _sql: string,
        _batchSize: number,
        _onRows: (rows: Array<T>) => void,
        callback: (err: Error | null | undefined, streamed: boolean) => void,
    ): void {
        callback(null, false);
    }
}
