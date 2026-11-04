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
}
