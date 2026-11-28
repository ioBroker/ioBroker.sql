import { ConnectionFactory } from './connection-factory';
import SQLClient from './sql-client';
import { SQLClientPool, type PoolConfig } from './sql-client-pool';

import type { Client, QueryResult, ClientConfig as PostgreSQLOptions } from 'pg';
import type Cursor from 'pg-cursor';

export type { PostgreSQLOptions };

// PostgreSQLConnectionFactory does not use any of node-pg's built-in pooling.
class PostgreSQLConnectionFactory extends ConnectionFactory {
    private Client: typeof Client | undefined;
    /** pg-native cannot run a pg-cursor, so a native client never streams */
    private isNative = false;
    /** pg-cursor, imported on first use. It is an optional dependency like pg itself. */
    private Cursor: typeof Cursor | undefined;
    /** pg-cursor could not be loaded: do not try the import again for every read */
    private cursorUnavailable = false;

    openConnection(connectString: PostgreSQLOptions, callback: (err: Error | null, connection?: Client) => void): void {
        if (!this.Client) {
            void import('pg').then(
                pg => {
                    const nativeClient = pg.default.native?.Client;
                    this.Client = nativeClient || pg.default.Client;
                    this.isNative = !!nativeClient;
                    this.openConnection(connectString, callback);
                },
                // pg is an optional dependency, so report a missing driver instead of
                // letting the rejection escape as an unhandled promise rejection
                e => callback(new Error(`Node.js DB driver "pg" could not be loaded: ${e}`)),
            );
            return;
        }
        const connection = new this.Client(connectString);
        connection.connect((err: Error) => callback(err, connection));
    }

    closeConnection(connection: Client | null | undefined, callback: (error?: Error | null) => void): void {
        if (connection) {
            connection.end(callback);
        } else {
            callback?.(null);
        }
    }

    execute<T>(
        connection: Client,
        sql: string,
        callback: (err: Error | null | undefined, results?: Array<T>) => void,
    ): void {
        connection.query(sql, (err: Error, results: QueryResult): void => {
            if (err) {
                return callback(err);
            }
            return callback(null, results?.rows);
        });
    }

    executeStreamed<T>(
        connection: Client,
        sql: string,
        batchSize: number,
        onRows: (rows: Array<T>) => void,
        callback: (err: Error | null | undefined, streamed: boolean) => void,
    ): void {
        if (this.isNative || this.cursorUnavailable) {
            callback(null, false);
            return;
        }
        if (!this.Cursor) {
            void import('pg-cursor').then(
                mod => {
                    this.Cursor = mod.default;
                    this.executeStreamed(connection, sql, batchSize, onRows, callback);
                },
                // optional dependency missing: remember it and let the caller read the buffered way
                () => {
                    this.cursorUnavailable = true;
                    callback(null, false);
                },
            );
            return;
        }

        const cursor = connection.query(new this.Cursor<T>(sql));

        let finished = false;
        const finish = (err: Error | null | undefined): void => {
            if (!finished) {
                finished = true;
                callback(err, true);
            }
        };

        const read = (): void => {
            cursor.read(batchSize, (err: Error | undefined, rows: T[]): void => {
                if (err) {
                    // Do not wait for cursor.close() here. After an error pg-cursor has already sent the
                    // `sync` itself, and close() waits for a `readyForQuery`. When the connection is gone
                    // (backend terminated, socket reset) that never comes: the callback, and with it the
                    // return of the client to the pool, would never be called. A dead connection is
                    // flagged by its `error` event and dropped by the pool on the next borrow.
                    finish(err);
                } else if (!rows.length) {
                    // Every row was delivered. An error while closing the cursor does not change the
                    // result; a broken connection is noticed by the pool on the next borrow.
                    cursor.close(() => finish(null));
                } else {
                    onRows(rows);
                    read();
                }
            });
        };
        read();
    }
}

export class PostgreSQLClient extends SQLClient {
    constructor(connectString: PostgreSQLOptions) {
        super(connectString, new PostgreSQLConnectionFactory());
    }
}

export class PostgreSQLClientPool extends SQLClientPool {
    constructor(poolOptions: PoolConfig, connectString: PostgreSQLOptions) {
        super(poolOptions, connectString, new PostgreSQLConnectionFactory());
    }
}
