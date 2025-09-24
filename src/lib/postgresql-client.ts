import { ConnectionFactory } from './connection-factory';
import SQLClient from './sql-client';
import { SQLClientPool, type PoolConfig } from './sql-client-pool';

import type { Client, QueryResult, ClientConfig as PostgreSQLOptions } from 'pg';

export type { PostgreSQLOptions };

// PostgreSQLConnectionFactory does not use any of node-pg's built-in pooling.
class PostgreSQLConnectionFactory extends ConnectionFactory {
    private Client: typeof Client | undefined;

    openConnection(connectString: PostgreSQLOptions, callback: (err: Error | null, connection: Client) => void): void {
        if (!this.Client) {
            void import('pg').then(pg => {
                this.Client = pg.default.native?.Client || pg.default.Client;
                this.openConnection(connectString, callback);
            });
            return;
        }
        const connection = new this.Client(connectString);
        connection.connect(err => callback(err, connection));
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
            return callback(null, results.rows);
        });
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
