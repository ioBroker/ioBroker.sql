import { ConnectionFactory, type SQLConnection } from './connection-factory';
import SQLClient from './sql-client';
import { SQLClientPool, type PoolConfig } from './sql-client-pool';

import type { Connection, ConnectionOptions as MySQLOptions } from 'mysql2';

export type { MySQLOptions };

class MySQL2ConnectionFactory extends ConnectionFactory {
    private createConnection: any;
    openConnection(options: MySQLOptions, callback: (err: Error | null, connection?: Connection) => void): void {
        if (!this.createConnection) {
            void import('mysql2').then(
                mysql2 => {
                    this.createConnection = mysql2.default.createConnection;
                    this.openConnection(options, callback);
                },
                // mysql2 is an optional dependency, so report a missing driver instead of
                // letting the rejection escape as an unhandled promise rejection
                e => callback(new Error(`Node.js DB driver "mysql2" could not be loaded: ${e}`)),
            );
            return;
        }

        const connection = this.createConnection(options);
        connection.connect((err: Error | null): void => callback(err, connection));
    }

    closeConnection(connection: Connection | null | undefined, callback: (error?: Error | null) => void): void {
        if (connection) {
            connection.end(callback);
        } else {
            callback?.(null);
        }
    }

    execute<T>(
        connection: SQLConnection,
        sql: string,
        callback: (err: Error | null | undefined, results?: Array<T>) => void,
    ): void {
        // query(), NOT execute(): mysql2's execute() prepares a server-side statement and caches it per
        // connection, keyed by the SQL text. This adapter inlines all values by string concatenation, so
        // every single INSERT/SELECT is a distinct SQL text and would allocate its own prepared statement -
        // MySQL then fails with "Can't create more than max_prepared_stmt_count statements" (default 16382)
        // after a few thousand logged values. There is nothing to prepare here anyway: no placeholders are
        // ever bound.
        connection.query(sql, (err: Error | null | undefined, results: Array<T>) => {
            if (err) {
                return callback(err);
            }
            return callback(null, results);
        });
    }
}

export class MySQL2Client extends SQLClient {
    constructor(sqlConnection: MySQLOptions) {
        super(sqlConnection, new MySQL2ConnectionFactory());
    }
}

export class MySQL2ClientPool extends SQLClientPool {
    constructor(poolOptions: PoolConfig, sqlOptions: MySQLOptions) {
        super(poolOptions, sqlOptions, new MySQL2ConnectionFactory());
    }
}
