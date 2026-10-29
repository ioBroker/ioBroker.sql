import { ConnectionFactory, type SQLConnection } from './connection-factory';
import SQLClient from './sql-client';
import { SQLClientPool, type PoolConfig } from './sql-client-pool';

import type { SqlParam } from '../types';
import { importDriver, type MySQLConnection, type MySQLModule, type MySQLOptions } from './optional-drivers';

export type { MySQLOptions };

class MySQL2ConnectionFactory extends ConnectionFactory {
    private createConnection: any;
    openConnection(options: MySQLOptions, callback: (err: Error | null, connection?: MySQLConnection) => void): void {
        if (!this.createConnection) {
            void importDriver<MySQLModule>('mysql2').then(
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

    closeConnection(connection: MySQLConnection | null | undefined, callback: (error?: Error | null) => void): void {
        if (connection) {
            connection.end(callback);
        } else {
            callback?.(null);
        }
    }

    override readonly supportsParams = true;

    execute<T>(
        connection: SQLConnection,
        sql: string,
        callback: (err: Error | null | undefined, results?: Array<T>) => void,
        params?: SqlParam[],
    ): void {
        // query(), NOT execute(): mysql2's execute() prepares a server-side statement and caches it per
        // connection, keyed by the SQL text. An INSERT of a batch has as many placeholders as values, so
        // nearly every batch would be a distinct text with its own prepared statement, and MySQL fails
        // with "Can't create more than max_prepared_stmt_count statements" (default 16382) after a while.
        // query() with values lets mysql2 escape them on the client (quotes, backslashes, NUL) and send
        // one plain statement.
        const done = (err: Error | null | undefined, results: Array<T>): void => {
            if (err) {
                return callback(err);
            }
            return callback(null, results);
        };
        if (params?.length) {
            connection.query(sql, params, done);
        } else {
            connection.query(sql, done);
        }
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
