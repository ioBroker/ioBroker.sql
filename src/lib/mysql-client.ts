import { ConnectionFactory, type SQLConnection } from './connection-factory';
import SQLClient from './sql-client';
import { SQLClientPool, type PoolConfig } from './sql-client-pool';

import type { SqlParam } from '../types';
import { importDriver, type MySQLConnection, type MySQLModule, type MySQLOptions } from './optional-drivers';

export type { MySQLOptions };

/**
 * Refuse a connection whose session runs with `NO_BACKSLASH_ESCAPES`.
 *
 * mysql2 escapes the bound values on the client, with backslashes (`'` becomes `\'`). With
 * `NO_BACKSLASH_ESCAPES` the server reads a backslash as a plain character, so a value with `'` or
 * `\` would end the string literal early: the statement fails, and a crafted value would be SQL
 * again. The mode is not a default, but it can be set for the server, the user or the database.
 * Removing it per session would silently override what the admin configured, so the connection is
 * refused instead, with an error that says why. One query per new connection; the pool reuses them.
 *
 * @param connection the freshly connected mysql2 connection
 * @param callback receives the connection, or the error after the connection was closed
 */
export function checkSqlMode(
    connection: MySQLConnection,
    callback: (err: Error | null, connection?: MySQLConnection) => void,
): void {
    connection.query<{ mode?: string }>('SELECT @@SESSION.sql_mode AS mode', (err, rows) => {
        if (err) {
            connection.end(() => callback(err));
            return;
        }
        const mode = String(rows?.[0]?.mode ?? '');
        if (mode.toUpperCase().split(',').includes('NO_BACKSLASH_ESCAPES')) {
            const error = Object.assign(
                new Error(
                    "The MySQL server uses sql_mode NO_BACKSLASH_ESCAPES, so values with ' or \\ cannot be stored safely. " +
                        'Remove NO_BACKSLASH_ESCAPES from the sql_mode of the server, of the user or of the database.',
                ),
                { code: 'IOB_NO_BACKSLASH_ESCAPES' },
            );
            connection.end(() => callback(error));
            return;
        }
        callback(null, connection);
    });
}

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
        connection.connect((err: Error | null): void => {
            if (err) {
                return callback(err, connection);
            }
            checkSqlMode(connection, callback);
        });
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
