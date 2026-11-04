import { ConnectionFactory, type SQLConnection } from './connection-factory';
import SQLClient from './sql-client';
import { SQLClientPool, type PoolConfig } from './sql-client-pool';

import type { SqlParam } from '../types';
import { importDriver, type MySQLConnection, type MySQLModule, type MySQLOptions } from './optional-drivers';

export type { MySQLOptions };

/** Called with the server's `sql_mode` when `NO_BACKSLASH_ESCAPES` was removed from a session */
export type SqlModeAdjusted = (serverMode: string) => void;

/**
 * The `sql_mode` without `NO_BACKSLASH_ESCAPES`, or `null` if it does not contain the mode.
 *
 * @param mode the comma-separated `sql_mode` of a session
 */
export function withoutNoBackslashEscapes(mode: string): string | null {
    const modes = mode.split(',').filter(m => m.trim());
    const kept = modes.filter(m => m.trim().toUpperCase() !== 'NO_BACKSLASH_ESCAPES');
    return kept.length === modes.length ? null : kept.join(',');
}

/**
 * Make sure the session of a new connection does not run with `NO_BACKSLASH_ESCAPES`.
 *
 * mysql2 escapes the bound values on the client, with backslashes (`'` becomes `\'`). With
 * `NO_BACKSLASH_ESCAPES` the server reads a backslash as a plain character, so a value with `'` or
 * `\` would end the string literal early: the statement fails, and a crafted value would be SQL
 * again. The mode is not a default, but it can be set globally (server option, `SET GLOBAL`) or by
 * `init_connect`; MySQL has no `sql_mode` per user or per database.
 *
 * The mode is removed for this session only, which changes neither the server's global setting nor
 * other clients. Refusing the connection instead would stop all logging after an update on a server
 * where the adapter used to work, and on a hosted database the user usually cannot change the global
 * mode. Only if the mode cannot be removed is the connection closed and refused.
 * One query per new connection, two when the mode is set; the pool reuses the connections.
 *
 * @param connection the freshly connected mysql2 connection
 * @param callback receives the connection, or the error after the connection was closed
 * @param onAdjusted called when the mode was removed, to log it
 */
export function checkSqlMode(
    connection: MySQLConnection,
    callback: (err: Error | null, connection?: MySQLConnection) => void,
    onAdjusted?: SqlModeAdjusted,
): void {
    connection.query<{ mode?: string }>('SELECT @@SESSION.sql_mode AS mode', (err, rows) => {
        if (err) {
            connection.end(() => callback(err));
            return;
        }
        const mode = String(rows?.[0]?.mode ?? '');
        const adjusted = withoutNoBackslashEscapes(mode);
        if (adjusted === null) {
            callback(null, connection);
            return;
        }
        // Bound like any value. Mode names contain no quotes or backslashes, so this statement is
        // safe although the mode is still active while it runs.
        connection.query('SET SESSION sql_mode = ?', [adjusted], err => {
            if (err) {
                const error = Object.assign(
                    new Error(
                        `The MySQL server uses sql_mode NO_BACKSLASH_ESCAPES, so values with ' or \\ cannot be stored safely, and it could not be removed for the session: ${err.message}. ` +
                            'Remove NO_BACKSLASH_ESCAPES from the global sql_mode of the server.',
                    ),
                    { code: 'IOB_NO_BACKSLASH_ESCAPES', cause: err },
                );
                connection.end(() => callback(error));
                return;
            }
            onAdjusted?.(mode);
            callback(null, connection);
        });
    });
}

class MySQL2ConnectionFactory extends ConnectionFactory {
    constructor(private readonly onSqlModeAdjusted?: SqlModeAdjusted) {
        super();
    }

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
            checkSqlMode(connection, callback, this.onSqlModeAdjusted);
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
    constructor(sqlConnection: MySQLOptions, onSqlModeAdjusted?: SqlModeAdjusted) {
        super(sqlConnection, new MySQL2ConnectionFactory(onSqlModeAdjusted));
    }
}

export class MySQL2ClientPool extends SQLClientPool {
    constructor(poolOptions: PoolConfig, sqlOptions: MySQLOptions, onSqlModeAdjusted?: SqlModeAdjusted) {
        super(poolOptions, sqlOptions, new MySQL2ConnectionFactory(onSqlModeAdjusted));
    }
}
