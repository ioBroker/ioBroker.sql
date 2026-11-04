"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.MySQL2ClientPool = exports.MySQL2Client = void 0;
exports.withoutNoBackslashEscapes = withoutNoBackslashEscapes;
exports.checkSqlMode = checkSqlMode;
const connection_factory_1 = require("./connection-factory");
const sql_client_1 = __importDefault(require("./sql-client"));
const sql_client_pool_1 = require("./sql-client-pool");
const optional_drivers_1 = require("./optional-drivers");
/**
 * The `sql_mode` without `NO_BACKSLASH_ESCAPES`, or `null` if it does not contain the mode.
 *
 * @param mode the comma-separated `sql_mode` of a session
 */
function withoutNoBackslashEscapes(mode) {
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
function checkSqlMode(connection, callback, onAdjusted) {
    connection.query('SELECT @@SESSION.sql_mode AS mode', (err, rows) => {
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
                const error = Object.assign(new Error(`The MySQL server uses sql_mode NO_BACKSLASH_ESCAPES, so values with ' or \\ cannot be stored safely, and it could not be removed for the session: ${err.message}. ` +
                    'Remove NO_BACKSLASH_ESCAPES from the global sql_mode of the server.'), { code: 'IOB_NO_BACKSLASH_ESCAPES', cause: err });
                connection.end(() => callback(error));
                return;
            }
            onAdjusted?.(mode);
            callback(null, connection);
        });
    });
}
class MySQL2ConnectionFactory extends connection_factory_1.ConnectionFactory {
    onSqlModeAdjusted;
    constructor(onSqlModeAdjusted) {
        super();
        this.onSqlModeAdjusted = onSqlModeAdjusted;
    }
    createConnection;
    openConnection(options, callback) {
        if (!this.createConnection) {
            void (0, optional_drivers_1.importDriver)('mysql2').then(mysql2 => {
                this.createConnection = mysql2.default.createConnection;
                this.openConnection(options, callback);
            }, 
            // mysql2 is an optional dependency, so report a missing driver instead of
            // letting the rejection escape as an unhandled promise rejection
            e => callback(new Error(`Node.js DB driver "mysql2" could not be loaded: ${e}`)));
            return;
        }
        const connection = this.createConnection(options);
        connection.connect((err) => {
            if (err) {
                return callback(err, connection);
            }
            checkSqlMode(connection, callback, this.onSqlModeAdjusted);
        });
    }
    closeConnection(connection, callback) {
        if (connection) {
            connection.end(callback);
        }
        else {
            callback?.(null);
        }
    }
    supportsParams = true;
    execute(connection, sql, callback, params) {
        // query(), NOT execute(): mysql2's execute() prepares a server-side statement and caches it per
        // connection, keyed by the SQL text. An INSERT of a batch has as many placeholders as values, so
        // nearly every batch would be a distinct text with its own prepared statement, and MySQL fails
        // with "Can't create more than max_prepared_stmt_count statements" (default 16382) after a while.
        // query() with values lets mysql2 escape them on the client (quotes, backslashes, NUL) and send
        // one plain statement.
        const done = (err, results) => {
            if (err) {
                return callback(err);
            }
            return callback(null, results);
        };
        if (params?.length) {
            connection.query(sql, params, done);
        }
        else {
            connection.query(sql, done);
        }
    }
}
class MySQL2Client extends sql_client_1.default {
    constructor(sqlConnection, onSqlModeAdjusted) {
        super(sqlConnection, new MySQL2ConnectionFactory(onSqlModeAdjusted));
    }
}
exports.MySQL2Client = MySQL2Client;
class MySQL2ClientPool extends sql_client_pool_1.SQLClientPool {
    constructor(poolOptions, sqlOptions, onSqlModeAdjusted) {
        super(poolOptions, sqlOptions, new MySQL2ConnectionFactory(onSqlModeAdjusted));
    }
}
exports.MySQL2ClientPool = MySQL2ClientPool;
//# sourceMappingURL=mysql-client.js.map