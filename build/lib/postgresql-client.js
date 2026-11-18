"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.PostgreSQLClientPool = exports.PostgreSQLClient = void 0;
const connection_factory_1 = require("./connection-factory");
const sql_client_1 = __importDefault(require("./sql-client"));
const sql_client_pool_1 = require("./sql-client-pool");
// PostgreSQLConnectionFactory does not use any of node-pg's built-in pooling.
class PostgreSQLConnectionFactory extends connection_factory_1.ConnectionFactory {
    Client;
    /** pg-native cannot run a pg-cursor, so a native client never streams */
    isNative = false;
    /** pg-cursor, imported on first use. It is an optional dependency like pg itself. */
    Cursor;
    /** pg-cursor could not be loaded: do not try the import again for every read */
    cursorUnavailable = false;
    openConnection(connectString, callback) {
        if (!this.Client) {
            void import('pg').then(pg => {
                const nativeClient = pg.default.native?.Client;
                this.Client = nativeClient || pg.default.Client;
                this.isNative = !!nativeClient;
                this.openConnection(connectString, callback);
            }, 
            // pg is an optional dependency, so report a missing driver instead of
            // letting the rejection escape as an unhandled promise rejection
            e => callback(new Error(`Node.js DB driver "pg" could not be loaded: ${e}`)));
            return;
        }
        const connection = new this.Client(connectString);
        connection.connect((err) => callback(err, connection));
    }
    closeConnection(connection, callback) {
        if (connection) {
            connection.end(callback);
        }
        else {
            callback?.(null);
        }
    }
    execute(connection, sql, callback) {
        connection.query(sql, (err, results) => {
            if (err) {
                return callback(err);
            }
            return callback(null, results?.rows);
        });
    }
    executeStreamed(connection, sql, batchSize, onRows, callback) {
        if (this.isNative || this.cursorUnavailable) {
            callback(null, false);
            return;
        }
        if (!this.Cursor) {
            void import('pg-cursor').then(mod => {
                this.Cursor = mod.default;
                this.executeStreamed(connection, sql, batchSize, onRows, callback);
            }, 
            // optional dependency missing: remember it and let the caller read the buffered way
            () => {
                this.cursorUnavailable = true;
                callback(null, false);
            });
            return;
        }
        const cursor = connection.query(new this.Cursor(sql));
        let finished = false;
        const finish = (err) => {
            if (!finished) {
                finished = true;
                callback(err, true);
            }
        };
        const read = () => {
            cursor.read(batchSize, (err, rows) => {
                if (err) {
                    cursor.close(() => finish(err));
                }
                else if (!rows.length) {
                    // Every row was delivered. An error while closing the cursor does not change the
                    // result; a broken connection is noticed by the pool on the next borrow.
                    cursor.close(() => finish(null));
                }
                else {
                    onRows(rows);
                    read();
                }
            });
        };
        read();
    }
}
class PostgreSQLClient extends sql_client_1.default {
    constructor(connectString) {
        super(connectString, new PostgreSQLConnectionFactory());
    }
}
exports.PostgreSQLClient = PostgreSQLClient;
class PostgreSQLClientPool extends sql_client_pool_1.SQLClientPool {
    constructor(poolOptions, connectString) {
        super(poolOptions, connectString, new PostgreSQLConnectionFactory());
    }
}
exports.PostgreSQLClientPool = PostgreSQLClientPool;
//# sourceMappingURL=postgresql-client.js.map