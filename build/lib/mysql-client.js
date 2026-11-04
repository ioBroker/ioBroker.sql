"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.MySQL2ClientPool = exports.MySQL2Client = void 0;
const connection_factory_1 = require("./connection-factory");
const sql_client_1 = __importDefault(require("./sql-client"));
const sql_client_pool_1 = require("./sql-client-pool");
const optional_drivers_1 = require("./optional-drivers");
class MySQL2ConnectionFactory extends connection_factory_1.ConnectionFactory {
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
    constructor(sqlConnection) {
        super(sqlConnection, new MySQL2ConnectionFactory());
    }
}
exports.MySQL2Client = MySQL2Client;
class MySQL2ClientPool extends sql_client_pool_1.SQLClientPool {
    constructor(poolOptions, sqlOptions) {
        super(poolOptions, sqlOptions, new MySQL2ConnectionFactory());
    }
}
exports.MySQL2ClientPool = MySQL2ClientPool;
//# sourceMappingURL=mysql-client.js.map