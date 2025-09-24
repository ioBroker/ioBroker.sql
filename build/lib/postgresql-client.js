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
    openConnection(connectString, callback) {
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
            return callback(null, results.rows);
        });
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