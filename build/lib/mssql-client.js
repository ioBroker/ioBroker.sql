"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.MSSQLClientPool = exports.MSSQLClient = void 0;
const connection_factory_1 = require("./connection-factory");
const sql_client_1 = __importDefault(require("./sql-client"));
const sql_client_pool_1 = require("./sql-client-pool");
class MSSQLConnectionFactory extends connection_factory_1.ConnectionFactory {
    ConnectionPool;
    Request;
    openConnection(options, callback) {
        if (!this.ConnectionPool) {
            void import('mssql').then(mssql => {
                this.ConnectionPool = mssql.default.ConnectionPool;
                this.Request = mssql.default.Request;
                this.openConnection(options, callback);
            });
            return;
        }
        const pos = options.server.indexOf(':');
        if (pos !== -1) {
            options.port = parseInt(options.server.substring(pos + 1), 10);
            options.server = options.server.substring(0, pos);
        }
        options.pool ||= {};
        options.pool.min = 0;
        options.pool.max = 1;
        const connection = new this.ConnectionPool(options, (err) => {
            if (err) {
                callback(new Error(err));
            }
            else {
                callback(null, connection);
            }
        });
    }
    closeConnection(connection, callback) {
        connection.close(callback);
    }
    execute(connection, sql, callback) {
        if (!this.Request) {
            throw new Error('Not initialized request');
        }
        const request = new this.Request(connection);
        request.query(sql, (err, result) => {
            callback(err, result?.recordset);
        });
    }
}
class MSSQLClient extends sql_client_1.default {
    constructor(options) {
        super(options, new MSSQLConnectionFactory());
    }
}
exports.MSSQLClient = MSSQLClient;
class MSSQLClientPool extends sql_client_pool_1.SQLClientPool {
    constructor(poolOptions, sqlOptions) {
        super(poolOptions, sqlOptions, new MSSQLConnectionFactory());
    }
}
exports.MSSQLClientPool = MSSQLClientPool;
//# sourceMappingURL=mssql-client.js.map