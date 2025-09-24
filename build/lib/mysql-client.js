"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.MySQL2ClientPool = exports.MySQL2Client = void 0;
const connection_factory_1 = require("./connection-factory");
const sql_client_1 = __importDefault(require("./sql-client"));
const sql_client_pool_1 = require("./sql-client-pool");
class MySQL2ConnectionFactory extends connection_factory_1.ConnectionFactory {
    createConnection;
    openConnection(options, callback) {
        if (!this.createConnection) {
            void import('mysql2').then(mysql2 => {
                this.createConnection = mysql2.default.createConnection;
                this.openConnection(options, callback);
            });
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
    execute(connection, sql, callback) {
        connection.execute(sql, (err, results) => {
            if (err) {
                return callback(err);
            }
            return callback(null, results);
        });
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