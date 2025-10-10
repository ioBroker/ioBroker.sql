"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.SQLite3ClientPool = exports.SQLite3Client = exports.SQLite3ConnectionFactory = void 0;
const connection_factory_1 = require("./connection-factory");
const sql_client_1 = __importDefault(require("./sql-client"));
const sql_client_pool_1 = require("./sql-client-pool");
class SQLite3ConnectionFactory extends connection_factory_1.ConnectionFactory {
    Database;
    openConnection(options, callback) {
        if (!this.Database) {
            void import('sqlite3').then(sqlite3 => {
                this.Database = sqlite3.default.Database;
                this.openConnection(options, callback);
            });
            return;
        }
        if (options.mode) {
            const db = new this.Database(options.fileName, options.mode, (err) => {
                if (err) {
                    callback(err);
                }
                else {
                    callback(null, db);
                }
            });
            return;
        }
        const db = new this.Database(options.fileName, (err) => {
            if (err) {
                callback(err);
            }
            else {
                callback(null, db);
            }
        });
    }
    closeConnection(db, callback) {
        if (db) {
            db.close(callback);
        }
        else {
            callback?.();
        }
    }
    execute(db, sql, callback) {
        db.all(sql, [], callback);
    }
}
exports.SQLite3ConnectionFactory = SQLite3ConnectionFactory;
class SQLite3Client extends sql_client_1.default {
    constructor(sqliteOptions) {
        super(sqliteOptions, new SQLite3ConnectionFactory());
    }
}
exports.SQLite3Client = SQLite3Client;
class SQLite3ClientPool extends sql_client_pool_1.SQLClientPool {
    constructor(poolOptions, sqliteOptions) {
        super(poolOptions, sqliteOptions, new SQLite3ConnectionFactory());
    }
}
exports.SQLite3ClientPool = SQLite3ClientPool;
//# sourceMappingURL=sqlite3-client.js.map