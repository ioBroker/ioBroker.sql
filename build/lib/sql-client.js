"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const node_events_1 = require("node:events");
class SQLClient extends node_events_1.EventEmitter {
    options;
    factory;
    pooled_at = null;
    borrowed_at = null;
    connected_at = null;
    connection;
    constructor(options, connectionFactory) {
        super();
        this.options = options;
        this.factory = connectionFactory;
    }
    connect(callback) {
        if (!this.connection) {
            return this.factory.openConnection(this.options, (err, connection) => {
                if (err) {
                    callback?.(err);
                    return;
                }
                this.connection = connection;
                this.connected_at = Date.now();
                return callback?.();
            });
        }
        return callback?.();
    }
    disconnect(callback) {
        if (this.connection) {
            this.factory.closeConnection(this.connection, err => {
                if (err) {
                    callback?.(err, this.connection);
                    return;
                }
                this.connection = null;
                this.connected_at = null;
                callback?.();
            });
            return;
        }
        return callback?.();
    }
    execute(sql, callback) {
        if (!this.connection) {
            return this.connect(err => {
                if (err) {
                    callback(err);
                }
                else {
                    this.execute(sql, callback);
                }
            });
        }
        this.factory.execute(this.connection, sql, (err, result) => {
            if (err) {
                callback(err);
            }
            else {
                callback(null, result);
            }
        });
    }
}
exports.default = SQLClient;
//# sourceMappingURL=sql-client.js.map