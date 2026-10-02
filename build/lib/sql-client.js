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
    broken = false;
    lastError = null;
    constructor(options, connectionFactory) {
        super();
        this.options = options;
        this.factory = connectionFactory;
    }
    /**
     * Take ownership of a freshly opened connection.
     *
     * All four drivers hand out an EventEmitter (mysql2 `Connection`, pg `Client`, mssql
     * `ConnectionPool`, sqlite3 `Database`) and emit `error` on it asynchronously when the server
     * drops the socket - `ECONNRESET` or `PROTOCOL_CONNECTION_LOST`. An `error` event without a
     * listener is thrown by EventEmitter, which used to terminate the adapter with
     * UNCAUGHT_EXCEPTION instead of reconnecting, so the listener is attached here, centrally, for
     * every dialect.
     *
     * @param connection the connection just returned by the factory
     */
    #adoptConnection(connection) {
        this.connection = connection;
        this.connected_at = Date.now();
        this.broken = false;
        this.lastError = null;
        if (!connection || typeof connection.on !== 'function') {
            return;
        }
        connection.on('error', (err) => {
            // The listener is never removed, so that a driver emitting `error` while or after
            // closing can never throw either. That also means events from a connection we have
            // already replaced can still arrive - ignore those instead of flagging the client that
            // now holds a healthy connection.
            if (this.connection !== connection) {
                return;
            }
            // A connection-level error means this connection is unusable: any further statement
            // would fail with "Can't add new command when connection is in closed state". Flagging
            // it makes the pool drop the client on the next borrow and open a fresh connection.
            this.broken = true;
            this.lastError = err instanceof Error ? err : new Error(String(err));
            // Re-emit for whoever borrowed this client, but only when someone is listening:
            // emit('error') without a listener throws ERR_UNHANDLED_ERROR, which is the very crash
            // this handler exists to prevent.
            if (this.listenerCount('error')) {
                this.emit('error', this.lastError);
            }
        });
    }
    /**
     * Drop the connection and the error state that belonged to it.
     *
     * The `error` listener stays on the old connection object on purpose - removing it would let a
     * driver that emits during or after close throw again. It is inert once `this.connection` no
     * longer points at that object, and goes away with it.
     */
    #releaseConnection() {
        this.connection = null;
        this.connected_at = null;
        this.broken = false;
        this.lastError = null;
    }
    /** Whether the driver reported a connection-level error, so this client must not be reused. */
    isBroken() {
        return this.broken;
    }
    /** The connection-level error that broke this client, if any. */
    getLastError() {
        return this.lastError;
    }
    connect(callback) {
        if (!this.connection) {
            return this.factory.openConnection(this.options, (err, connection) => {
                if (err) {
                    callback?.(err);
                    return;
                }
                this.#adoptConnection(connection);
                callback?.();
            });
        }
        callback?.();
    }
    connectAsync() {
        if (!this.connection) {
            return new Promise((resolve, reject) => this.factory.openConnection(this.options, (err, connection) => {
                if (err) {
                    reject(err);
                }
                else {
                    this.#adoptConnection(connection);
                    resolve();
                }
            }));
        }
        return Promise.resolve();
    }
    disconnect(callback) {
        if (this.connection) {
            this.factory.closeConnection(this.connection, err => {
                if (err) {
                    callback?.(err);
                    return;
                }
                this.#releaseConnection();
                callback?.();
            });
            return;
        }
        return callback?.();
    }
    disconnectAsync() {
        if (this.connection) {
            return new Promise((resolve, reject) => this.factory.closeConnection(this.connection, err => {
                if (err) {
                    reject(err);
                }
                else {
                    this.#releaseConnection();
                    resolve();
                }
            }));
        }
        return Promise.resolve();
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
    async executeAsync(sql) {
        if (!this.connection) {
            await this.connectAsync();
        }
        return new Promise((resolve, reject) => this.factory.execute(this.connection, sql, (err, result) => {
            if (err) {
                reject(err);
            }
            else {
                resolve(result);
            }
        }));
    }
}
exports.default = SQLClient;
//# sourceMappingURL=sql-client.js.map