"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ConnectionFactory = void 0;
class ConnectionFactory {
    /**
     * Whether `execute()` binds `params`. Only the MySQL factory does so far; for the other drivers a
     * statement with values is rejected instead of running it without them.
     */
    supportsParams = false;
    /**
     * Read the rows of a query in batches instead of buffering the whole result in RAM.
     *
     * `streamed` in the callback says whether streaming took place. `false` means this driver cannot
     * stream (or the connection could not be used for it), and the caller has to read the buffered
     * way with `execute()`. `onRows` is only called while streaming really takes place, so a caller
     * can start any per-request work lazily on the first batch.
     *
     * The default reports "not supported"; only a driver that overrides it streams.
     *
     * @param _connection the open connection
     * @param _sql plain SQL without bound values
     * @param _batchSize maximum number of rows per `onRows` call
     * @param _onRows receives the rows batch by batch, in the order of the query
     * @param callback called once at the end
     */
    executeStreamed(_connection, _sql, _batchSize, _onRows, callback) {
        callback(null, false);
    }
}
exports.ConnectionFactory = ConnectionFactory;
//# sourceMappingURL=connection-factory.js.map