"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ConnectionFactory = void 0;
class ConnectionFactory {
    /**
     * Whether `execute()` binds `params`. Only the MySQL factory does so far; for the other drivers a
     * statement with values is rejected instead of running it without them.
     */
    supportsParams = false;
}
exports.ConnectionFactory = ConnectionFactory;
//# sourceMappingURL=connection-factory.js.map