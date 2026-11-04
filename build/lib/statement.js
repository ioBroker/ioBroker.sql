"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.isBoundStatement = isBoundStatement;
exports.statementToString = statementToString;
/**
 * Whether a statement carries bound values.
 *
 * @param statement plain SQL or SQL with values
 */
function isBoundStatement(statement) {
    return typeof statement === 'object' && statement !== null && typeof statement.sql === 'string';
}
/**
 * Text of a statement for the log.
 *
 * The values are appended as JSON instead of being merged into the SQL, so the log shows exactly what
 * was sent to the driver.
 *
 * @param statement plain SQL or SQL with values
 */
function statementToString(statement) {
    if (!isBoundStatement(statement)) {
        return statement;
    }
    return statement.params.length ? `${statement.sql} ${JSON.stringify(statement.params)}` : statement.sql;
}
//# sourceMappingURL=statement.js.map