import type { BoundStatement, SqlStatement } from '../types';

/**
 * Whether a statement carries bound values.
 *
 * @param statement plain SQL or SQL with values
 */
export function isBoundStatement(statement: SqlStatement): statement is BoundStatement {
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
export function statementToString(statement: SqlStatement): string {
    if (!isBoundStatement(statement)) {
        return statement;
    }
    return statement.params.length ? `${statement.sql} ${JSON.stringify(statement.params)}` : statement.sql;
}
