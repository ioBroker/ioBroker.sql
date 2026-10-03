/**
 * Types and the loader for the optional database drivers.
 *
 * `mysql2`, `pg`, `mssql` and `sqlite3` are **optionalDependencies**: npm skips a driver whose
 * installation fails and says so only in a warning, so a checkout can legitimately be missing one.
 * At runtime that is handled - the drivers are `import()`ed lazily and a missing one degrades to a
 * log message. The *build* used to be stricter than the runtime:
 *
 * - `import type { Database } from 'sqlite3'` makes TypeScript resolve the package while type
 *   checking, and
 * - so does `import('sqlite3')`, even though the specifier is only ever evaluated at runtime.
 *
 * Both fail with TS2307 when the driver is absent, so `npm run build:ts` broke on a CI runner that
 * had skipped the optional dependency. `pg` and `mssql` survive that because `@types/pg` and
 * `@types/mssql` are regular devDependencies and are therefore always installed; `mysql2` and
 * `sqlite3` ship their types inside the driver package and have no `@types` fallback.
 *
 * Hence the declarations below: they describe only the sliver of each driver this adapter actually
 * touches, which keeps the build independent of whether the driver is installed. When a new driver
 * call is needed, add it here rather than reaching back into the driver's own types.
 */

/** The `mysql2` connection options this adapter sets. */
export interface MySQLOptions {
    host?: string;
    port?: number;
    user?: string;
    password?: string;
    /** Takes precedence over host and port - mysql2 then ignores both. */
    socketPath?: string;
    ssl?: { rejectUnauthorized: boolean };
}

/** The `mysql2` connection surface this adapter uses. */
export interface MySQLConnection {
    end(callback?: (err?: Error | null) => void): void;
}

/** The `sqlite3` database surface this adapter uses. */
export interface SQLite3Database {
    close(callback?: (err: Error | null) => void): void;
    all<T>(sql: string, params: Array<unknown>, callback: (err: Error | null, rows?: Array<T>) => void): void;
}

/** The two `new sqlite3.Database(...)` overloads this adapter calls. */
export interface SQLite3DatabaseConstructor {
    new (fileName: string, callback: (err: Error | null) => void): SQLite3Database;
    new (fileName: string, mode: number, callback: (err: Error | null) => void): SQLite3Database;
}

/** Shape of `await import('mysql2')`. */
export interface MySQLModule {
    default: { createConnection: (options: MySQLOptions) => MySQLConnection };
}

/** Shape of `await import('sqlite3')`. */
export interface SQLite3Module {
    default: { Database: SQLite3DatabaseConstructor };
}

/**
 * Import an optional driver at runtime without making the build depend on it.
 *
 * TypeScript resolves `import('<literal>')` while type checking, so a literal specifier would
 * reintroduce the TS2307 this module exists to avoid. Passing the name through a `string`-typed
 * parameter keeps the import opaque to the type checker; Node resolves it exactly as before.
 *
 * @param name the driver's package name
 */
export function importDriver<T>(name: string): Promise<T> {
    return import(name) as Promise<T>;
}
