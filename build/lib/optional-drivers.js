"use strict";
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
Object.defineProperty(exports, "__esModule", { value: true });
exports.importDriver = importDriver;
/**
 * Import an optional driver at runtime without making the build depend on it.
 *
 * TypeScript resolves `import('<literal>')` while type checking, so a literal specifier would
 * reintroduce the TS2307 this module exists to avoid. Passing the name through a `string`-typed
 * parameter keeps the import opaque to the type checker; Node resolves it exactly as before.
 *
 * @param name the driver's package name
 */
function importDriver(name) {
    return import(name);
}
//# sourceMappingURL=optional-drivers.js.map