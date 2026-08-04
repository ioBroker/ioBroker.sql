# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`iobroker.sql` is an ioBroker adapter that logs ioBroker state history into a SQL database and serves it back as
time series. It supports four dialects: **MySQL, PostgreSQL, MS SQL Server, SQLite**.

The DB drivers (`mysql2`, `pg`, `mssql`, `sqlite3`) are **optionalDependencies** and are `import()`ed lazily at
connect time — a missing driver must degrade to a log message, not a crash at require time.

## Commands

```bash
npm run build          # tsc -p tsconfig.build.json  → src/*.ts to build/
npm run check:ts       # type-check only (tsconfig.json has noEmit: true)
npm run lint           # eslint (@iobroker/eslint-config); --fix handles most findings
npx prettier --write src   # formatting (config re-exported from @iobroker/eslint-config)
```

Minimum runtime is **Node.js 22** (`engines.node`), and CI covers 22.x and 24.x.

**Build before testing.** Tests load the compiled output (`build/lib/aggregate`, and js-controller starts
`build/main.js`). `build/` is committed to git — the release script runs `npm run build` before the release
commit, so regenerate and commit it when you change `src/`.

### Tests

`npm test` runs `mocha --exit` over the whole `test/` directory, which requires MySQL, PostgreSQL **and** MSSQL
to be reachable — not usually what you want locally. Run individual files instead:

```bash
npx mocha test/testSQLite.js --exit    # no external DB needed
npx mocha test/testCommons.js --exit   # pure unit tests of aggregate.ts
npx mocha test/testIntegral.js --exit  # pure unit tests of integral aggregation
npx mocha test/testPackageFiles.js --exit
SQL_PASS=root npx mocha test/testMySQL.js --exit   # server must already be running
SQL_USER=iobroker SQL_PASS=iobroker npx mocha 'test/testMySQL*.js' --exit   # e.g. against a local MariaDB
```

- DB credentials come from `SQL_USER` / `SQL_PASS`; both default to what CI uses (`root` for MySQL,
  `postgres`, `sa`), so leaving them unset reproduces CI. Host and dbtype are set in each test file's
  `before` hook.
- **The MySQL tests write into a database literally named `iobroker`** and cannot be redirected — the
  final assertion in each file queries `iobroker.datapoints` by name. They only ever add datapoints and
  rows (the suite issues no `delete`/`deleteAll`/`destroy`, and `retention()` is scoped to one datapoint
  index), but do not point them at a server holding history you care about.
- The `test*.js` files (except `testCommons`/`testIntegral`/`testPackageFiles`) are **integration tests**:
  `test/lib/setup.js` installs a real js-controller into `tmp/`, starts it, and drives the adapter over the
  message bus. The first run downloads js-controller and can take minutes (600 s mocha timeout).
- `test/lib/testcases.js` holds the shared assertion suite; each DB file calls
  `tests.register(it, expect, sendTo, name, writeNulls, assumeExistingData, additionalActiveObjects)`.
- **Order matters for MySQL**: `testMySQLExisting.js` / `testMySQLExistingNoNulls.js` pass
  `assumeExistingData = 1 / 2` and expect the rows written by an earlier `testMySQL.js` run in the same
  `iobroker` database. CI runs them together via `test/testMySQL*.js` (alphabetical order).
  `testMySQLDash.js` uses a separate database (`io-broker`) to exercise dbname quoting.

## Architecture

### Layers

```
src/main.ts              SqlAdapter — all adapter logic, lifecycle, queues, message handlers
src/lib/<dialect>.ts     pure SQL-string builders (mysql|postgresql|mssql|sqlite)
src/lib/<dialect>-client.ts  driver glue: ConnectionFactory + SQLClient + SQLClientPool subclasses
src/lib/sql-client.ts    generic connection wrapper (callback + *Async variants)
src/lib/sql-client-pool.ts   generic connection pool (borrow/return/evict)
src/lib/aggregate.ts     read-path aggregation & response shaping (no adapter/DB knowledge)
```

**Adding or changing a dialect** means touching two files plus one registration:

1. `src/lib/<db>.ts` — must export the full set of builders typed by `SQLFunc` in `main.ts:34`
   (`init`, `destroy`, `getFirstTs`, `insert`, `retention`, `getIdSelect/Insert/Update`,
   `getFromSelect/Insert`, `getCounterDiff`, `getHistory`, `deleteFromTable`, `update`).
2. `src/lib/<db>-client.ts` — a `ConnectionFactory` (`openConnection`/`closeConnection`/`execute`) plus
   `XClient extends SQLClient` and `XClientPool extends SQLClientPool`.
3. Register both in the `SQLFuncs` record and the `connect()` branch in `main.ts`.

Dialect differences that trip people up:

- **Table qualification is not uniform.** MySQL uses `` `dbname`.table ``, MS SQL uses `dbname.dbo.table`,
  PostgreSQL and SQLite use bare `table` names and ignore the `dbName` argument entirely.
- **PostgreSQL needs a two-phase connect**: connect to the `postgres` database, `CREATE DATABASE`, then
  reconnect to the target DB (`postgresDbCreated` flag in `connect()`).
- MS SQL creates explicit indexes instead of composite primary keys, and its millisecond resolution is why
  several code paths offset timestamps by `+4` / `-4` ms.
- SQLite is the only dialect with `multiRequests: false` — see the task queue below. It also forces
  `writeNulls = false` and `maxConnections = 1`.

### Schema

Four data tables plus two lookup tables, created by `<dialect>.init()`:

| table         | contents                                                              |
|---------------|-----------------------------------------------------------------------|
| `datapoints`  | `id` ↔ state ID, `type` (0 = number, 1 = string, 2 = boolean)          |
| `sources`     | `id` ↔ `state.from`                                                   |
| `ts_number`   | numeric values, keyed `(id, ts)`                                       |
| `ts_string`   | string values (objects are `JSON.stringify`ed into here)              |
| `ts_bool`     | boolean values                                                        |
| `ts_counter`  | counter reset markers used by `getCounter`                            |

`ts_*.id` is the integer from `datapoints`, `_from` the integer from `sources` — both are resolved and cached
in memory (`sqlDPs[id].index`, `this.from[...]`) with in-flight de-duplication so concurrent writes for a new
ID only issue one INSERT.

### Write path

`stateChange` → `pushHistory` (all the per-datapoint filtering: `debounceTime`, `blockTime`, `changesOnly`,
`changesMinDelta`, `changesRelogInterval`, `ignoreZero`, `ignoreBelow/AboveNumber`, `round`, skipped-value
charting optimization) → `pushValueIntoDB` → appended to the in-RAM buffer `sqlDPs[id].list` → flushed by
`storeCached` (when `list.length > maxLength`, on a 10-minute `bufferChecker` interval, on unload, or when a
callback demands it) → `pushValuesIntoDB`.

`pushValuesIntoDB` then forks on `multiRequests`:

- **true** (MySQL/PostgreSQL/MSSQL): fire the INSERT straight at the pool.
- **false** (SQLite): push onto `this.tasks` and let `processTasks()` drain it strictly serially, one task at a
  time (`lockTasks`), spaced by `config.requestInterval`. Adjacent `insert` tasks for the same datapoint index
  are merged. The queue is capped at `MAX_TASKS = 100`.

Connections are borrowed via `borrowClientFromPool` / `returnClientToPool`, which implement an *additional*
limiter on top of the pool (`activeConnections` vs `config.maxConnections`, with `poolBorrowGuard` holding
queued callbacks). Every borrow must have a matching return on all error paths or the adapter deadlocks.

### Read path

`getHistory` message → `getHistorySql` builds the query via `sqlFuncs.getHistory` → results go to
`sendResponse` in `aggregate.ts`, which does interval bucketing and applies the aggregate method
(`onchange`, `minmax`, `min`, `max`, `average`, `total`, `count`, `none`, `percentile`, `quantile`,
`integral`, `integralTotal`). `getCounter` → `getCounterDiff` → `sendResponseCounter`.

`aggregate.ts` is deliberately adapter-agnostic and is the only part covered by fast unit tests
(`testCommons.js`, `testIntegral.js`) — prefer putting new pure logic there.

### Messages API

`processMessage` (`main.ts:1409`) is the external surface, reachable from scripts via `sendTo`:
`features`, `getHistory`, `getCounter`, `test`, `destroy`, `query`, `update`, `delete`, `deleteAll`,
`deleteRange`, `storeState`, `getDpOverview`, `enableHistory`, `disableHistory`, `getEnabledDPs`,
`stopInstance`. The README documents the payloads for most of these.

### Config

`SqlAdapterConfig` / `SqlCustomConfig` (in `src/types.d.ts`) are the *raw* shapes where numbers and booleans
may arrive as strings from the admin UI. `normalizeAdapterConfig()` and `normalizeCustomConfig()` coerce them
into the `…Typed` variants; `this.config` is declared as `SqlAdapterConfigTyped`. Read config through the
normalized types and add new fields to both the raw and typed interfaces plus `io-package.json`'s `native`.

Per-datapoint settings live in `obj.common.custom['sql.<instance>']`, are loaded on startup via the
`system.custom` object view, and are kept live through `objectChange`. `aliasId` maps a real state ID to a
different logging ID (`aliasMap`).

### Admin UI

JSON-config only: `admin/jsonConfig.json` (instance settings, panels `dbTab`, `defaultTab`, `dockerMysql`,
`dockerPhpMyAdmin`) and `admin/jsonCustom.json` (per-datapoint settings). Translations are in
`admin/i18n/<lang>/translations.json` and are maintained by Weblate — do not hand-edit non-English files.
`admin/words.js` is legacy and unused.

### Docker

The adapter can ship its own MySQL + phpMyAdmin containers. **`docker-compose.yaml` is the single source of
truth** for them; the `@iobroker/plugin-docker` plugin (declared in `io-package.json` →
`common.plugins.docker.iobDockerComposeFiles`) parses it, substitutes `${config.<path>[:-default]}` and
`${instance}` from the instance's native config, and creates/starts/updates the containers. The adapter never
constructs a `DockerManager` itself — this mirrors how `ioBroker.frigate` does it.

The adapter's only job is `applyDockerMysqlConfig()`, which mirrors the container's connection settings into
the adapter config (dbtype/dbname/host/port/user/password). It must run **before**
`normalizeAdapterConfig()`, because normalisation derives `multiRequests`/`maxConnections`/`writeNulls` from
`dbtype`. `createUserInDocker()` is a retrying fallback for volumes created before `MYSQL_USER` was declared
in the compose file.

Placeholder gotcha: an empty default (`${config.x:-}`) resolves to the **number 0**, not an empty string
(`Number('') === 0` in the plugin's `parseField`). Omit the `:-` to get `''`.

## Conventions

- **SQL is built by string concatenation, not parameter binding**, throughout `src/lib/<dialect>.ts`. Values
  are sanitized ad hoc (e.g. stripping `'` from strings). Match the existing escaping when adding queries.
- The codebase is callback-first (`cb(err, result)`) with `*Async` wrappers only where they already exist.
  Don't convert existing callback chains to promises opportunistically.
- Everything runs under `strict` TypeScript except `useUnknownInCatchVariables`, which is off — `catch (e)`
  gives you `any`.
- Releases go through `@alcalzone/release-script` (`npm run release-patch|minor|major`); the changelog lives in
  both `README.md` and `io-package.json` → `common.news`. Tagging `v<semver>` triggers npm publish in CI.

## Testing conventions

Tests use **`node:assert`** — there is no chai/expect dependency. `test/lib/testcases.js` exports
`register(it, sendTo, adapterShortName, writeNulls, assumeExistingData, additionalActiveObjects)`.

`test/testPackageFiles.js` delegates to `@iobroker/testing`'s `tests.packageFiles()`.

**Every assertion in the integration tests runs inside a `sendTo` callback**, which the states-redis
client invokes from an `Immediate`. A throw there never reaches mocha — it is caught and printed by
`statesInRedisClient.js`, `done()` is never called, and mocha reports a bare *"Timeout of Nms
exceeded"*. So a wall of timeouts usually means **one** failed assertion, not a hung adapter: search
the run output for `AssertionError` / `ReferenceError` to get the real message and line. Capture the
full log (`npx mocha test/testSQLite.js --exit > run.log 2>&1`) — the interesting part is in the
middle, not the tail.

eslint **ignores `test/**/*.js`** (`eslint.config.mjs`), so `no-undef` never runs on the test suite —
that is how a `testcases.js` missing its `require('node:assert')` passed `check-and-lint` while every
DB job failed. When touching the tests, lint them explicitly with node+mocha globals.

## Notes for future work

- **Known limitation of `@iobroker/plugin-docker@1.0.3`:** a `networks:` entry in a compose file never reaches
  the created container. `compose2config` only emits `networks`, never `networkMode`, and container creation
  reads only `networkMode` (`DockerManager.js:523`). Containers therefore land on Docker's default bridge,
  which has no name-based DNS — so `phpMyAdmin` cannot reach `iob_sql_<instance>_mysql` until the plugin maps
  `networks: - true` to `networkMode`. The compose file already declares it correctly for that day.
- The pool abstraction is still loosely typed: `SQLConnection = any` (`connection-factory.ts:1`) and
  `options: any` in `sql-client.ts`. Making `ConnectionFactory` generic over connection/options types would be
  the highest-value remaining typing improvement.
- `SQLClientPool` does **not** open itself in the constructor; callers must call `open(opts, cb)` so that
  errors surface through the callback.
- Local integration tests need a machine with **no** running js-controller — `test/lib/setup.js` refuses to do
  its first-run setup otherwise.
