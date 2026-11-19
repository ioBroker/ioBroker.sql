"use strict";
/**
 * The answer of the message `help`: what the instance answers, written for an AI agent (e.g. ioBroker.pi-agent, or
 * the MCP server with `send_to`) and for script authors. Keep it in sync with `#dispatchMessage` in main.ts.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.HELP = exports.COMMANDS = void 0;
/** Every command `#dispatchMessage` answers; `write` = it changes data, settings or the instance */
exports.COMMANDS = [
    { command: 'help', write: false },
    { command: 'getHistory', write: false },
    { command: 'getCounter', write: false },
    { command: 'getEnabledDPs', write: false },
    { command: 'getDatapoints', write: false },
    { command: 'getDpOverview', write: false },
    { command: 'getRawEntries', write: false },
    { command: 'getDpStatistics', write: false },
    { command: 'features', write: false },
    { command: 'test', write: false },
    { command: 'query', write: true },
    { command: 'enableHistory', write: true },
    { command: 'disableHistory', write: true },
    { command: 'storeState', write: true },
    { command: 'update', write: true },
    { command: 'delete', write: true },
    { command: 'deleteRange', write: true },
    { command: 'deleteAll', write: true },
    { command: 'cleanupOrphaned', write: true },
    { command: 'destroy', write: true },
    { command: 'stopInstance', write: true },
];
exports.HELP = `ioBroker.sql: stores the history of the states that are enabled for it (per state in the object settings) in an
SQL database: MySQL/MariaDB, PostgreSQL, MS SQL Server or SQLite (native.dbtype of system.adapter.sql.0). An agent
can read the course of a state (the stored values or one value per interval), the consumption of a counter, list
the logged states and the stored data, run a SELECT and, only on the user's request, enable or disable the
logging and store, change or delete values.

Times (start, end, ts): milliseconds since 1970 or a date text such as "2026-11-19T08:00" (local time of the host).
Times in the answers and in the tables are milliseconds since 1970.

Tables (native.dbname of the instance, default "iobroker", qualifies them in MySQL and MS SQL only):
- MySQL: \`iobroker\`.datapoints; MS SQL: iobroker.dbo.datapoints; PostgreSQL and SQLite: datapoints.
- datapoints (id, name = the state ID, type: 0 number, 1 string, 2 boolean); sources (id, name = state.from).
- ts_number, ts_string, ts_bool (id = datapoints.id, ts in ms, val, ack, _from = sources.id, q): the values, in
  the table of the type. ts_counter (id, ts, val): the readings around a reset or an exchange of a counter.
- ack and the values of ts_bool: 0/1 in MySQL and SQLite, BOOLEAN in PostgreSQL, BIT in MS SQL. String values
  (objects as JSON) are in ts_string. Dialects: LIMIT n in MySQL, PostgreSQL and SQLite, SELECT TOP n in MS SQL;
  strings in single quotes. Example (MySQL): SELECT ts, val FROM \`iobroker\`.ts_number WHERE id = (SELECT id
  FROM \`iobroker\`.datapoints WHERE name = 'hm-rpc.0.ABC.1.TEMPERATURE') ORDER BY ts DESC LIMIT 10

Commands (sendTo sql.0):
- getHistory {id, options: {start?, end?, aggregate?, count?, step?, ...}}: the course of one state:
  {result: [{ts, val, ack?, from?, q?, id?}], step, sessionId}, on an error {result: [], error}. options is
  required (may be {}).
  aggregate: average (default), min, max, minmax (min and max of each interval), total, count, percentile
  {percentile 0-100, default 50}, quantile {quantile 0-1, default 0.5}, integral {integralUnit in s, default 60,
  3600 gives Wh from W; integralInterpolation none (default) | linear}: one value per interval;
  none or onchange: the stored values. id "*" with none or onchange: the values of all datapoints, id = the
  number in the table datapoints.
  start: without it (or 0) only the newest count values are read. end: default now + 5000 s.
  count: the number of intervals (default 500), with none/onchange the number of values (default limit).
  step: the interval in ms instead of count. intervalBorders: [ms, ...] ascending borders of the intervals
  (e.g. local days), replaces start, end and step. limit: at most this many intervals or values (default
  count, else 2000). returnNewestEntries: true = the newest values instead of the oldest when count cuts.
  ack, from, q, addId: true adds these fields (from = the name of the source). round: digits after the decimal
  point (default the setting of the state). ignoreNull: false (default, null stays), true (replaced by the
  value before), 0 (replaced by 0). removeBorderValues: true drops the values before start and after end that
  are added for charts. sessionId: returned as it is.
- getCounter {id, options?: {start?, end?}}: how much a counter (a rising meter reading, e.g. kWh) rose between
  start (default: the beginning) and end (default now), a reset or a replaced meter included (setting "counter"
  of the state): {result: number}. Only for a logged state, else {error: "Not enabled"}.
- getEnabledDPs {}: the states this instance logs: {[id]: their settings (enabled, changesOnly, debounceTime,
  blockTime, changesMinDelta, changesRelogInterval, retention in s, maxLength, counter, storageType, aliasId,
  ...)}.
- getDatapoints {}: every row of the table datapoints, also the ones no longer logged:
  {success, result: [{id, index (datapoints.id), type: Number | String | Boolean | null}]}.
- getDpOverview {}: like getDatapoints with the time of the first value, slow (5 s pause per table):
  {success, result: {[id]: {type: number | string | boolean | undefined, ts}}}.
- getRawEntries {id, start?, end?, limit? (1-5000, default 100), offset? (default 0), sort? desc (default) | asc,
  noTotal?}: the rows exactly as stored (ack and booleans as the database returns them, from null if unknown):
  {id, index, type, table, total, limit, offset, sort, result: [{ts, val, ack, q, from}]}; noTotal: true skips
  the counting (total is missing then).
- getDpStatistics {}: per datapoint the rows and the estimated size: {success, result: [{id, index, type, table,
  count, firstTs, lastTs, estimatedBytes, status}], summary: {datapoints, rows, estimatedBytes, byStatus}}.
  status: active, loggingDisabled (not logged, the object or its settings exist), objectMissing.
- features {}: {supportedFeatures: ["update", "delete", "deleteRange", "deleteAll", "storeState"]}.
- query "SQL text" (the message is the string itself): runs it as it is: {error, result: rows}. It runs ANY
  statement (also DELETE, UPDATE, DROP); use it only for SELECT. ts in the rows is a number.
- enableHistory {id, options?}: logs the state from now on; options = its settings as in getEnabledDPs (the
  defaults of the instance otherwise): {success} or {error}.
- disableHistory {id}: stops logging the state, the stored values stay: {success} or {error}.
- storeState {id, state: {ts?, val, ack?, q?, from?} or a list of them, rules?} or [{id, state, rules?}]: writes
  values into the database, also of a state that is not logged (ts default now); rules: true applies the logging
  settings (only for a logged state): {success, successCount} or {error, errors, successCount}.
- update {id, state: {ts, val?, ack?, q?, from?}} or [{id, state}] or {id, state: [...]}: changes the stored row
  with exactly this ts: {success} or {error, errors}.
- delete {id, ts} or [{id, ts}] or {id, ts: [ts, ...]} (also as {id, state: {ts}}): deletes the rows with
  exactly these ts: {success} or {error, errors}.
- deleteRange {id, start, end?} or [{id, start, end?}]: deletes the rows with start <= ts <= end (end default
  now): {success} or {error, errors}.
- deleteAll {id} or [{id}]: deletes every stored row of these states: {success} or {error, errors}.
- cleanupOrphaned {scope?: {objectMissing? (default true), loggingDisabled? (default false)}, confirm?}: the
  rows of datapoints that are not logged any more. Without confirm: true only the preview {dryRun: true,
  datapoints, rows, estimatedBytes, items}; with confirm: true it deletes them and adds {deleted}.
- destroy {}: drops all tables (and the database, if the adapter created it), then restarts the instance:
  {result: "deleted"} or {error}. Every stored value is lost.
- test {config}, stopInstance: for the settings and js-controller, not for agents.

How to talk to the user:
- Find the state first (search the objects); name states by their name, give times in local time and values
  with their unit.
- A state has a history only if it is or was logged (getEnabledDPs, getDatapoints). For a longer period request
  one value per interval (e.g. aggregate average with count 24 to 200) instead of every stored value. Prefer
  getHistory; write a SELECT only for what it cannot answer, and read dbtype and dbname from the instance first.
- Send only SELECT statements with query. Any other statement only on the user's explicit request.
- query (other than SELECT), delete, deleteRange, deleteAll, update, storeState, cleanupOrphaned, destroy,
  enableHistory, disableHistory and stopInstance change the database, the settings or the instance: never
  without the user's explicit request. Deleted values cannot be restored: name the state and the range and ask
  for confirmation first; show the preview of cleanupOrphaned (without confirm) before deleting; for destroy
  say that the whole history is lost.`;
//# sourceMappingURL=help.js.map