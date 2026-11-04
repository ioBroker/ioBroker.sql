"use strict";
/**
 * Picking the row of a datapoint or source from a lookup by name.
 *
 * Kept out of `main.ts` so it can be unit tested, like `values.ts`.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.pickRowByName = pickRowByName;
/** The form under which MySQL/MariaDB and MS SQL compare two names by default */
function caseFolded(name) {
    return name.toLowerCase().replace(/ +$/, '');
}
/**
 * The row of `name` from the result of `getIdSelect()` or `getFromSelect()`, or `undefined` if the
 * lookup only found other names.
 *
 * ioBroker IDs are case-sensitive, but MySQL/MariaDB and MS SQL compare `name` with the collation of
 * the column, and their defaults (`utf8_general_ci`, which the adapter uses for a database it creates,
 * `utf8mb4_0900_ai_ci`, `SQL_Latin1_General_CP1_CI_AS`) ignore case and, with PAD SPACE, trailing
 * spaces. `WHERE name='alias.0.licht'` therefore also returns the row of `alias.0.Licht`. Taking
 * `rows[0]` gave both datapoints one series, and the type of the first one, so the values of the
 * second were even converted into it. See https://github.com/ioBroker/ioBroker.sql/issues/588
 *
 * A row with exactly this name wins. A row whose name differs only in case or trailing spaces belongs
 * to another ID and is never taken. Any other row is still the same ID: MS SQL stores `name` as
 * `varchar`, which replaces characters outside the code page of the collation (`alias.0.Кухня` comes
 * back as `alias.0.?????`). Such a name never matches exactly, and refusing its row would insert a new
 * one on every lookup.
 *
 * This only stops new values from being mixed. Values already stored under the other ID stay there,
 * and the comparison itself (accents, for example) can only be fixed with a case-sensitive collation.
 *
 * @param rows the rows the lookup returned
 * @param name the name that was looked up
 */
function pickRowByName(rows, name) {
    if (!rows?.length) {
        return undefined;
    }
    const exact = rows.find(row => row.name === name);
    if (exact) {
        return exact;
    }
    const folded = caseFolded(name);
    return rows.find(row => typeof row.name !== 'string' || caseFolded(row.name) !== folded);
}
//# sourceMappingURL=names.js.map