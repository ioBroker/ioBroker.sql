/** The shape `processMessage` answers with when it cannot serve a message yet */
export type NotInitializedResponse = {
    result: [];
    step: null;
    error: string;
};

/**
 * Messages that cannot be answered before the SQL dialect is known, i.e. before `sqlFuncs` has been
 * picked from the configuration in `main()`.
 *
 * The `message` handler is installed in the constructor, so the message box starts delivering as
 * soon as the adapter is ready - while `main()` is still awaiting `system.config`. Charts that kept
 * polling during a restart have their `getHistory` requests queued and delivered in one batch right
 * at that moment, which is how a single restart used to produce an immediate UNCAUGHT_EXCEPTION.
 * `stateChange` and `objectChange` cannot hit this window: both subscribe only after the dialect is
 * set. See https://github.com/ioBroker/ioBroker.sql/issues/527
 *
 * `features`, `enableHistory`, `disableHistory`, `getEnabledDPs` and `stopInstance` are deliberately
 * absent: they answer from memory or only write objects, so they work before the dialect is known.
 */
export const COMMANDS_REQUIRING_DB = new Set([
    'getHistory',
    'getCounter',
    'destroy',
    'query',
    'update',
    'delete',
    'deleteAll',
    'deleteRange',
    'storeState',
    'getRawEntries',
    'getDatapoints',
    'getDpOverview',
]);

/**
 * Decide whether a message has to be rejected because the dialect is not known yet.
 *
 * This lives outside `main.ts` so that it can be unit tested: importing `main.ts` pulls in
 * `@iobroker/adapter-core`, which calls `process.exit(10)` at module load time when js-controller
 * cannot be resolved - and js-controller is not a dependency of this repository.
 *
 * @param command the message command, as delivered in `msg.command`
 * @param dialectKnown whether `sqlFuncs` has been assigned
 * @returns the response to answer with, or `null` when the message may be processed
 */
export function guardUninitialized(command: string, dialectKnown: boolean): NotInitializedResponse | null {
    if (!dialectKnown && COMMANDS_REQUIRING_DB.has(command)) {
        return { result: [], step: null, error: 'Adapter is not initialized yet' };
    }
    return null;
}
