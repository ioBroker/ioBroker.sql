import type { ConnectionFactory } from './connection-factory';
import SQLClient from './sql-client';

export type PoolConfig = {
    min_idle?: number;
    max_idle?: number;
    max_active?: number;
    when_exhausted?: 'grow' | 'block' | 'fail';
    max_wait?: number;
    wait_interval?: number;
    max_retries?: number | null;
    retry_interval?: number;
    max_age?: number;
    evictionRunInterval?: number;
    eviction_run_length?: number;
    unref_eviction_runner?: boolean;
};

export class SQLClientPool {
    static MESSAGES = {
        POOL_NOT_OPEN: "The pool is not open; please call 'open' before invoking this method.",
        TOO_MANY_RETURNED:
            'More clients have been returned to the pool than were active. A client may have been returned twice.',
        EXHAUSTED: 'The maximum number of clients are already active; cannot obtain a new client.',
        MAX_WAIT:
            'The maximum number of clients are already active and the maximum wait time has been exceeded; cannot obtain a new client.',
        INVALID: 'Unable to create a valid client.',
        INTERNAL_ERROR: 'Internal error.',
        INVALID_ARGUMENT: 'Invalid argument.',
        NULL_RETURNED: 'A null object was returned.',
        CLOSED_WITH_ACTIVE: 'The pool was closed, but some clients remain active (were never returned).',
    };
    static DEFAULT_WAIT_INTERVAL = 50;
    static DEFAULT_RETRY_INTERVAL = 50;

    private pool: SQLClient[] = [];

    private poolOptions: PoolConfig = {};

    private poolIsOpen = false;

    private returned = 0;

    private active = 0;

    private evictionRunner: ReturnType<typeof setInterval> | null = null;

    private readonly sqlOptions: any;

    private readonly factory: ConnectionFactory;

    constructor(poolOptions: PoolConfig, sqlOptions: any, factory: ConnectionFactory) {
        this.factory = factory;
        // CONFIGURATION OPTIONS:
        //  - `min_idle` - minimum number of idle connections in an "empty" pool
        //  - `max_idle` - maximum number of idle connections in a "full" pool
        //  - `max_active` - maximum number of connections active at one time
        //  - `when_exhausted` - what to do when max_active is reached (`grow`,`block`,`fail`),
        //  - `max_wait` - when `when_exhausted` is `block` max time (in millis) to wait before failure, use < 0 for no maximum
        //  - `wait_interval`  - when `when_exhausted` is `BLOCK`, amount of time (in millis) to wait before rechecking if connections are available
        //  - `max_retries` - number of times to attempt to create another new connection when a newly created connection is invalid; when `null` no retry attempts will be made; when < 0 an infinite number of retries will be attempted
        //  - `retry_interval`  - when `max_retries` is > 0, amount of time (in millis) to wait before retrying
        //  - `max_age` - when a positive integer, connections that have been idle for `max_age` milliseconds will be considered invalid and eligable for eviction
        //  - `evictionRunInterval` - when a positive integer, the number of milliseconds between eviction runs; during an eviction run idle connections will be tested for validity and if invalid, evicted from the pool
        //   - `eviction_run_length` - when a positive integer, the maxiumum number of connections to examine per eviction run (when not set, all idle connections will be evamined during each eviction run)
        //   - `unref_eviction_runner` - unless `false`, `unref` (https://nodejs.org/api/timers.html#timers_unref) will be called on the eviction run interval timer
        this.sqlOptions = sqlOptions;
        this.factory = factory;
        this.open(poolOptions);
    }

    create(callback: (err: Error | undefined, client: SQLClient) => void): void {
        const client = new SQLClient(this.sqlOptions, this.factory);
        client.connect(err => callback(err, client));
    }

    activate(client: SQLClient | undefined, callback: (err?: Error | null, client?: SQLClient) => void): void {
        callback(null, client);
    }

    validate(
        client: SQLClient | undefined,
        callback: (err: Error | null | undefined, valid: boolean, client?: SQLClient) => void,
    ): void {
        if (
            client &&
            (!client.pooled_at ||
                !this.poolOptions?.max_age ||
                Date.now() - client.pooled_at < this.poolOptions.max_age)
        ) {
            callback(null, true, client);
        } else {
            callback(null, false, client);
        }
    }

    passivate(client: SQLClient, callback: (err?: Error | null, client?: SQLClient) => void): void {
        return callback(null, client);
    }

    destroy(client: SQLClient | undefined, callback?: (err?: Error | null, client?: SQLClient) => void): void {
        if (client) {
            client.disconnect(callback);
        } else {
            callback?.();
        }
    }

    open(opts: PoolConfig, callback?: (err?: Error | null) => void): void {
        this._config(opts, err => {
            this.poolIsOpen = true;
            callback?.(err);
        });
    }

    close(callback?: (err?: Error | null) => void): void {
        this.poolIsOpen = false;
        if (this.evictionRunner) {
            this.evictionRunner.ref();
            clearTimeout(this.evictionRunner);
            this.evictionRunner = null;
        }
        if (this.pool.length > 0) {
            this.destroy(this.pool.shift(), () => this.close(callback));
            return;
        }
        if (this.active > 0) {
            callback?.(new Error(SQLClientPool.MESSAGES.CLOSED_WITH_ACTIVE));
            return;
        }
        callback?.();
    }

    execute<T>(sql: string, callback: (err: Error | null, result?: Array<T>) => void): void {
        this.borrow((err, client) => {
            if (err) {
                callback(err);
                return;
            }
            if (client == null) {
                callback(new Error('non-null client expected'));
                return;
            }
            client.execute(sql, (err: Error | null, result?: Array<T>): void =>
                this.return(client, () => callback(err, result)),
            );
        });
    }

    borrow(
        callback: (err?: Error | null, client?: SQLClient) => void,
        blockedSince: number | null = null,
        retryCount = 0,
    ): void {
        if (typeof callback !== 'function') {
            throw new Error(SQLClientPool.MESSAGES.INVALID_ARGUMENT);
        }
        if (!this.poolIsOpen) {
            return callback(new Error(SQLClientPool.MESSAGES.POOL_NOT_OPEN));
        }
        if (
            this.poolOptions.max_active &&
            this.active >= this.poolOptions.max_active &&
            this.poolOptions.when_exhausted === 'fail'
        ) {
            return callback(new Error(SQLClientPool.MESSAGES.EXHAUSTED));
        }
        if (
            this.poolOptions.max_active &&
            this.active >= this.poolOptions.max_active &&
            this.poolOptions.when_exhausted === 'block'
        ) {
            if (blockedSince && Date.now() - blockedSince >= this.poolOptions.max_wait!) {
                return callback(new Error(SQLClientPool.MESSAGES.MAX_WAIT));
            }
            blockedSince ||= Date.now();
            setTimeout(() => {
                this.borrow(callback, blockedSince, retryCount);
            }, this.poolOptions.wait_interval);
            return;
        }
        if (this.pool.length > 0) {
            const client = this.pool.shift()!;
            this.#activateAndValidateOrDestroy(client, (err, valid, client) => {
                if (err) {
                    callback(err);
                    return;
                }
                if (!valid || !client) {
                    this.borrow(callback);
                    return;
                }
                client.pooled_at = null;
                client.borrowed_at = Date.now();
                this.active++;
                return callback(null, client);
            });
            return;
        }
        return this.create((err, client) => {
            if (err) {
                return callback(err);
            }
            return this.#activateAndValidateOrDestroy(client, (err, valid, client) => {
                if (err) {
                    return callback(err);
                }
                if (!valid || !client) {
                    if (this.poolOptions.max_retries && this.poolOptions.max_retries > retryCount) {
                        return setTimeout(() => {
                            return this.borrow(callback, blockedSince, retryCount + 1);
                        }, this.poolOptions.retry_interval || 0);
                    }
                    return callback(new Error(SQLClientPool.MESSAGES.INVALID));
                }
                client.pooled_at = null;
                client.borrowed_at = Date.now();
                this.active++;
                return callback(null, client);
            });
        });
    }

    return(client: SQLClient, callback?: (err?: Error | null) => void): void {
        if (!client) {
            callback?.(new Error(SQLClientPool.MESSAGES.NULL_RETURNED));
            return;
        }
        if (this.active <= 0) {
            callback?.(new Error(SQLClientPool.MESSAGES.TOO_MANY_RETURNED));
            return;
        }
        this.returned++;
        this.active--;
        this.passivate(client, (err, client) => {
            if (err || !client) {
                callback?.(err || new Error(SQLClientPool.MESSAGES.NULL_RETURNED));
                return;
            }
            client.pooled_at = Date.now();
            client.borrowed_at = null;
            if (this.pool.length >= this.poolOptions.max_idle!) {
                this.destroy(client, callback);
                return;
            }
            this.pool.push(client);
            callback?.();
        });
        return;
    }

    _config(opts: PoolConfig, callback?: (err?: Error | null) => void): void {
        if (
            opts.max_retries !== null &&
            opts.max_retries !== undefined &&
            (typeof opts.max_retries !== 'number' || opts.max_retries <= 0)
        ) {
            opts.max_retries = null;
        }
        if (opts.max_retries) {
            if (opts.retry_interval && (typeof opts.retry_interval !== 'number' || opts.retry_interval <= 0)) {
                opts.retry_interval = 0;
            } else if (opts.retry_interval == null) {
                opts.retry_interval = SQLClientPool.DEFAULT_RETRY_INTERVAL;
            }
        }
        if (typeof opts.max_idle === 'number' && opts.max_idle < 0) {
            opts.max_idle = Number.MAX_VALUE;
        } else if (typeof opts.max_idle !== 'number') {
            opts.max_idle = 0;
        }
        if (typeof opts.min_idle !== 'number' || opts.min_idle < 0) {
            opts.min_idle = 0;
        }
        if (opts.min_idle > opts.max_idle) {
            opts.min_idle = opts.max_idle;
        }
        if (typeof opts.max_active !== 'number' || opts.max_active < 0) {
            opts.max_active = Number.MAX_VALUE;
        }
        if (typeof opts.max_wait !== 'number' || opts.max_wait < 0) {
            opts.max_wait = Number.MAX_VALUE;
        }
        if (typeof opts.wait_interval !== 'number' || opts.wait_interval < 0) {
            opts.wait_interval = SQLClientPool.DEFAULT_WAIT_INTERVAL;
        }
        if (opts.when_exhausted !== 'grow' && opts.when_exhausted !== 'block' && opts.when_exhausted !== 'fail') {
            opts.when_exhausted = 'grow';
        }
        if (opts.max_age === null || opts.max_age === undefined || opts.max_age < 0) {
            opts.max_age = Number.MAX_VALUE;
        }
        this.poolOptions = opts;
        this.#reconfig(callback);
    }

    #reconfig(callback?: (err?: Error | null, borrowed?: SQLClient[]) => void): void {
        if (this.evictionRunner) {
            this.evictionRunner.ref();
            clearTimeout(this.evictionRunner);
            this.evictionRunner = null;
        }
        this._evict(err => {
            if (err) {
                callback?.(err);
                return;
            }
            this._prepopulate((err?: Error | null, borrowed?: SQLClient[]): void => {
                if (this.poolOptions.evictionRunInterval && this.poolOptions.evictionRunInterval > 0) {
                    this.evictionRunner = setInterval(() => this.#evictionRun(), this.poolOptions.evictionRunInterval);
                    if (this.poolOptions.unref_eviction_runner !== false) {
                        this.evictionRunner.unref();
                    }
                }
                callback?.(err, borrowed);
            });
        });
    }

    _evict(callback: (err?: Error | null) => void): void {
        return this.#evictionRun(0, callback);
    }

    #evictionRun(numToCheck?: number | null, callback?: (err?: Error | null) => void): void {
        const newPool = [];
        let numChecked = 0;
        while (this.pool.length > 0) {
            const client = this.pool.shift()!;
            if (numToCheck === null || numToCheck === undefined || numToCheck <= 0 || numChecked < numToCheck) {
                numChecked += 1;
                if (newPool.length < this.poolOptions.max_idle! && client) {
                    newPool.push(client);
                } else {
                    client.disconnect();
                }
            } else {
                newPool.push(client);
            }
        }
        this.pool = newPool;
        callback?.();
    }

    _prepopulate(callback: (err?: Error | null, borrowed?: SQLClient[]) => void): void {
        const n = this.poolOptions.min_idle! - this.pool.length;
        if (n > 0) {
            this.#borrowN(n, [], (err, borrowed) => {
                if (err) {
                    return callback(err);
                }
                this.#returnN(borrowed, callback);
            });
            return;
        }
        return callback();
    }

    #borrowN(n: number, borrowed: SQLClient[], callback: (err?: Error | null, borrowed?: SQLClient[]) => void): void {
        if (n > borrowed.length) {
            this.borrow((err, client) => {
                if (client) {
                    borrowed.push(client);
                }
                if (err) {
                    this.#returnN(borrowed, () => callback(err));
                    return;
                }
                this.#borrowN(n, borrowed, callback);
            });
            return;
        }
        return callback(null, borrowed);
    }

    #returnN(borrowed: SQLClient[] | undefined, callback: (err?: Error | null, borrowed?: SQLClient[]) => void): void {
        if (!Array.isArray(borrowed)) {
            callback(new Error(SQLClientPool.MESSAGES.INTERNAL_ERROR));
            return;
        }
        if (borrowed?.length > 0) {
            const client = borrowed.shift()!;
            this.return(client, () => this.#returnN(borrowed, callback));
            return;
        }
        return callback(null);
    }

    #activateAndValidateOrDestroy(
        client: SQLClient | undefined,
        callback: (err: Error | null, valid: boolean, client: SQLClient | null) => void,
    ): void {
        return this.activate(client, (err, client) => {
            if (err) {
                if (client) {
                    this.destroy(client, () => callback(err, false, null));
                    return;
                }
                callback(err, false, null);
                return;
            }
            this.validate(client, (err, valid, client) => {
                if (err) {
                    if (client) {
                        this.destroy(client, () => callback(err, false, null));
                        return;
                    }
                    return callback(err, false, null);
                }
                if (!valid || !client) {
                    this.destroy(client, () => callback(null, false, null));
                    return;
                }
                callback(null, true, client);
            });
        });
    }
}
