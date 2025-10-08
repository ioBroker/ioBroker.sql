import { Adapter, type AdapterOptions, getAbsoluteDefaultDataDir } from '@iobroker/adapter-core'; // Get common adapter utils
import { sendResponseCounter, sendResponse } from './lib/aggregate';
import { existsSync, mkdirSync } from 'node:fs';
import { join, normalize } from 'node:path';
import type {
    DbType,
    SqlAdapterConfig,
    SqlAdapterConfigTyped,
    SqlCustomConfig,
    SqlCustomConfigTyped,
    TableName,
} from './types';

import * as MSSQL from './lib/mssql';
import * as MySQL from './lib/mysql';
import * as PostgreSQL from './lib/postgresql';
import * as SQLite from './lib/sqlite';

import { MSSQLClientPool, MSSQLClient, type MSSQLOptions } from './lib/mssql-client';
import { MySQL2ClientPool, MySQL2Client, type MySQLOptions } from './lib/mysql-client';
import { PostgreSQLClientPool, PostgreSQLClient, type PostgreSQLOptions } from './lib/postgresql-client';
import { SQLite3ClientPool, SQLite3Client, type SQLite3Options } from './lib/sqlite3-client';
import type { SQLClientPool, PoolConfig } from './lib/sql-client-pool';
import type SQLClient from './lib/sql-client';
import type { IobDataEntry } from './lib/types';
import type { ContainerConfig } from './lib/dockerManager.types';
import DockerManager from './lib/DockerManager';

export interface IobDataEntryEx extends Omit<IobDataEntry, 'val'> {
    val: string | boolean | number | null;
    lc: number;
}

type SQLFunc = {
    init: (dbName: string, doNotCreateDatabase?: boolean) => string[];
    destroy: (dbName: string) => string[];
    getFirstTs: (dbName: string, table: 'ts_string' | 'ts_number' | 'ts_bool' | 'ts_counter') => string;
    insert: (
        dbName: string,
        index: number,
        values: {
            table: 'ts_string' | 'ts_number' | 'ts_bool' | 'ts_counter';
            state: { val: any; ts: number; ack?: boolean; q?: number };
            from?: number;
        }[],
    ) => string;
    retention: (
        dbName: string,
        index: number,
        table: 'ts_string' | 'ts_number' | 'ts_bool' | 'ts_counter',
        retention: number,
    ) => string;
    getIdSelect: (dbName: string, name?: string) => string;
    getIdInsert: (dbName: string, name: string, type: 0 | 1 | 2) => string;
    getIdUpdate: (dbName: string, index: number, type: 0 | 1 | 2) => string;
    getFromSelect: (dbName: string, name?: string) => string;
    getFromInsert: (dbName: string, values: string) => string;
    getCounterDiff: (
        dbName: string,
        options: {
            index: number;
            start: number;
            end: number;
        },
    ) => string;
    getHistory: (
        dbName: string,
        table: string,
        options: ioBroker.GetHistoryOptions & { index: number | null },
    ) => string;
    deleteFromTable: (
        dbName: string,
        table: 'ts_string' | 'ts_number' | 'ts_bool' | 'ts_counter',
        index: number,
        start?: number,
        end?: number,
    ) => string;
    update: (
        dbName: string,
        index: number,
        state: { val: number | string | boolean | null | undefined; ts: number; q?: number; ack?: boolean },
        from: number,
        table: 'ts_bool' | 'ts_number' | 'ts_string' | 'ts_counter',
    ) => string;
};

const SQLFuncs: Record<DbType, SQLFunc> = {
    mssql: {
        init: MSSQL.init,
        destroy: MSSQL.destroy,
        getFirstTs: MSSQL.getFirstTs,
        insert: MSSQL.insert,
        retention: MSSQL.retention,
        getIdSelect: MSSQL.getIdSelect,
        getIdInsert: MSSQL.getIdInsert,
        getIdUpdate: MSSQL.getIdUpdate,
        getFromSelect: MSSQL.getFromSelect,
        getFromInsert: MSSQL.getFromInsert,
        getCounterDiff: MSSQL.getCounterDiff,
        getHistory: MSSQL.getHistory,
        deleteFromTable: MSSQL.deleteFromTable,
        update: MSSQL.update,
    },
    mysql: {
        init: MySQL.init,
        destroy: MySQL.destroy,
        getFirstTs: MySQL.getFirstTs,
        insert: MySQL.insert,
        retention: MySQL.retention,
        getIdSelect: MySQL.getIdSelect,
        getIdInsert: MySQL.getIdInsert,
        getIdUpdate: MySQL.getIdUpdate,
        getFromSelect: MySQL.getFromSelect,
        getFromInsert: MySQL.getFromInsert,
        getCounterDiff: MySQL.getCounterDiff,
        getHistory: MySQL.getHistory,
        deleteFromTable: MySQL.deleteFromTable,
        update: MySQL.update,
    },
    postgresql: {
        init: PostgreSQL.init,
        destroy: PostgreSQL.destroy,
        getFirstTs: PostgreSQL.getFirstTs,
        insert: PostgreSQL.insert,
        retention: PostgreSQL.retention,
        getIdSelect: PostgreSQL.getIdSelect,
        getIdInsert: PostgreSQL.getIdInsert,
        getIdUpdate: PostgreSQL.getIdUpdate,
        getFromSelect: PostgreSQL.getFromSelect,
        getFromInsert: PostgreSQL.getFromInsert,
        getCounterDiff: PostgreSQL.getCounterDiff,
        getHistory: PostgreSQL.getHistory,
        deleteFromTable: PostgreSQL.deleteFromTable,
        update: PostgreSQL.update,
    },
    sqlite: {
        init: SQLite.init,
        destroy: SQLite.destroy,
        getFirstTs: SQLite.getFirstTs,
        insert: SQLite.insert,
        retention: SQLite.retention,
        getIdSelect: SQLite.getIdSelect,
        getIdInsert: SQLite.getIdInsert,
        getIdUpdate: SQLite.getIdUpdate,
        getFromSelect: SQLite.getFromSelect,
        getFromInsert: SQLite.getFromInsert,
        getCounterDiff: SQLite.getCounterDiff,
        getHistory: SQLite.getHistory,
        deleteFromTable: SQLite.deleteFromTable,
        update: SQLite.update,
    },
};

const clients = {
    postgresql: { multiRequests: true },
    mysql: { multiRequests: true },
    sqlite: { multiRequests: false },
    mssql: { multiRequests: true },
};

const types: { [valueType: string]: 0 | 1 | 2 } = {
    number: 0,
    string: 1,
    boolean: 2,
    object: 1,
};
const dbNames: TableName[] = ['ts_number', 'ts_string', 'ts_bool'];

const storageTypes: ('Number' | 'String' | 'Boolean')[] = ['Number', 'String', 'Boolean'];

function isEqual(a: any, b: any): boolean {
    //console.log('Compare ' + JSON.stringify(a) + ' with ' +  JSON.stringify(b));
    // Create arrays of property names
    if (a === null || a === undefined || b === null || b === undefined) {
        return a === b;
    }

    const aProps = Object.getOwnPropertyNames(a);
    const bProps = Object.getOwnPropertyNames(b);

    // If the number of properties is different,
    // objects are not equivalent
    if (aProps.length !== bProps.length) {
        //console.log('num props different: ' + JSON.stringify(aProps) + ' / ' + JSON.stringify(bProps));
        return false;
    }

    for (let i = 0; i < aProps.length; i++) {
        const propName = aProps[i];

        if (typeof a[propName] !== typeof b[propName]) {
            //console.log('type props ' + propName + ' different');
            return false;
        } else if (typeof a[propName] === 'object') {
            if (!isEqual(a[propName], b[propName])) {
                return false;
            }
        } else {
            // If values of same property are not equal,
            // objects are not equivalent
            if (a[propName] !== b[propName]) {
                //console.log('props ' + propName + ' different');
                return false;
            }
        }
    }

    // If we made it this far, objects
    // are considered equivalent
    return true;
}

const MAX_TASKS = 100;

type SQLPointConfig = {
    realId: string;
    relogTimeout: NodeJS.Timeout | null;
    timeout: NodeJS.Timeout | null;
    type: 0 | 1 | 2;
    index: number;
    dbType: 0 | 1 | 2;
    config: SqlCustomConfigTyped;
    state: IobDataEntryEx | null;
    skipped: IobDataEntryEx | undefined | null;
    ts: number | null;
    lastCheck: number;
    lastLogTime: number;
    list: { state: IobDataEntryEx; from: number; table: TableName }[];
    inFlight: { [inFlightId: string]: { state: IobDataEntryEx; from: number; table: TableName }[] };
    isRunning?: { id: string; state: IobDataEntryEx; isCounter: boolean; cb?: (err?: Error | null) => void }[];
};

function sortByTs(
    a: IobDataEntryEx & {
        date?: Date;
        id?: string | undefined;
    },
    b: IobDataEntryEx & {
        date?: Date;
        id?: string | undefined;
    },
): 0 | 1 | -1 {
    const aTs = a.ts;
    const bTs = b.ts;
    return aTs < bTs ? -1 : aTs > bTs ? 1 : 0;
}

type TaskUserQuery = { operation: 'userQuery'; msg: ioBroker.Message };
type TaskDelete = { operation: 'delete'; query: string };
type TaskInsert = {
    operation: 'insert';
    index: number;
    list: { state: IobDataEntryEx; from: number; table: TableName }[];
    id: string;
    callback?: (err?: Error | null) => void;
};
type TaskQuery = {
    operation: 'query';
    query: string;
    id: string;
    callback?: (err?: Error | null, results?: IobDataEntry[]) => void;
};
type TaskSelect = {
    operation: 'select';
    query: string;
    options: ioBroker.GetHistoryOptions & { id: string | null; index: number | null };
    callback?: (err: Error | null, result?: (IobDataEntryEx & { date?: Date; id?: string })[]) => void;
};

export class SqlAdapter extends Adapter {
    declare config: SqlAdapterConfigTyped;
    private readonly sqlDPs: { [aliasId: string]: SQLPointConfig } = {};
    private readonly from: { [from: string]: number } = {};
    private readonly tasks: (TaskUserQuery | TaskDelete | TaskInsert | TaskQuery | TaskSelect)[] = [];
    private readonly tasksReadType: {
        id: string;
        state: IobDataEntryEx;
        cb?: ((err?: Error | null) => void) | null;
    }[] = [];
    private readonly tasksStart: { id: string; now: number }[] = [];
    private readonly isFromRunning: {
        [id: string]: { id: string; state: IobDataEntryEx; cb?: (err?: Error | null) => void }[] | null;
    } = {};
    private readonly aliasMap: { [alias: string]: string } = {};

    private finished: (() => void)[] | boolean = false;
    private sqlConnected: boolean | null = null;
    private multiRequests = true;
    private subscribeAll = false;
    private clientPool: SQLClientPool | null = null;
    private reconnectTimeout: NodeJS.Timeout | null = null;
    private testConnectTimeout: NodeJS.Timeout | null = null;
    private dpOverviewTimeout: NodeJS.Timeout | null = null;
    private bufferChecker: NodeJS.Timeout | null = null;
    private activeConnections = 0;
    private poolBorrowGuard: ((err: Error | null | undefined, client: SQLClient) => void)[] = []; // required until https://github.com/intellinote/sql-client/issues/7 is fixed
    private readonly logConnectionUsage = true;
    private postgresDbCreated = false;
    private lockTasks = false;
    private sqlFuncs: SQLFunc | null = null;
    private dockerManager: DockerManager | null = null;

    public constructor(options: Partial<AdapterOptions> = {}) {
        super({
            ...options,
            name: 'sql',
            ready: () => this.main(),
            message: (obj: ioBroker.Message) => this.processMessage(obj),
            stateChange: (id: string, state: ioBroker.State | null | undefined): void => {
                id = this.aliasMap[id] || id;
                this.pushHistory(id, state as IobDataEntryEx);
            },
            objectChange: (id: string, obj: ioBroker.Object | null | undefined): void => {
                let tmpState: IobDataEntryEx | undefined;
                const now = Date.now();
                const formerAliasId = this.aliasMap[id] || id;

                if (
                    obj?.common?.custom?.[this.namespace] &&
                    typeof obj.common.custom[this.namespace] === 'object' &&
                    obj.common.custom[this.namespace].enabled
                ) {
                    const realId = id;
                    let checkForRemove = true;

                    if (obj.common.custom?.[this.namespace]?.aliasId) {
                        if (obj.common.custom[this.namespace].aliasId !== id) {
                            this.aliasMap[id] = obj.common.custom[this.namespace].aliasId;
                            this.log.debug(`Registered Alias: ${id} --> ${this.aliasMap[id]}`);
                            id = this.aliasMap[id];
                            checkForRemove = false;
                        } else {
                            this.log.warn(`Ignoring Alias-ID because identical to ID for ${id}`);
                            obj.common.custom[this.namespace].aliasId = '';
                        }
                    }

                    if (checkForRemove && this.aliasMap[id]) {
                        this.log.debug(`Removed Alias: ${id} !-> ${this.aliasMap[id]}`);
                        delete this.aliasMap[id];
                    }

                    if (!this.sqlDPs[formerAliasId]?.config && !this.subscribeAll) {
                        // un-subscribe
                        for (const _id in this.sqlDPs) {
                            if (
                                Object.prototype.hasOwnProperty.call(this.sqlDPs, _id) &&
                                Object.prototype.hasOwnProperty.call(this.sqlDPs, this.sqlDPs[_id].realId)
                            ) {
                                this.unsubscribeForeignStates(this.sqlDPs[_id].realId);
                            }
                        }
                        this.subscribeAll = true;
                        this.subscribeForeignStates('*');
                    }

                    if (this.sqlDPs[id] && this.sqlDPs[id].index === undefined) {
                        this.getId(id, this.sqlDPs[id].dbType, () =>
                            this.reInit(id, realId, formerAliasId, obj as ioBroker.StateObject),
                        );
                    } else {
                        this.reInit(id, realId, formerAliasId, obj as ioBroker.StateObject);
                    }
                } else {
                    if (this.aliasMap[id]) {
                        this.log.debug(`Removed Alias: ${id} !-> ${this.aliasMap[id]}`);
                        delete this.aliasMap[id];
                    }
                    const sqlDP = this.sqlDPs[id];

                    id = formerAliasId;

                    if (sqlDP?.config) {
                        this.log.info(`disabled logging of ${id}`);
                        if (sqlDP.relogTimeout) {
                            clearTimeout(sqlDP.relogTimeout);
                            sqlDP.relogTimeout = null;
                        }
                        if (sqlDP.timeout) {
                            clearTimeout(sqlDP.timeout);
                            sqlDP.timeout = null;
                        }

                        if (sqlDP.state) {
                            tmpState = { ...sqlDP.state };
                        }
                        const state = sqlDP.state ? tmpState || null : null;

                        if (sqlDP.config) {
                            sqlDP.config.enabled = false;
                            if (sqlDP.skipped && !sqlDP.config.disableSkippedValueLogging) {
                                this.pushValueIntoDB(id, sqlDP.skipped, false, true);
                                sqlDP.skipped = null;
                            }

                            if (this.config.writeNulls) {
                                const nullValue: IobDataEntryEx = {
                                    val: null,
                                    ts: now,
                                    lc: now,
                                    q: 0x40,
                                    from: `system.adapter.${this.namespace}`,
                                    ack: true,
                                };

                                if (sqlDP.config.changesOnly && state && state.val !== null) {
                                    ((_id, _state, _nullValue) => {
                                        _state.ts = now;
                                        _state.from = `system.adapter.${this.namespace}`;
                                        nullValue.ts += 4;
                                        nullValue.lc += 4; // because of MS SQL
                                        this.log.debug(`Write 1/2 "${_state.val}" _id: ${_id}`);
                                        this.pushValueIntoDB(_id, _state, false, true, () => {
                                            // terminate values with null to indicate adapter stop. timestamp + 1
                                            this.log.debug(`Write 2/2 "null" _id: ${_id}`);
                                            this.pushValueIntoDB(
                                                _id,
                                                _nullValue,
                                                false,
                                                false,
                                                () => delete this.sqlDPs[id],
                                            );
                                        });
                                    })(id, state, nullValue);
                                } else {
                                    // terminate values with null to indicate adapter stop. timestamp + 1
                                    this.log.debug(`Write 0 NULL _id: ${id}`);
                                    this.pushValueIntoDB(id, nullValue, false, false, () => delete this.sqlDPs[id]);
                                }
                            } else {
                                this.storeCached(id, () => delete this.sqlDPs[id]);
                            }
                        } else {
                            delete this.sqlDPs[id];
                        }
                    }
                }
            },
            unload: (callback: () => void): void => {
                void this.finish(callback);
            },
        });
    }

    borrowClientFromPool(callback: (err: Error | null | undefined, client?: SQLClient) => void): void {
        if (!this.clientPool) {
            this.setConnected(false);
            return callback(new Error('No database connection'));
        }
        this.setConnected(true);

        if (this.activeConnections >= this.config.maxConnections) {
            if (this.logConnectionUsage) {
                this.log.debug(`Borrow connection not possible: ${this.activeConnections} >= Max - Store for Later`);
            }
            this.poolBorrowGuard.push(callback);
            return;
        }
        this.activeConnections++;
        if (this.logConnectionUsage) {
            this.log.debug(`Borrow connection from pool: ${this.activeConnections} now`);
        }
        this.clientPool.borrow((err: Error | null | undefined, client?: SQLClient): void => {
            if (!err && client) {
                // make sure we always have at least one error listener to prevent crashes
                if (client.on && client.listenerCount && !client.listenerCount('error')) {
                    client.on('error', (err: string): void => this.log.warn(`SQL client error: ${err}`));
                }
            } else if (!client) {
                this.activeConnections--;
            }

            callback(err, client);
        });
    }

    returnClientToPool(client?: SQLClient): void {
        if (client) {
            this.activeConnections--;
        }
        if (this.logConnectionUsage) {
            this.log.debug(`Return connection to pool: ${this.activeConnections} now`);
        }
        if (this.clientPool && client) {
            if (this.poolBorrowGuard.length) {
                this.activeConnections++;
                if (this.logConnectionUsage) {
                    this.log.debug(`Borrow returned connection directly: ${this.activeConnections} now`);
                }
                const callback = this.poolBorrowGuard.shift()!;
                callback(null, client);
            } else {
                try {
                    this.clientPool.return(client);
                } catch {
                    // Ignore
                }
            }
        }
    }

    normalizeCustomConfig(customConfig: SqlCustomConfig): SqlCustomConfigTyped {
        // maxLength
        if (!customConfig.maxLength && customConfig.maxLength !== '0' && customConfig.maxLength !== 0) {
            customConfig.maxLength = this.config.maxLength || 0;
        } else {
            customConfig.maxLength = parseInt(customConfig.maxLength as string, 10);
        }

        // retention
        if (customConfig.retention || customConfig.retention === 0) {
            customConfig.retention = parseInt(customConfig.retention as string, 10) || 0;
        } else {
            customConfig.retention = this.config.retention;
        }
        if (customConfig.retention === -1) {
            // customRetentionDuration
            if (
                customConfig.customRetentionDuration !== undefined &&
                customConfig.customRetentionDuration !== null &&
                customConfig.customRetentionDuration !== ''
            ) {
                customConfig.customRetentionDuration =
                    parseInt(customConfig.customRetentionDuration as string, 10) || 0;
            } else {
                customConfig.customRetentionDuration = this.config.customRetentionDuration;
            }
            customConfig.retention = customConfig.customRetentionDuration * 24 * 60 * 60;
        }

        // debounceTime and debounce compatibility handling
        if (!customConfig.blockTime && customConfig.blockTime !== '0' && customConfig.blockTime !== 0) {
            if (!customConfig.debounce && customConfig.debounce !== '0' && customConfig.debounce !== 0) {
                customConfig.blockTime = this.config.blockTime || 0;
            } else {
                customConfig.blockTime = parseInt(customConfig.debounce as string, 10) || 0;
            }
        } else {
            customConfig.blockTime = parseInt(customConfig.blockTime as string, 10) || 0;
        }
        if (!customConfig.debounceTime && customConfig.debounceTime !== '0' && customConfig.debounceTime !== 0) {
            customConfig.debounceTime = this.config.debounceTime || 0;
        } else {
            customConfig.debounceTime = parseInt(customConfig.debounceTime as string, 10) || 0;
        }

        // changesOnly
        customConfig.changesOnly = customConfig.changesOnly === true || customConfig.changesOnly === 'true';

        // ignoreZero
        customConfig.ignoreZero = customConfig.ignoreZero === true || customConfig.ignoreZero === 'true';

        // round
        if (customConfig.round !== null && customConfig.round !== undefined && customConfig.round !== '') {
            customConfig.round = parseInt(customConfig.round as string, 10);
            if (!isFinite(customConfig.round) || customConfig.round < 0) {
                customConfig.round = this.config.round as number;
            } else {
                customConfig.round = Math.pow(10, parseInt(customConfig.round as unknown as string, 10));
            }
        } else {
            customConfig.round = this.config.round as number;
        }

        // ignoreAboveNumber
        if (
            customConfig.ignoreAboveNumber !== undefined &&
            customConfig.ignoreAboveNumber !== null &&
            customConfig.ignoreAboveNumber !== ''
        ) {
            customConfig.ignoreAboveNumber = parseFloat(customConfig.ignoreAboveNumber as string) || null;
        }

        // ignoreBelowNumber incl. ignoreBelowZero compatibility handling
        if (
            customConfig.ignoreBelowNumber !== undefined &&
            customConfig.ignoreBelowNumber !== null &&
            customConfig.ignoreBelowNumber !== ''
        ) {
            customConfig.ignoreBelowNumber = parseFloat(customConfig.ignoreBelowNumber as string) || null;
        } else if (customConfig.ignoreBelowZero === 'true' || customConfig.ignoreBelowZero === true) {
            customConfig.ignoreBelowNumber = 0;
        }

        // disableSkippedValueLogging
        if (
            customConfig.disableSkippedValueLogging !== undefined &&
            customConfig.disableSkippedValueLogging !== null &&
            (customConfig.disableSkippedValueLogging as any) !== ''
        ) {
            customConfig.disableSkippedValueLogging =
                customConfig.disableSkippedValueLogging === 'true' || customConfig.disableSkippedValueLogging === true;
        } else {
            customConfig.disableSkippedValueLogging = this.config.disableSkippedValueLogging;
        }

        // enableDebugLogs
        if (
            customConfig.enableDebugLogs !== undefined &&
            customConfig.enableDebugLogs !== null &&
            (customConfig.enableDebugLogs as any) !== ''
        ) {
            customConfig.enableDebugLogs =
                customConfig.enableDebugLogs === 'true' || customConfig.enableDebugLogs === true;
        } else {
            customConfig.enableDebugLogs = this.config.enableDebugLogs;
        }

        // changesRelogInterval
        if (customConfig.changesRelogInterval || customConfig.changesRelogInterval === 0) {
            customConfig.changesRelogInterval = parseInt(customConfig.changesRelogInterval as string, 10) || 0;
        } else {
            customConfig.changesRelogInterval = this.config.changesRelogInterval;
        }

        // changesMinDelta
        if (customConfig.changesMinDelta || customConfig.changesMinDelta === 0) {
            customConfig.changesMinDelta = parseFloat(customConfig.changesMinDelta.toString().replace(/,/g, '.')) || 0;
        } else {
            customConfig.changesMinDelta = this.config.changesMinDelta;
        }

        // storageType
        if (!customConfig.storageType) {
            customConfig.storageType = false;
        }

        // add one day if retention is too small
        if (customConfig.retention && customConfig.retention <= 604800) {
            customConfig.retention += 86400;
        }
        return customConfig as SqlCustomConfigTyped;
    }

    reInit(id: string, realId: string, formerAliasId: string, obj: ioBroker.StateObject): void {
        const customConfig = this.normalizeCustomConfig(obj.common.custom?.[this.namespace]);
        if (this.sqlDPs[formerAliasId]?.config && isEqual(customConfig, this.sqlDPs[formerAliasId].config)) {
            if (obj.common.custom?.[this.namespace].enableDebugLogs) {
                this.log.debug(`Object ${id} unchanged. Ignore`);
            }
            return;
        }

        // relogTimeout
        if (this.sqlDPs[formerAliasId] && this.sqlDPs[formerAliasId].relogTimeout) {
            clearTimeout(this.sqlDPs[formerAliasId].relogTimeout);
            this.sqlDPs[formerAliasId].relogTimeout = null;
        }

        const writeNull = !this.sqlDPs[id]?.config;

        this.sqlDPs[id] = {
            config: customConfig,
            state: this.sqlDPs[id]?.state || null,
            list: this.sqlDPs[id]?.list || [],
            inFlight: this.sqlDPs[id]?.inFlight || {},
            timeout: this.sqlDPs[id]?.timeout || null,
            ts: this.sqlDPs[id]?.ts || null,
            lastLogTime: 0,
            relogTimeout: null,
            realId: realId,
            lastCheck: this.sqlDPs[id]?.lastCheck || Date.now() - Math.floor(Math.random() * 21600000 /* 6 hours */), // randomize lastCheck to avoid all datapoints to be checked at same timepoint
        } as SQLPointConfig;

        // changesRelogInterval
        if (this.sqlDPs[id].config.changesOnly && this.sqlDPs[id].config.changesRelogInterval > 0) {
            this.sqlDPs[id].relogTimeout = setTimeout(
                (_id: string) => this.reLogHelper(_id),
                this.sqlDPs[id].config.changesRelogInterval * 500 * (1 + Math.random()),
                id,
            );
        }

        if (writeNull && this.config.writeNulls) {
            this.writeNulls(id);
        }

        this.log.info(`enabled logging of ${id}, Alias=${id !== realId}, WriteNulls=${writeNull}`);
    }

    setConnected(isConnected: boolean): void {
        if (this.sqlConnected !== isConnected) {
            this.sqlConnected = isConnected;
            this.setState('info.connection', this.sqlConnected, true);
        }
    }

    connect(callback: (err?: Error | null) => void): void {
        if (this.reconnectTimeout) {
            clearTimeout(this.reconnectTimeout);
            this.reconnectTimeout = null;
        }

        if (!this.clientPool) {
            this.setConnected(false);
            let sqLiteOptions: SQLite3Options | undefined;
            let msSQLOptions: MSSQLOptions | undefined;
            let mySQLOptions: MySQLOptions | undefined;
            let postgreSQLOptions: PostgreSQLOptions | undefined;

            const poolOptions: PoolConfig = {
                max_idle: this.config.dbtype === 'sqlite' ? 1 : 2,
            };

            if (this.config.maxConnections) {
                poolOptions.max_active = this.config.maxConnections + 1; // we use our own Pool limiter logic, so let library have one more to not block us too early
                poolOptions.max_wait = 10000; // hard code for now
                poolOptions.when_exhausted = 'block';
            }

            if (!this.config.dbtype) {
                return this.log.error('DB Type is not defined!');
            }
            if (!clients[this.config.dbtype]) {
                return this.log.error(`Unknown type "${this.config.dbtype}"`);
            }

            if (this.config.dbtype === 'postgresql') {
                postgreSQLOptions = {
                    host: this.config.host,
                    user: this.config.user || '',
                    password: this.config.password || '',
                    port: this.config.port || undefined,
                    database: 'postgres',
                    ssl: this.config.encrypt
                        ? {
                              rejectUnauthorized: !!this.config.rejectUnauthorized,
                          }
                        : undefined,
                };
            } else if (this.config.dbtype === 'mssql') {
                msSQLOptions = {
                    server: this.config.host, // needed for MSSQL
                    user: this.config.user || '',
                    password: this.config.password || '',
                    port: this.config.port || undefined,
                    options: {
                        encrypt: !!this.config.encrypt,
                        trustServerCertificate: !this.config.rejectUnauthorized,
                    },
                };
            } else if (this.config.dbtype === 'mysql') {
                mySQLOptions = {
                    host: this.config.host, // needed for PostgreSQL , MySQL
                    user: this.config.user || '',
                    password: this.config.password || '',
                    port: this.config.port || undefined,
                    ssl: this.config.encrypt
                        ? {
                              rejectUnauthorized: !!this.config.rejectUnauthorized,
                          }
                        : undefined,
                };
            } else if (this.config.dbtype === 'sqlite') {
                sqLiteOptions = { fileName: this.getSqlLiteDir(this.config.fileName) };
            }

            if (this.config.dbtype === 'postgresql' && !this.postgresDbCreated && postgreSQLOptions) {
                // special solution for postgres. Connect first to Db "postgres", create new DB "iobroker" and then connect to "iobroker" DB.
                // connect first to DB postgres and create iobroker DB
                this.log.info(
                    `Postgres connection options: ${JSON.stringify(postgreSQLOptions).replace((postgreSQLOptions.password as string) || '******', '****')}`,
                );
                const _client: SQLClient = new PostgreSQLClient(postgreSQLOptions);
                _client.on?.('error', (err: Error | null): void => this.log.warn(`SQL client error: ${err}`));

                return _client.connect((err?: Error | null): void => {
                    if (err) {
                        this.log.error(err.toString());
                        if (this.reconnectTimeout) {
                            clearTimeout(this.reconnectTimeout);
                        }
                        this.reconnectTimeout = setTimeout(() => {
                            this.reconnectTimeout = null;
                            this.connect(callback);
                        }, 30000);
                        return;
                    }

                    if (this.config.doNotCreateDatabase) {
                        _client.disconnect();
                        this.postgresDbCreated = true;
                        this.reconnectTimeout && clearTimeout(this.reconnectTimeout);
                        this.reconnectTimeout = setTimeout(() => {
                            this.reconnectTimeout = null;
                            this.connect(callback);
                        }, 100);
                    } else {
                        _client.execute(`CREATE DATABASE ${this.config.dbname};`, (err: Error | null): void => {
                            _client.disconnect();
                            const typedError: {
                                code: string;
                            } = err as any;
                            if (typedError && typedError.code !== '42P04') {
                                // if error not about yet exists
                                this.postgresDbCreated = false;
                                this.log.error(JSON.stringify(typedError));
                                this.reconnectTimeout && clearTimeout(this.reconnectTimeout);
                                this.reconnectTimeout = setTimeout(() => {
                                    this.reconnectTimeout = null;
                                    this.connect(callback);
                                }, 30000);
                            } else {
                                // remember that DB is created
                                this.postgresDbCreated = true;
                                this.reconnectTimeout && clearTimeout(this.reconnectTimeout);
                                this.reconnectTimeout = setTimeout(() => {
                                    this.reconnectTimeout = null;
                                    this.connect(callback);
                                }, 100);
                            }
                        });
                    }
                });
            }

            if (this.config.dbtype === 'postgresql' && postgreSQLOptions) {
                postgreSQLOptions.database = this.config.dbname;
            }

            try {
                if (this.config.dbtype === 'mssql' && msSQLOptions) {
                    this.clientPool = new MSSQLClientPool(poolOptions, msSQLOptions);
                } else if (this.config.dbtype === 'mysql' && mySQLOptions) {
                    this.clientPool = new MySQL2ClientPool(poolOptions, mySQLOptions);
                } else if (this.config.dbtype === 'sqlite' && sqLiteOptions) {
                    this.clientPool = new SQLite3ClientPool(poolOptions, sqLiteOptions);
                } else if (this.config.dbtype === 'postgresql' && postgreSQLOptions) {
                    this.clientPool = new PostgreSQLClientPool(poolOptions, postgreSQLOptions);
                } else {
                    throw new Error('DB connection options not defined');
                }

                return this.clientPool.open(poolOptions, (err?: Error | null): void => {
                    this.activeConnections = 0;
                    if (err) {
                        this.clientPool = null;
                        this.setConnected(false);
                        this.log.error(JSON.stringify(err));
                        this.reconnectTimeout && clearTimeout(this.reconnectTimeout);
                        this.reconnectTimeout = setTimeout(() => {
                            this.reconnectTimeout = null;
                            this.connect(callback);
                        }, 30000);
                    } else {
                        if (this.reconnectTimeout) {
                            clearTimeout(this.reconnectTimeout);
                        }
                        setImmediate(() => this.connect(callback));
                    }
                });
            } catch (ex) {
                if (ex.toString() === 'TypeError: undefined is not a function') {
                    this.log.error(`Node.js DB driver for "${this.config.dbtype}" could not be installed.`);
                } else {
                    this.log.error(ex.toString());
                    this.log.error(ex.stack);
                }
                this.clientPool = null;
                this.activeConnections = 0;
                this.setConnected(false);
                this.reconnectTimeout && clearTimeout(this.reconnectTimeout);
                this.reconnectTimeout = setTimeout(() => {
                    this.reconnectTimeout = null;
                    this.connect(callback);
                }, 30000);
                return;
            }
        }

        this.allScripts(this.sqlFuncs!.init(this.config.dbname, this.config.doNotCreateDatabase), 0, err => {
            if (err) {
                //this.log.error(err);
                this.reconnectTimeout && clearTimeout(this.reconnectTimeout);
                this.reconnectTimeout = setTimeout(() => {
                    this.reconnectTimeout = null;
                    this.connect(callback);
                }, 30000);
            } else {
                this.log.info(`Connected to ${this.config.dbtype}`);
                // read all DB IDs and all FROM ids
                this.getAllIds(() => this.getAllFroms(callback));
            }
        });
    }

    // Find sqlite data directory
    getSqlLiteDir(fileName: string): string {
        fileName ||= 'sqlite.db';
        fileName = fileName.replace(/\\/g, '/');
        if (fileName[0] === '/' || fileName.match(/^\w:\//)) {
            return fileName;
        }
        // normally /opt/iobroker/node_modules/iobroker.js-controller
        // but can be /example/ioBroker.js-controller
        const config = join(getAbsoluteDefaultDataDir(), 'sqlite');

        // create sqlite directory
        if (!existsSync(config)) {
            mkdirSync(config);
        }

        return normalize(join(config, fileName));
    }

    testConnection(msg: ioBroker.Message): void {
        if (!msg.message.config) {
            if (msg.callback) {
                this.sendTo(msg.from, msg.command, { error: 'invalid config' }, msg.callback);
            }
            return;
        }
        let sqLiteOptions: SQLite3Options | undefined;
        let msSQLOptions: MSSQLOptions | undefined;
        let mySQLOptions: MySQLOptions | undefined;
        let postgreSQLOptions: PostgreSQLOptions | undefined;

        const config: SqlAdapterConfig = msg.message.config;

        config.port = parseInt(config.port as string, 10) || 0;

        if (config.dbtype === 'postgresql') {
            postgreSQLOptions = {
                host: config.host,
                user: config.user || '',
                password: config.password || '',
                port: config.port || undefined,
                database: 'postgres',
                ssl: config.encrypt
                    ? {
                          rejectUnauthorized: !!config.rejectUnauthorized,
                      }
                    : undefined,
            };
        } else if (config.dbtype === 'mssql') {
            msSQLOptions = {
                server: config.host, // needed for MSSQL
                user: config.user || '',
                password: config.password || '',
                port: config.port || undefined,
                options: {
                    encrypt: !!config.encrypt,
                    trustServerCertificate: !config.rejectUnauthorized,
                },
            };
        } else if (config.dbtype === 'mysql') {
            mySQLOptions = {
                host: config.host, // needed for PostgreSQL , MySQL
                user: config.user || '',
                password: config.password || '',
                port: config.port || undefined,
                ssl: config.encrypt
                    ? {
                          rejectUnauthorized: !!config.rejectUnauthorized,
                      }
                    : undefined,
            };
        } else if (config.dbtype === 'sqlite') {
            sqLiteOptions = { fileName: this.getSqlLiteDir(config.fileName) };
        }

        try {
            let client: SQLClient | undefined;
            if (config.dbtype === 'postgresql' && postgreSQLOptions) {
                client = new PostgreSQLClient(postgreSQLOptions);
            } else if (config.dbtype === 'mssql' && msSQLOptions) {
                client = new MSSQLClient(msSQLOptions);
            } else if (config.dbtype === 'mysql' && mySQLOptions) {
                client = new MySQL2Client(mySQLOptions);
            } else if (config.dbtype === 'sqlite' && sqLiteOptions) {
                client = new SQLite3Client(sqLiteOptions);
            } else {
                this.sendTo(msg.from, msg.command, { error: 'Unknown DB type' }, msg.callback);
                return;
            }
            client.on?.('error', (err?: string): void => this.log.warn(`SQL client error: ${err}`));

            this.testConnectTimeout = setTimeout(() => {
                this.testConnectTimeout = null;
                this.sendTo(msg.from, msg.command, { error: 'connect timeout' }, msg.callback);
            }, 5000);

            client.connect((err?: Error | null): void => {
                if (err) {
                    if (this.testConnectTimeout) {
                        clearTimeout(this.testConnectTimeout);
                        this.testConnectTimeout = null;
                    }
                    this.sendTo(
                        msg.from,
                        msg.command,
                        { error: `${(err as any).code} ${err.toString()}` },
                        msg.callback,
                    );
                    return;
                }

                client.execute('SELECT 2 + 3 AS x', (err?: Error | null /* , rows, fields */) => {
                    client.disconnect();

                    if (this.testConnectTimeout) {
                        clearTimeout(this.testConnectTimeout);
                        this.testConnectTimeout = null;
                        this.sendTo(msg.from, msg.command, { error: err?.toString() || null }, msg.callback);
                        return;
                    }
                });
            });
        } catch (ex) {
            if (this.testConnectTimeout) {
                clearTimeout(this.testConnectTimeout);
                this.testConnectTimeout = null;
            }
            if (ex.toString() === 'TypeError: undefined is not a function') {
                this.sendTo(
                    msg.from,
                    msg.command,
                    { error: 'Node.js DB driver could not be installed.' },
                    msg.callback,
                );
            } else {
                this.sendTo(msg.from, msg.command, { error: ex.toString() }, msg.callback);
            }
        }
    }

    destroyDB(msg: ioBroker.Message): void {
        try {
            this.allScripts(this.sqlFuncs!.destroy(this.config.dbname), 0, err => {
                if (err) {
                    this.log.error(err.toString());
                    this.sendTo(msg.from, msg.command, { error: err.toString() }, msg.callback);
                } else {
                    this.sendTo(msg.from, msg.command, { error: null, result: 'deleted' }, msg.callback);
                    // restart adapter
                    setTimeout(
                        () =>
                            this.getForeignObject(`system.adapter.${this.namespace}`, (err, obj) => {
                                if (!err && obj) {
                                    void this.setForeignObject(obj._id, obj);
                                } else {
                                    this.log.error(
                                        `Cannot read object "system.adapter.${this.namespace}": ${err || 'Config does not exist'}`,
                                    );
                                    this.stop ? void this.stop() : void this.terminate();
                                }
                            }),
                        2000,
                    );
                }
            });
        } catch (ex) {
            return this.sendTo(msg.from, msg.command, { error: ex.toString() }, msg.callback);
        }
    }

    _userQuery(msg: ioBroker.Message, callback?: () => void): void {
        try {
            if (typeof msg.message !== 'string' || !msg.message.length) {
                throw new Error('No query provided');
            }
            this.log.debug(msg.message);

            this.borrowClientFromPool((err, client) => {
                if (err || !client) {
                    this.sendTo(msg.from, msg.command, { error: err?.toString() || 'No client' }, msg.callback);
                    this.returnClientToPool(client);
                    callback?.();
                } else {
                    client.execute<any>(msg.message, (err, rows /* , fields */) => {
                        this.returnClientToPool(client);
                        //convert ts for postgresql and ms sqlserver
                        if (!err && rows?.[0] && typeof rows[0].ts === 'string') {
                            for (let i = 0; i < rows.length; i++) {
                                rows[i].ts = parseInt(rows[i].ts, 10);
                            }
                        }
                        this.sendTo(
                            msg.from,
                            msg.command,
                            { error: err ? err.toString() : null, result: rows },
                            msg.callback,
                        );
                        callback?.();
                    });
                }
            });
        } catch (err) {
            this.sendTo(msg.from, msg.command, { error: err.toString() }, msg.callback);
            callback?.();
        }
    }

    // execute custom query
    query(msg: ioBroker.Message): void {
        if (!this.multiRequests) {
            if (this.tasks.length > MAX_TASKS) {
                const error = `Cannot queue new requests, because more than ${MAX_TASKS}`;
                this.log.error(error);
                this.sendTo(msg.from, msg.command, { error }, msg.callback);
            } else {
                this.tasks.push({ operation: 'userQuery', msg });
                if (this.tasks.length === 1) {
                    this.processTasks();
                }
            }
        } else {
            this._userQuery(msg);
        }
    }

    // one script
    oneScript(script: string, cb?: (err?: Error | null) => void): void {
        try {
            this.borrowClientFromPool((err, client) => {
                if (err || !client) {
                    this.clientPool?.close();
                    this.activeConnections = 0;
                    this.clientPool = null;
                    this.setConnected(false);
                    this.log.error(err?.toString() || 'No database connection');
                    return cb?.(err || new Error('No database connection'));
                }

                this.log.debug(script);

                client.execute(script, (err?: Error | null): void => {
                    this.log.debug(`Response: ${JSON.stringify(err)}`);
                    if (err) {
                        const typedError: {
                            number?: number;
                            errno?: number;
                            code?: string;
                            message?: string;
                        } = err as any;
                        // Database 'iobroker' already exists. Choose a different database name.
                        if (
                            typedError.number === 1801 ||
                            // There is already an object named 'sources' in the database.
                            typedError.number === 2714
                        ) {
                            // do nothing
                            err = null;
                        } else if (typedError.message?.match(/^SQLITE_ERROR: table [\w_]+ already exists$/)) {
                            // do nothing
                            err = null;
                        } else if (typedError.errno == 1007 || typedError.errno == 1050) {
                            // if a database exists or table exists,
                            // do nothing
                            err = null;
                        } else if (typedError.code === '42P04') {
                            // if a database exists or table exists,
                            // do nothing
                            err = null;
                        } else if (typedError.code === '42P07') {
                            const match = script.match(/CREATE\s+TABLE\s+(\w*)\s+\(/);
                            if (match) {
                                this.log.debug(`OK. Table "${match[1]}" yet exists`);
                                err = null;
                            } else {
                                this.log.error(script);
                                this.log.error(err.toString());
                            }
                        } else if (script.startsWith('CREATE INDEX')) {
                            this.log.info('Ignore Error on Create index. You might want to create the index yourself!');
                            this.log.info(script);
                            this.log.info(`${typedError.code}: ${err}`);
                            err = null;
                        } else {
                            this.log.error(script);
                            this.log.error(err.toString());
                        }
                    }
                    this.returnClientToPool(client);
                    cb?.(err);
                });
            });
        } catch (ex) {
            this.log.error(ex);
            cb?.(ex);
        }
    }

    // all scripts
    allScripts(scripts: string[], index: number, cb: (err?: Error | null) => void): void {
        index ||= 0;

        if (scripts && index < scripts.length) {
            this.oneScript(scripts[index], err => {
                if (err) {
                    cb?.(err);
                } else {
                    this.allScripts(scripts, index + 1, cb);
                }
            });
        } else {
            cb?.();
        }
    }

    finish(callback: () => void): void {
        let count = 0;
        const now = Date.now();

        const allFinished = (): void => {
            if (this.clientPool) {
                this.clientPool.close();
                this.activeConnections = 0;
                this.clientPool = null;
                this.setConnected(false);
            }
            if (typeof this.finished === 'object') {
                setTimeout(
                    (cb: (() => void)[]): void => {
                        for (let f = 0; f < cb.length; f++) {
                            typeof cb[f] === 'function' && cb[f]();
                        }
                    },
                    500,
                    this.finished,
                );
                this.finished = true;
            }
        };

        const finishId = (id: string): void => {
            if (!this.sqlDPs[id]) {
                return;
            }
            if (this.sqlDPs[id].relogTimeout) {
                clearTimeout(this.sqlDPs[id].relogTimeout);
                this.sqlDPs[id].relogTimeout = null;
            }
            if (this.sqlDPs[id].timeout) {
                clearTimeout(this.sqlDPs[id].timeout);
                this.sqlDPs[id].timeout = null;
            }
            const state: IobDataEntryEx | null = this.sqlDPs[id].state ? { ...this.sqlDPs[id].state } : null;

            if (
                this.sqlDPs[id].skipped &&
                !(this.sqlDPs[id].config && this.sqlDPs[id].config.disableSkippedValueLogging)
            ) {
                count++;
                this.pushValueIntoDB(id, this.sqlDPs[id].skipped, false, true, () => {
                    if (!--count) {
                        this.pushValuesIntoDB(id, this.sqlDPs[id].list, () => {
                            allFinished();
                        });
                    }
                });
                this.sqlDPs[id].skipped = null;
            }

            const nullValue: IobDataEntryEx = {
                ack: true,
                val: null,
                ts: now,
                lc: now,
                q: 0x40,
                from: `system.adapter.${this.namespace}`,
            };

            if (this.sqlDPs[id].config && this.config.writeNulls) {
                if (this.sqlDPs[id].config.changesOnly && state && state.val !== null) {
                    count++;
                    ((_id: string, _state: IobDataEntryEx, _nullValue: IobDataEntryEx): void => {
                        _state.ts = now;
                        _state.from = `system.adapter.${this.namespace}`;
                        nullValue.ts += 4;
                        nullValue.lc += 4; // because of MS SQL
                        this.log.debug(`Write 1/2 "${_state.val}" _id: ${_id}`);
                        this.pushValueIntoDB(_id, _state, false, true, () => {
                            // terminate values with null to indicate adapter stop. timestamp + 1
                            this.log.debug(`Write 2/2 "null" _id: ${_id}`);
                            this.pushValueIntoDB(_id, _nullValue, false, true, () => {
                                if (!--count) {
                                    this.pushValuesIntoDB(id, this.sqlDPs[id].list, () => {
                                        allFinished();
                                    });
                                }
                            });
                        });
                    })(id, state, nullValue);
                } else {
                    // terminate values with null to indicate adapter stop. timestamp + 1
                    count++;
                    this.log.debug(`Write 0 NULL _id: ${id}`);
                    this.pushValueIntoDB(id, nullValue, false, true, () => {
                        if (!--count) {
                            this.pushValuesIntoDB(id, this.sqlDPs[id].list, () => {
                                allFinished();
                            });
                        }
                    });
                }
            }
        };

        if (!this.subscribeAll) {
            for (const _id in this.sqlDPs) {
                if (
                    Object.prototype.hasOwnProperty.call(this.sqlDPs, _id) &&
                    Object.prototype.hasOwnProperty.call(this.sqlDPs, this.sqlDPs[_id].realId)
                ) {
                    this.unsubscribeForeignStates(this.sqlDPs[_id].realId);
                }
            }
        } else {
            this.subscribeAll = false;
            this.unsubscribeForeignStates('*');
        }

        if (this.reconnectTimeout) {
            clearTimeout(this.reconnectTimeout);
            this.reconnectTimeout = null;
        }
        if (this.testConnectTimeout) {
            clearTimeout(this.testConnectTimeout);
            this.testConnectTimeout = null;
        }
        if (this.dpOverviewTimeout) {
            clearTimeout(this.dpOverviewTimeout);
            this.dpOverviewTimeout = null;
        }
        if (this.bufferChecker) {
            clearInterval(this.bufferChecker);
            this.bufferChecker = null;
        }

        if (this.finished) {
            if (callback) {
                if (this.finished === true) {
                    callback();
                } else {
                    this.finished.push(callback);
                }
            }
            return;
        }
        this.finished = [callback];
        let dpcount = 0;
        let delay = 0;
        for (const id in this.sqlDPs) {
            if (!Object.prototype.hasOwnProperty.call(this.sqlDPs, id)) {
                continue;
            }
            dpcount++;
            delay += dpcount % 50 === 0 ? 1000 : 0;
            setTimeout(finishId, delay, id);
        }

        if (!dpcount && callback) {
            if (this.clientPool) {
                this.clientPool.close();
                this.activeConnections = 0;
                this.clientPool = null;
                this.setConnected(false);
            }
            callback();
        }
    }

    processMessage(msg: ioBroker.Message): void {
        if (msg.command === 'features') {
            this.sendTo(
                msg.from,
                msg.command,
                { supportedFeatures: ['update', 'delete', 'deleteRange', 'deleteAll', 'storeState'] },
                msg.callback,
            );
        } else if (msg.command === 'getHistory') {
            this.getHistorySql(msg);
        } else if (msg.command === 'getCounter') {
            this.getCounterDiff(msg);
        } else if (msg.command === 'test') {
            this.testConnection(msg);
        } else if (msg.command === 'destroy') {
            this.destroyDB(msg);
        } else if (msg.command === 'query') {
            this.query(msg);
        } else if (msg.command === 'update') {
            this.updateState(msg);
        } else if (msg.command === 'delete') {
            this.deleteHistoryEntry(msg);
        } else if (msg.command === 'deleteAll') {
            this.deleteStateAll(msg);
        } else if (msg.command === 'deleteRange') {
            this.deleteHistoryEntry(msg);
        } else if (msg.command === 'storeState') {
            this.storeState(msg).catch(e => this.log.error(`Cannot store state: ${e}`));
        } else if (msg.command === 'getDpOverview') {
            this.getDpOverview(msg);
        } else if (msg.command === 'enableHistory') {
            this.enableHistory(msg);
        } else if (msg.command === 'disableHistory') {
            this.disableHistory(msg);
        } else if (msg.command === 'getEnabledDPs') {
            this.getEnabledDPs(msg);
        } else if (msg.command === 'stopInstance') {
            this.finish(() => {
                if (msg.callback) {
                    this.sendTo(msg.from, msg.command, 'stopped', msg.callback);
                    setTimeout(() => (this.stop ? this.stop() : this.terminate?.()), 200);
                }
            });
        }
    }

    processStartValues(callback?: () => void): void {
        if (this.tasksStart?.length) {
            const task = this.tasksStart.shift()!;
            const sqlDP = this.sqlDPs[task.id];
            if (sqlDP.config.changesOnly) {
                this.getForeignState(sqlDP.realId, (err, state) => {
                    const now = task.now || Date.now();
                    this.pushHistory(task.id, {
                        val: null,
                        ts: state ? now - 4 : now, // 4 is because of MS SQL
                        lc: state ? now - 4 : now, // 4 is because of MS SQL
                        ack: true,
                        q: 0x40,
                        from: `system.adapter.${this.namespace}`,
                    });

                    if (state) {
                        state.ts = now;
                        state.lc = now;
                        state.from = `system.adapter.${this.namespace}`;
                        this.pushHistory(task.id, state as IobDataEntryEx);
                    }

                    setImmediate(() => this.processStartValues());
                });
            } else {
                const now = Date.now();
                this.pushHistory(task.id, {
                    val: null,
                    ts: task.now || now,
                    lc: task.now || now,
                    ack: true,
                    q: 0x40,
                    from: `system.adapter.${this.namespace}`,
                });

                setImmediate(() => this.processStartValues());
            }
            if (sqlDP.config?.changesOnly && sqlDP.config.changesRelogInterval > 0) {
                if (sqlDP.relogTimeout) {
                    clearTimeout(sqlDP.relogTimeout);
                }
                sqlDP.relogTimeout = setTimeout(
                    (id: string) => this.reLogHelper(id),
                    sqlDP.config.changesRelogInterval * 500 * Math.random() + sqlDP.config.changesRelogInterval * 500,
                    task.id,
                );
            }
        } else {
            callback && callback();
        }
    }

    writeNulls(id?: string, now?: number): void {
        if (!id) {
            now = Date.now();

            Object.keys(this.sqlDPs)
                .filter(_id => this.sqlDPs[_id] && this.sqlDPs[_id].config)
                .forEach(_id => this.writeNulls(_id, now));
        } else {
            now ||= Date.now();

            this.tasksStart.push({ id, now });

            if (this.tasksStart.length === 1 && this.sqlConnected) {
                this.processStartValues();
            }
        }
    }

    pushHistory(id: string, state: IobDataEntryEx | null | undefined, timerRelog?: boolean): void {
        timerRelog ||= false;

        // Push into DB
        if (this.sqlDPs[id]) {
            const settings = this.sqlDPs[id].config;

            if (!settings || !state) {
                return;
            }

            if (state && state.val === undefined) {
                return this.log.warn(`state value undefined received for ${id} which is not allowed. Ignoring.`);
            }

            if (typeof state.val === 'string' && settings.storageType !== 'String') {
                if (isFinite(state.val as any)) {
                    state.val = parseFloat(state.val);
                }
            }

            if (settings.enableDebugLogs) {
                this.log.debug(
                    `new value received for ${id} (storageType ${settings.storageType}), new-value=${state.val}, ts=${state.ts}, relog=${timerRelog}`,
                );
            }

            let ignoreDebounce = false;

            if (!timerRelog) {
                const valueUnstable = !!this.sqlDPs[id].timeout;
                // When a debounce timer runs and the value is the same as the last one, ignore it
                if (this.sqlDPs[id].timeout && state.ts !== state.lc) {
                    settings.enableDebugLogs &&
                        this.log.debug(
                            `value not changed debounce ${id}, value=${state.val}, ts=${state.ts}, debounce timer keeps running`,
                        );
                    return;
                } else if (this.sqlDPs[id].timeout) {
                    // if value changed, clear timer
                    settings.enableDebugLogs &&
                        this.log.debug(
                            `value changed during debounce time ${id}, value=${state.val}, ts=${state.ts}, debounce timer restarted`,
                        );
                    clearTimeout(this.sqlDPs[id].timeout);
                    this.sqlDPs[id].timeout = null;
                }

                if (
                    !valueUnstable &&
                    settings.blockTime &&
                    this.sqlDPs[id].state &&
                    this.sqlDPs[id].state.ts + settings.blockTime > state.ts
                ) {
                    settings.enableDebugLogs &&
                        this.log.debug(
                            `value ignored blockTime ${id}, value=${state.val}, ts=${state.ts}, lastState.ts=${this.sqlDPs[id].state.ts}, blockTime=${settings.blockTime}`,
                        );
                    return;
                }

                if (settings.ignoreZero && (state.val === undefined || state.val === null || state.val === 0)) {
                    if (settings.enableDebugLogs) {
                        this.log.debug(
                            `value ignore because zero or null ${id}, new-value=${state.val}, ts=${state.ts}`,
                        );
                    }
                    return;
                }
                if (
                    typeof settings.ignoreBelowNumber === 'number' &&
                    typeof state.val === 'number' &&
                    state.val < settings.ignoreBelowNumber
                ) {
                    if (settings.enableDebugLogs) {
                        this.log.debug(
                            `value ignored because below ${settings.ignoreBelowNumber} for ${id}, new-value=${state.val}, ts=${state.ts}`,
                        );
                    }
                    return;
                }
                if (
                    typeof settings.ignoreAboveNumber === 'number' &&
                    typeof state.val === 'number' &&
                    state.val > settings.ignoreAboveNumber
                ) {
                    if (settings.enableDebugLogs) {
                        this.log.debug(
                            `value ignored because above ${settings.ignoreAboveNumber} for ${id}, new-value=${state.val}, ts=${state.ts}`,
                        );
                    }
                    return;
                }

                if (this.sqlDPs[id].state && settings.changesOnly) {
                    if (!settings.changesRelogInterval) {
                        if ((this.sqlDPs[id].state.val !== null || state.val === null) && state.ts !== state.lc) {
                            // remember new timestamp
                            if (!valueUnstable && !settings.disableSkippedValueLogging) {
                                this.sqlDPs[id].skipped = state;
                            }
                            settings.enableDebugLogs &&
                                this.log.debug(
                                    `value not changed ${id}, last-value=${this.sqlDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`,
                                );
                            return;
                        }
                    } else if (this.sqlDPs[id].lastLogTime) {
                        if (
                            (this.sqlDPs[id].state.val !== null || state.val === null) &&
                            state.ts !== state.lc &&
                            Math.abs(this.sqlDPs[id].lastLogTime - state.ts) < settings.changesRelogInterval * 1000
                        ) {
                            // remember new timestamp
                            if (!valueUnstable && !settings.disableSkippedValueLogging) {
                                this.sqlDPs[id].skipped = state;
                            }
                            settings.enableDebugLogs &&
                                this.log.debug(
                                    `value not changed ${id}, last-value=${this.sqlDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`,
                                );
                            return;
                        }
                        if (state.ts !== state.lc) {
                            settings.enableDebugLogs &&
                                this.log.debug(
                                    `value-not-changed-relog ${id}, value=${state.val}, lastLogTime=${this.sqlDPs[id].lastLogTime}, ts=${state.ts}`,
                                );
                            ignoreDebounce = true;
                        }
                    }
                    if (typeof state.val === 'number') {
                        if (
                            this.sqlDPs[id].state.val !== null &&
                            settings.changesMinDelta &&
                            Math.abs((this.sqlDPs[id].state.val as number) - state.val) < settings.changesMinDelta
                        ) {
                            if (!valueUnstable && !settings.disableSkippedValueLogging) {
                                this.sqlDPs[id].skipped = state;
                            }
                            if (settings.enableDebugLogs) {
                                this.log.debug(
                                    `Min-Delta not reached ${id}, last-value=${this.sqlDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`,
                                );
                            }
                            return;
                        }
                        if (settings.changesMinDelta && settings.enableDebugLogs) {
                            this.log.debug(
                                `Min-Delta reached ${id}, last-value=${this.sqlDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`,
                            );
                        }
                    } else if (settings.enableDebugLogs) {
                        this.log.debug(
                            `Min-Delta ignored because no number ${id}, last-value=${this.sqlDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`,
                        );
                    }
                }
            }

            if (settings.counter && this.sqlDPs[id].state) {
                if (this.sqlDPs[id].type !== types.number) {
                    this.log.error('Counter must have type "number"!');
                } else if (
                    state.val === null ||
                    this.sqlDPs[id].state.val === null ||
                    (state.val as unknown as number) < (this.sqlDPs[id].state.val as unknown as number)
                ) {
                    // if the actual value is less then last seen counter, store both values
                    this.pushValueIntoDB(id, this.sqlDPs[id].state, true);
                    this.pushValueIntoDB(id, state, true);
                }
            }

            if (this.sqlDPs[id].relogTimeout) {
                clearTimeout(this.sqlDPs[id].relogTimeout);
                this.sqlDPs[id].relogTimeout = null;
            }

            if (timerRelog) {
                state = { ...state };
                state.ts = Date.now();
                state.from = `system.adapter.${this.namespace}`;
                settings.enableDebugLogs &&
                    this.log.debug(
                        `timed-relog ${id}, value=${state.val}, lastLogTime=${this.sqlDPs[id].lastLogTime}, ts=${state.ts}`,
                    );
                ignoreDebounce = true;
            } else {
                if (settings.changesOnly && this.sqlDPs[id].skipped) {
                    settings.enableDebugLogs &&
                        this.log.debug(
                            `Skipped value logged ${id}, value=${this.sqlDPs[id].skipped.val}, ts=${this.sqlDPs[id].skipped.ts}`,
                        );
                    this.pushHelper(id, this.sqlDPs[id].skipped);
                    this.sqlDPs[id].skipped = null;
                }
                if (
                    this.sqlDPs[id].state &&
                    ((this.sqlDPs[id].state.val === null && state.val !== null) ||
                        (this.sqlDPs[id].state.val !== null && state.val === null))
                ) {
                    ignoreDebounce = true;
                } else if (!this.sqlDPs[id].state && state.val === null) {
                    ignoreDebounce = true;
                }
            }

            if (settings.debounceTime && !ignoreDebounce && !timerRelog) {
                // Discard changes in the debounce time to store last stable value
                this.sqlDPs[id].timeout && clearTimeout(this.sqlDPs[id].timeout);
                this.sqlDPs[id].timeout = setTimeout(
                    (id, state) => {
                        if (!this.sqlDPs[id]) {
                            return;
                        }
                        this.sqlDPs[id].timeout = null;
                        this.sqlDPs[id].state = state;
                        this.sqlDPs[id].lastLogTime = state.ts;
                        if (settings.enableDebugLogs) {
                            this.log.debug(
                                `Value logged ${id}, value=${this.sqlDPs[id].state.val}, ts=${this.sqlDPs[id].state.ts}`,
                            );
                        }
                        this.pushHelper(id);
                        if (settings.changesOnly && settings.changesRelogInterval > 0) {
                            this.sqlDPs[id].relogTimeout = setTimeout(
                                (_id: string) => this.reLogHelper(_id),
                                settings.changesRelogInterval * 1000,
                                id,
                            );
                        }
                    },
                    settings.debounceTime,
                    id,
                    state,
                );
            } else {
                if (!timerRelog) {
                    this.sqlDPs[id].state = state;
                }
                this.sqlDPs[id].lastLogTime = state.ts;

                if (settings.enableDebugLogs) {
                    this.log.debug(
                        `Value logged ${id}, value=${this.sqlDPs[id].state?.val}, ts=${this.sqlDPs[id].state?.ts}`,
                    );
                }

                this.pushHelper(id, state);

                if (settings.changesOnly && settings.changesRelogInterval > 0) {
                    this.sqlDPs[id].relogTimeout = setTimeout(
                        (_id: string) => this.reLogHelper(_id),
                        settings.changesRelogInterval * 1000,
                        id,
                    );
                }
            }
        }
    }

    reLogHelper(_id: string): void {
        if (!this.sqlDPs[_id]) {
            this.log.info(`non-existing id ${_id}`);
        } else {
            this.sqlDPs[_id].relogTimeout = null;
            if (this.sqlDPs[_id].skipped) {
                this.pushHistory(_id, this.sqlDPs[_id].skipped, true);
            } else if (this.sqlDPs[_id].state) {
                this.pushHistory(_id, this.sqlDPs[_id].state, true);
            } else {
                this.getForeignState(this.sqlDPs[_id].realId, (err, state) => {
                    if (err) {
                        this.log.info(`init timed Relog: can not get State for ${_id} : ${err}`);
                    } else if (!state) {
                        this.log.info(
                            `init timed Relog: disable relog because state not set so far for ${_id}: ${JSON.stringify(state)}`,
                        );
                    } else {
                        this.log.debug(
                            `init timed Relog: getState ${_id}:  Value=${state.val}, ack=${state.ack}, ts=${state.ts}, lc=${state.lc}`,
                        );
                        this.sqlDPs[_id].state = state;
                        this.pushHistory(_id, this.sqlDPs[_id].state, true);
                    }
                });
            }
        }
    }

    pushHelper(_id: string, state?: IobDataEntryEx, cb?: (err?: Error | null) => void): void {
        if (!this.sqlDPs[_id] || (!this.sqlDPs[_id].state && !state)) {
            return;
        }
        state ||= this.sqlDPs[_id].state!;

        const _settings = this.sqlDPs[_id].config || {};

        let val: ioBroker.StateValue = state.val;
        if (val !== null && (typeof val === 'object' || typeof val === 'undefined')) {
            val = JSON.stringify(val);
        }

        if (val !== null && val !== undefined) {
            if (_settings.enableDebugLogs) {
                this.log.debug(`Datatype ${_id}: Currently: ${typeof val}, StorageType: ${_settings.storageType}`);
            }

            if (typeof val === 'string' && _settings.storageType !== 'String') {
                _settings.enableDebugLogs && this.log.debug(`Do Automatic Datatype conversion for ${_id}`);
                if (isFinite(val as any)) {
                    val = parseFloat(val);
                } else if (val === 'true') {
                    val = true;
                } else if (val === 'false') {
                    val = false;
                }
            }

            if (_settings.storageType === 'String' && typeof val !== 'string') {
                val = val.toString();
            } else if (_settings.storageType === 'Number' && typeof val !== 'number') {
                if (typeof val === 'boolean') {
                    val = val ? 1 : 0;
                } else {
                    return this.log.info(`Do not store value "${val}" for ${_id} because no number`);
                }
            } else if (_settings.storageType === 'Boolean' && typeof val !== 'boolean') {
                val = !!val;
            }
        } else {
            _settings.enableDebugLogs && this.log.debug(`Datatype ${_id}: Currently: null`);
        }

        state.val = val;

        this.pushValueIntoDB(_id, state, false, false, cb);
    }

    getAllIds(cb: (err?: Error | null) => void): void {
        const query = this.sqlFuncs!.getIdSelect(this.config.dbname);
        this.log.debug(query);

        this.borrowClientFromPool((err, client) => {
            if (err || !client) {
                this.returnClientToPool(client);
                return cb?.(err || new Error('No client'));
            }

            client.execute<{ name: string; id: number; type: 0 | 1 | 2 }>(
                query,
                (err: Error | null | undefined, rows?: { name: string; id: number; type: 0 | 1 | 2 }[]): void => {
                    this.returnClientToPool(client);
                    if (err) {
                        this.log.error(`Cannot select ${query}: ${err}`);
                        return cb?.(err);
                    }

                    if (rows?.length) {
                        let id;
                        for (let r = 0; r < rows.length; r++) {
                            id = rows[r].name;
                            this.sqlDPs[id] ||= {} as SQLPointConfig;
                            this.sqlDPs[id].index = rows[r].id;
                            if (rows[r].type !== null) {
                                this.sqlDPs[id].dbType = rows[r].type;
                            }
                        }
                    }
                    cb?.();
                },
            );
        });
    }

    getAllFroms(cb: (err?: Error | null) => void): void {
        const query = this.sqlFuncs!.getFromSelect(this.config.dbname);
        this.log.debug(query);

        this.borrowClientFromPool((err, client) => {
            if (err || !client) {
                this.returnClientToPool(client);
                return cb?.(err || new Error('No client'));
            }

            client.execute<{ name: string; id: number }>(query, (err, rows /* , fields */) => {
                this.returnClientToPool(client);
                if (err) {
                    this.log.error(`Cannot select ${query}: ${err}`);
                    return cb?.(err);
                }

                if (rows?.length) {
                    for (let r = 0; r < rows.length; r++) {
                        this.from[rows[r].name] = rows[r].id;
                    }
                }

                cb?.();
            });
        });
    }

    _checkRetention(query: string, cb?: () => void): void {
        this.log.debug(query);

        this.borrowClientFromPool((err?: Error | null, client?: SQLClient): void => {
            if (err || !client) {
                this.returnClientToPool(client);
                this.log.error(err?.toString() || 'No client');
                cb?.();
            } else {
                client.execute(query, (err?: Error | null): void => {
                    this.returnClientToPool(client);
                    if (err) {
                        this.log.warn(`Retention: Cannot delete ${query}: ${err}`);
                    }
                    cb?.();
                });
            }
        });
    }

    checkRetention(id: string): void {
        if (this.sqlDPs[id]?.config?.retention) {
            const dt = Date.now();
            // check every 6 hours
            if (!this.sqlDPs[id].lastCheck || dt - this.sqlDPs[id].lastCheck >= 21600000 /* 6 hours */) {
                this.sqlDPs[id].lastCheck = dt;

                if (!dbNames[this.sqlDPs[id].type]) {
                    this.log.error(`No type ${this.sqlDPs[id].type} found for ${id}. Retention is not possible.`);
                } else {
                    const query = this.sqlFuncs!.retention(
                        this.config.dbname,
                        this.sqlDPs[id].index,
                        dbNames[this.sqlDPs[id].type],
                        this.sqlDPs[id].config.retention,
                    );

                    if (!this.multiRequests) {
                        if (this.tasks.length > MAX_TASKS) {
                            return this.log.error(`Cannot queue new requests, because more than ${MAX_TASKS}`);
                        }

                        const start = this.tasks.length === 1;
                        this.tasks.push({ operation: 'delete', query });

                        // delete counters too
                        if (this.sqlDPs[id] && this.sqlDPs[id].type === 0) {
                            // 0 === number
                            const query = this.sqlFuncs!.retention(
                                this.config.dbname,
                                this.sqlDPs[id].index,
                                'ts_counter',
                                this.sqlDPs[id].config.retention,
                            );
                            this.tasks.push({ operation: 'delete', query });
                        }

                        start && this.processTasks();
                    } else {
                        this._checkRetention(query, () => {
                            // delete counters too
                            if (this.sqlDPs[id] && this.sqlDPs[id].type === 0) {
                                // 0 === number
                                const query = this.sqlFuncs!.retention(
                                    this.config.dbname,
                                    this.sqlDPs[id].index,
                                    'ts_counter',
                                    this.sqlDPs[id].config.retention,
                                );
                                this._checkRetention(query);
                            }
                        });
                    }
                }
            }
        }
    }

    _insertValueIntoDB(query: string, id: string, cb?: (err?: Error | null) => void): void {
        this.log.debug(query);

        this.borrowClientFromPool((err, client) => {
            if (err || !client) {
                this.returnClientToPool(client);
                this.log.error(err?.toString() || 'No client');
                cb?.(); // BF asked (2021.12.14): may be return here err?
            } else {
                client.execute(query, (err /* , rows, fields */) => {
                    this.returnClientToPool(client);
                    if (err) {
                        this.log.error(`Cannot insert ${query}: ${err} (id: ${id})`);
                    } else {
                        this.checkRetention(id);
                    }
                    cb?.(); // BF asked (2021.12.14): may be return here err?
                });
            }
        });
    }

    _executeQuery(query: string, id: string, cb?: () => void): void {
        this.log.debug(query);

        this.borrowClientFromPool((err: Error | null | undefined, client?: SQLClient): void => {
            if (err || !client) {
                this.returnClientToPool(client);
                this.log.error(err?.toString() || 'No client');
                cb?.();
            } else {
                client.execute(query, (err?: Error | null): void => {
                    this.returnClientToPool(client);
                    if (err) {
                        this.log.error(`Cannot query ${query}: ${err} (id: ${id})`);
                    }
                    cb?.();
                });
            }
        });
    }

    processReadTypes(): void {
        if (this.tasksReadType?.length) {
            const task = this.tasksReadType[0];

            if (!this.sqlDPs[task.id]) {
                this.log.warn(`Ignore type lookup for ${task.id} because not enabled anymore`);
                task.cb?.(new Error(`Ignore type lookup for ${task.id} because not enabled anymore`));
                task.cb = null;
                setImmediate(() => {
                    this.tasksReadType.shift();
                    this.processReadTypes();
                });
                return;
            }
            const sqlDP = this.sqlDPs[task.id];

            this.log.debug(
                `Type set in Def for ${task.id}: ${this.sqlDPs[task.id].config && this.sqlDPs[task.id].config.storageType}`,
            );

            if (sqlDP.config?.storageType) {
                sqlDP.type = types[sqlDP.config.storageType.toLowerCase()];
                this.log.debug(`Type (from Def) for ${task.id}: ${sqlDP.type}`);
                this.processVerifyTypes(task);
            } else if (sqlDP.dbType !== undefined) {
                sqlDP.type = sqlDP.dbType;
                if (sqlDP.config) {
                    sqlDP.config.storageType = storageTypes[sqlDP.type];
                }
                this.log.debug(`Type (from DB-Type) for ${task.id}: ${sqlDP.type}`);
                this.processVerifyTypes(task);
            } else {
                void this.getForeignObject(sqlDP.realId, (err, obj) => {
                    if (err) {
                        this.log.warn(`Error while get Object for Def for ${sqlDP.realId}: ${err}`);
                    }

                    if (!sqlDP) {
                        this.log.warn(`Ignore type lookup for ${task.id} because not enabled anymore`);
                        task.cb?.(new Error(`Ignore type lookup for ${task.id} because not enabled anymore`));
                        task.cb = null;
                        return setImmediate(() => {
                            this.tasksReadType.shift();
                            this.processReadTypes();
                        });
                    } else if (obj?.common?.type && types[obj.common.type.toLowerCase()] !== undefined) {
                        // read type from object
                        this.log.debug(
                            `${obj.common.type.toLowerCase()} / ${types[obj.common.type.toLowerCase()]} / ${JSON.stringify(obj.common)}`,
                        );
                        sqlDP.type = types[obj.common.type.toLowerCase() as 'string' | 'number' | 'boolean'];
                        if (sqlDP.config) {
                            sqlDP.config.storageType = storageTypes[sqlDP.type];
                        }
                        this.log.debug(`Type (from Obj) for ${task.id}: ${sqlDP.type}`);
                        this.processVerifyTypes(task);
                    } else if (sqlDP.type === undefined) {
                        void this.getForeignState(sqlDP.realId, (err, state) => {
                            if (!this.sqlDPs[task.id]) {
                                this.log.warn(`Ignore type lookup for ${task.id} because not enabled anymore`);
                                task.cb?.(new Error(`Ignore type lookup for ${task.id} because not enabled anymore`));
                                task.cb = null;
                                return setImmediate(() => {
                                    this.tasksReadType.shift();
                                    this.processReadTypes();
                                });
                            }

                            if (err && task.state) {
                                this.log.warn(
                                    `Fallback to type of current state value because no other valid type found`,
                                );
                                state = task.state as ioBroker.State;
                            }
                            if (
                                state &&
                                state.val !== null &&
                                state.val !== undefined &&
                                types[typeof state.val] !== undefined
                            ) {
                                this.sqlDPs[task.id].type = types[typeof state.val];
                                if (this.sqlDPs[task.id].config) {
                                    this.sqlDPs[task.id].config.storageType = storageTypes[this.sqlDPs[task.id].type];
                                }
                            } else {
                                this.log.warn(
                                    `Store data for ${task.id} as string because no other valid type found (${state ? typeof state.val : 'state not existing'})`,
                                );
                                this.sqlDPs[task.id].type = 1; // string
                            }

                            this.log.debug(`Type (from State) for ${task.id}: ${this.sqlDPs[task.id].type}`);
                            this.processVerifyTypes(task);
                        });
                    } else {
                        // all OK
                        task.cb?.();
                        task.cb = null;
                        this.tasksReadType.shift();
                        this.processReadTypes();
                    }
                });
            }
        }
    }

    processVerifyTypes(task: { id: string; state: IobDataEntryEx; cb?: ((err?: Error | null) => void) | null }): void {
        if (
            this.sqlDPs[task.id].index !== undefined &&
            this.sqlDPs[task.id].type !== undefined &&
            this.sqlDPs[task.id].type !== this.sqlDPs[task.id].dbType
        ) {
            this.sqlDPs[task.id].dbType = this.sqlDPs[task.id].type;

            const query = this.sqlFuncs!.getIdUpdate(
                this.config.dbname,
                this.sqlDPs[task.id].index,
                this.sqlDPs[task.id].type,
            );

            this.log.debug(query);

            return this.borrowClientFromPool((err, client) => {
                if (err || !client) {
                    this.returnClientToPool(client);
                    this.processVerifyTypes(task);
                    return;
                }

                client.execute(query, err => {
                    this.returnClientToPool(client);
                    if (err) {
                        this.log.error(
                            `error updating history config for ${task.id} to pin datatype: ${query}: ${err}`,
                        );
                    } else {
                        this.log.info(`changed history configuration to pin detected datatype for ${task.id}`);
                    }
                    this.processVerifyTypes(task);
                });
            });
        }

        task.cb?.();
        task.cb = null;

        setTimeout(() => {
            this.tasksReadType.shift();
            this.processReadTypes();
        }, 50);
    }

    prepareTaskReadDbId(
        id: string,
        state: IobDataEntryEx,
        isCounter: boolean,
        cb?: (err?: Error | null) => void,
    ): void {
        if (!this.sqlDPs[id]) {
            cb?.(new Error(`${id} not active any more`));
            return;
        }
        const type = this.sqlDPs[id].type;

        if (type === undefined) {
            // Can not happen anymore
            let warn;
            if (state.val === null) {
                warn = `Ignore null value for ${id} because no type defined till now.`;
            } else {
                warn = `Cannot store values of type "${typeof state.val}" for ${id}`;
            }

            this.log.warn(warn);
            cb?.(new Error(warn));
            return;
        }

        let tmpState: IobDataEntryEx;
        // get SQL id of state
        if (this.sqlDPs[id].index === undefined) {
            this.sqlDPs[id].isRunning ||= [];

            tmpState = { ...state };

            this.sqlDPs[id].isRunning.push({ id, state: tmpState, cb, isCounter });

            if (this.sqlDPs[id].isRunning.length === 1) {
                // read or create in DB
                return this.getId(id, type, (err, _id) => {
                    this.log.debug(
                        `prepareTaskCheckTypeAndDbId getId Result - isRunning length = ${this.sqlDPs[id].isRunning ? this.sqlDPs[id].isRunning.length : 'none'}`,
                    );
                    if (err) {
                        this.log.warn(`Cannot get index of "${_id}": ${err}`);
                        this.sqlDPs[_id].isRunning?.forEach(r =>
                            r.cb?.(new Error(`Cannot get index of "${r.id}": ${err}`)),
                        );
                    } else {
                        this.sqlDPs[_id].isRunning?.forEach(r => r.cb?.());
                    }

                    this.sqlDPs[_id].isRunning = undefined;
                });
            }
            return;
        }

        // get from
        if (!isCounter && state.from && !this.from[state.from]) {
            this.isFromRunning[state.from] ||= [];
            tmpState = { ...state };
            const isFromRunning = this.isFromRunning[state.from]!;

            isFromRunning.push({ id, state: tmpState, cb });

            if (isFromRunning.length === 1) {
                // read or create in DB
                return this.getFrom(state.from, (err, from) => {
                    this.log.debug(
                        `prepareTaskCheckTypeAndDbId getFrom ${from} Result - isRunning length = ${this.isFromRunning[from] ? this.isFromRunning[from].length : 'none'}`,
                    );
                    if (err) {
                        this.log.warn(`Cannot get "from" for "${from}": ${err}`);
                        this.isFromRunning[from]?.forEach(f =>
                            f.cb?.(new Error(`Cannot get "from" for "${from}": ${err}`)),
                        );
                    } else {
                        this.isFromRunning[from]?.forEach(f => f.cb?.());
                    }
                    this.isFromRunning[from] = null;
                });
            }
            return;
        }

        if (state.ts) {
            state.ts = parseInt(state.ts as unknown as string, 10);
        }

        try {
            if (state.val !== null && typeof state.val === 'object') {
                state.val = JSON.stringify(state.val);
            }
        } catch {
            const error = `Cannot convert the object value "${id}"`;
            this.log.error(error);
            cb?.(new Error(error));
            return;
        }

        cb?.();
    }

    prepareTaskCheckTypeAndDbId(
        id: string,
        state: IobDataEntryEx,
        isCounter: boolean,
        cb?: (err?: Error | null) => void,
    ): void {
        // check if we know about this ID
        if (!this.sqlDPs[id]) {
            if (cb) {
                setImmediate(() => cb(new Error(`Unknown ID: ${id}`)));
            }
            return;
        }

        // Check SQL connection
        if (!this.clientPool) {
            this.log.warn('No Connection to database');
            if (cb) {
                setImmediate(() => cb(new Error('No Connection to database')));
            }
            return;
        }
        this.log.debug(`prepareTaskCheckTypeAndDbId CALLED for ${id}`);

        // read type of value
        if (this.sqlDPs[id].type !== undefined) {
            this.prepareTaskReadDbId(id, state, isCounter, cb);
        } else {
            // read type from DB
            this.tasksReadType.push({
                id,
                state,
                cb: err => {
                    if (err) {
                        cb?.(err);
                    } else {
                        this.prepareTaskReadDbId(id, state, isCounter, cb);
                    }
                },
            });

            if (this.tasksReadType.length === 1) {
                this.processReadTypes();
            }
        }
    }

    pushValueIntoDB(
        id: string,
        state: IobDataEntryEx,
        isCounter: boolean,
        storeInCacheOnly?: boolean,
        cb?: (err?: Error | null) => void,
    ): void {
        if (!this.sqlDPs[id] || !state) {
            return cb?.();
        }

        this.log.debug(
            `pushValueIntoDB called for ${id} (type: ${this.sqlDPs[id].type}, ID: ${this.sqlDPs[id].index}) and state: ${JSON.stringify(state)}`,
        );

        this.prepareTaskCheckTypeAndDbId(id, state, isCounter, (err?: Error | null): void => {
            if (!this.sqlDPs[id]) {
                return cb?.();
            }
            this.log.debug(
                `pushValueIntoDB-prepareTaskCheckTypeAndDbId RESULT for ${id} (type: ${this.sqlDPs[id].type}, ID: ${this.sqlDPs[id].index}) and state: ${JSON.stringify(state)}: ${err}`,
            );
            if (err) {
                return cb?.(err);
            }

            const type = this.sqlDPs[id].type;

            // increase timestamp if last is the same
            if (!isCounter && this.sqlDPs[id].ts && state.ts === this.sqlDPs[id].ts) {
                state.ts++;
            }

            // remember last timestamp
            this.sqlDPs[id].ts = state.ts;

            // if it was not deleted in this time
            this.sqlDPs[id].list ||= [];

            this.sqlDPs[id].list.push({
                state,
                from: state.from ? this.from[state.from] : 0,
                table: isCounter ? 'ts_counter' : dbNames[type],
            });

            const _settings = this.sqlDPs[id].config || {};
            const maxLength = _settings.maxLength !== undefined ? _settings.maxLength : this.config.maxLength || 0;
            if ((cb || (_settings && this.sqlDPs[id].list.length > maxLength)) && !storeInCacheOnly) {
                this.storeCached(id, cb);
            } else if (cb && storeInCacheOnly) {
                setImmediate(cb);
            }
        });
    }

    storeCached(onlyId?: string, cb?: ((err?: Error | null) => void) | null): void {
        let count = 0;
        for (const id in this.sqlDPs) {
            if (!Object.prototype.hasOwnProperty.call(this.sqlDPs, id) || (onlyId !== undefined && onlyId !== id)) {
                continue;
            }

            const _settings = this.sqlDPs[id].config || {};
            if (_settings && this.sqlDPs[id]?.list?.length) {
                if (_settings.enableDebugLogs) {
                    this.log.debug(`inserting ${this.sqlDPs[id].list.length} entries from ${id} to DB`);
                }
                const inFlightId = `${id}_${Date.now()}_${Math.random()}`;
                this.sqlDPs[id].inFlight ||= {};
                this.sqlDPs[id].inFlight[inFlightId] = this.sqlDPs[id].list;
                this.sqlDPs[id].list = [];
                count++;
                this.pushValuesIntoDB(id, this.sqlDPs[id].inFlight[inFlightId], (err?: Error | null): void => {
                    if (this.sqlDPs[id]?.inFlight?.[inFlightId]) {
                        delete this.sqlDPs[id].inFlight[inFlightId];
                    }
                    if (!--count && cb) {
                        cb?.(err);
                        cb = null;
                    }
                });
                if (onlyId !== undefined) {
                    break;
                }
            }
        }
        if (!count && cb) {
            cb();
        }
    }

    pushValuesIntoDB(
        id: string,
        list: { state: IobDataEntryEx; from: number; table: TableName }[],
        cb?: (err?: Error | null) => void,
    ): void {
        if (!list.length) {
            if (cb) {
                setImmediate(() => cb());
            }
            return;
        }

        if (!this.multiRequests) {
            if (this.tasks.length > MAX_TASKS) {
                const error = `Cannot queue new requests, because more than ${MAX_TASKS}`;
                this.log.error(error);
                cb?.(new Error(error));
            } else {
                this.tasks.push({ operation: 'insert', index: this.sqlDPs[id].index, list, id, callback: cb });
                this.tasks.length === 1 && this.processTasks();
            }
        } else {
            const query = this.sqlFuncs!.insert(this.config.dbname, this.sqlDPs[id].index, list);
            this._insertValueIntoDB(query, id, cb);
        }
    }

    processTasks(): void {
        if (this.lockTasks) {
            return this.log.debug('Tries to execute task, but last one not finished!');
        }

        this.lockTasks = true;

        if (this.tasks.length) {
            if (this.tasks[0].operation === 'query') {
                const taskQuery: TaskQuery = this.tasks[0];
                this._executeQuery(taskQuery.query, this.tasks[0].id, () => {
                    taskQuery.callback?.();
                    this.tasks.shift();
                    this.lockTasks = false;
                    if (this.tasks.length) {
                        setTimeout(() => this.processTasks(), this.config.requestInterval);
                    }
                });
            } else if (this.tasks[0].operation === 'insert') {
                const taskInsert: TaskInsert = this.tasks[0];
                const callbacks: ((err?: Error | null) => void)[] = [];
                if (taskInsert.callback) {
                    callbacks.push(taskInsert.callback);
                }
                for (let i = 1; i < this.tasks.length; i++) {
                    if (this.tasks[i].operation === 'insert') {
                        const _taskInsert: TaskInsert = this.tasks[i] as TaskInsert;
                        if (taskInsert.index === _taskInsert.index) {
                            taskInsert.list = taskInsert.list.concat(_taskInsert.list);
                            if (_taskInsert.callback) {
                                callbacks.push(_taskInsert.callback);
                            }
                            this.tasks.splice(i, 1);
                            i--;
                        }
                    }
                }
                const query = this.sqlFuncs!.insert(this.config.dbname, this.tasks[0].index, this.tasks[0].list);
                this._insertValueIntoDB(query, this.tasks[0].id, () => {
                    callbacks.forEach(cb => cb());
                    this.tasks.shift();
                    this.lockTasks = false;
                    if (this.tasks.length) {
                        setTimeout(() => this.processTasks(), this.config.requestInterval);
                    }
                });
            } else if (this.tasks[0].operation === 'select') {
                const taskSelect: TaskSelect = this.tasks[0];
                this.#getDataFromDB(taskSelect.query, taskSelect.options, (err, rows) => {
                    taskSelect.callback?.(err, rows);
                    this.tasks.shift();
                    this.lockTasks = false;
                    if (this.tasks.length) {
                        setTimeout(() => this.processTasks(), this.config.requestInterval);
                    }
                });
            } else if (this.tasks[0].operation === 'userQuery') {
                const taskUserQuery: TaskUserQuery = this.tasks[0];
                this._userQuery(taskUserQuery.msg, () => {
                    this.tasks.shift();
                    this.lockTasks = false;
                    if (this.tasks.length) {
                        setTimeout(() => this.processTasks(), this.config.requestInterval);
                    }
                });
            } else if (this.tasks[0].operation === 'delete') {
                const taskDelete: TaskDelete = this.tasks[0];
                this._checkRetention(taskDelete.query, () => {
                    this.tasks.shift();
                    this.lockTasks = false;
                    if (this.tasks.length) {
                        setTimeout(() => this.processTasks(), this.config.requestInterval);
                    }
                });
            } else {
                this.log.error(`unknown task: ${(this.tasks[0] as any).operation}`);
                (this.tasks[0] as any).callback?.('Unknown task');
                this.tasks.shift();
                this.lockTasks = false;
                if (this.tasks.length) {
                    setTimeout(() => this.processTasks(), this.config.requestInterval);
                }
            }
        }
    }

    // may be it is required to cache all the data in memory
    getId(id: string, type: 0 | 1 | 2 | null, cb: (err: Error | null, id: string) => void): void {
        let query = this.sqlFuncs!.getIdSelect(this.config.dbname, id);
        this.log.debug(query);

        this.borrowClientFromPool((err, client) => {
            if (err || !client) {
                this.returnClientToPool(client);
                cb?.(err || new Error('No client'), id);
                return;
            }

            client.execute<{ id: number; name: string; type: 0 | 1 | 2 }>(query, (err, rows /* , fields */) => {
                if (!this.sqlDPs[id]) {
                    this.returnClientToPool(client);
                    cb?.(new Error(`ID ${id} no longer active`), id);
                    return;
                }

                if (err) {
                    this.returnClientToPool(client);
                    this.log.error(`Cannot select ${query}: ${err}`);
                    cb?.(err, id);
                    return;
                }
                if (!rows?.length) {
                    if (type !== null && type !== undefined) {
                        // insert
                        query = this.sqlFuncs!.getIdInsert(this.config.dbname, id, type);

                        this.log.debug(query);

                        client.execute(query, (err /* , rows, fields */) => {
                            if (err) {
                                this.returnClientToPool(client);
                                this.log.error(`Cannot insert ${query}: ${err}`);
                                cb?.(err, id);
                            } else {
                                query = this.sqlFuncs!.getIdSelect(this.config.dbname, id);

                                this.log.debug(query);

                                client.execute<{ id: number; name: string; type: 0 | 1 | 2 }>(
                                    query,
                                    (err, rows /* , fields */) => {
                                        this.returnClientToPool(client);

                                        if (err) {
                                            this.log.error(`Cannot select ${query}: ${err}`);
                                            cb?.(err, id);
                                        } else if (rows?.[0]) {
                                            this.sqlDPs[id].index = rows[0].id;
                                            this.sqlDPs[id].type = rows[0].type;

                                            cb?.(null, id);
                                        } else {
                                            this.log.error(`No result for select ${query}: after insert`);
                                            cb?.(new Error(`No result for select ${query}: after insert`), id);
                                        }
                                    },
                                );
                            }
                        });
                    } else {
                        this.returnClientToPool(client);
                        cb?.(new Error('id not found'), id);
                    }
                } else {
                    this.sqlDPs[id].index = rows[0].id;
                    if (rows[0].type === null || typeof rows[0].type !== 'number') {
                        this.sqlDPs[id].type = type === null ? 1 : type; // default string

                        const query = this.sqlFuncs!.getIdUpdate(
                            this.config.dbname,
                            this.sqlDPs[id].index,
                            this.sqlDPs[id].type,
                        );

                        this.log.debug(query);

                        client.execute(query, err => {
                            this.returnClientToPool(client);
                            if (err) {
                                this.log.error(
                                    `error updating history config for ${id} to pin datatype: ${query}: ${err}`,
                                );
                            } else {
                                this.log.info(`changed history configuration to pin detected datatype for ${id}`);
                            }
                            cb?.(null, id);
                        });
                    } else {
                        this.returnClientToPool(client);

                        this.sqlDPs[id].type = rows[0].type;

                        cb?.(null, id);
                    }
                }
            });
        });
    }

    // may be it is required to cache all the data in memory
    getFrom(_from: string, cb: (err: Error | null | undefined, from: string) => void): void {
        // const sources    = (this.config.dbtype !== 'postgresql' ? (this.config.dbname + '.') : '') + 'sources';
        let query = this.sqlFuncs!.getFromSelect(this.config.dbname, _from);
        this.log.debug(query);

        this.borrowClientFromPool((err, client) => {
            if (err || !client) {
                this.returnClientToPool(client);
                return cb?.(err || new Error('No client'), _from);
            }
            client.execute<{ id: number }>(query, (err, rows /* , fields */) => {
                if (err) {
                    this.returnClientToPool(client);
                    this.log.error(`Cannot select ${query}: ${err}`);
                    return cb?.(err, _from);
                }
                if (!rows?.length) {
                    // insert
                    query = this.sqlFuncs!.getFromInsert(this.config.dbname, _from);
                    this.log.debug(query);
                    client.execute(query, err => {
                        if (err) {
                            this.returnClientToPool(client);
                            this.log.error(`Cannot insert ${query}: ${err}`);
                            return cb?.(err, _from);
                        }

                        query = this.sqlFuncs!.getFromSelect(this.config.dbname, _from);
                        this.log.debug(query);
                        client.execute<{ id: number }>(query, (err, rows /* , fields */) => {
                            this.returnClientToPool(client);
                            if (err || !rows?.length) {
                                this.log.error(`Cannot select ${query}: ${err}`);
                                return cb?.(err, _from);
                            }
                            this.from[_from] = rows[0].id;

                            cb?.(null, _from);
                        });
                    });
                } else {
                    this.returnClientToPool(client);

                    this.from[_from] = rows[0].id;

                    cb?.(null, _from);
                }
            });
        });
    }

    getOneCachedData(
        id: string,
        options: ioBroker.GetHistoryOptions & { id: string | null },
        cache: IobDataEntryEx[],
    ): boolean {
        if (this.sqlDPs[id]) {
            let anyInflight = false;
            let res: { state: IobDataEntryEx; from: number; table: TableName }[] = [];
            for (const inFlightId in this.sqlDPs[id].inFlight) {
                this.log.debug(
                    `getOneCachedData: add ${this.sqlDPs[id].inFlight[inFlightId].length} inFlight datapoints for ${options.id}`,
                );
                res = res.concat(this.sqlDPs[id].inFlight[inFlightId]);
                anyInflight ||= !!this.sqlDPs[id].inFlight[inFlightId].length;
            }
            res = res.concat(this.sqlDPs[id].list);
            // todo can be optimized
            if (res) {
                let iProblemCount = 0;
                let vLast = null;
                for (let i = res.length - 1; i >= 0; i--) {
                    if (!res[i] || !res[i].state) {
                        iProblemCount++;
                        continue;
                    }
                    if (options.start && res[i].state.ts < options.start) {
                        // add one before start
                        cache.unshift({ ...res[i].state });
                        break;
                    } else if (res[i].state.ts > options.end!) {
                        // add one after end
                        vLast = res[i].state;
                        continue;
                    }

                    if (vLast) {
                        cache.unshift({ ...vLast });
                        vLast = null;
                    }

                    cache.unshift({ ...res[i].state });

                    if (
                        options.returnNewestEntries &&
                        options.count &&
                        cache.length >= options.count &&
                        (options.aggregate === 'onchange' || !options.aggregate || options.aggregate === 'none')
                    ) {
                        break;
                    }
                }

                if (iProblemCount) {
                    this.log.warn(`getOneCachedData: got null states ${iProblemCount} times for ${options.id}`);
                }

                this.log.debug(`getOneCachedData: got ${res.length} datapoints for ${options.id}`);
                return anyInflight;
            }
            this.log.debug(`getOneCachedData: datapoints for ${options.id} do not yet exist`);
        }
        return false;
    }

    getCachedData(
        options: ioBroker.GetHistoryOptions & { id: string | null; index: number | null },
        callback: (
            cache: (IobDataEntryEx & { date?: Date })[],
            isFull: boolean,
            includesInFlightData: boolean,
            earliestTs: number | null,
        ) => void,
    ): void {
        const cache: (IobDataEntryEx & { date?: Date })[] = [];
        let anyInflight = false;

        if (options.id) {
            anyInflight = this.getOneCachedData(options.id, options, cache);
        } else {
            for (const id in this.sqlDPs) {
                if (Object.prototype.hasOwnProperty.call(this.sqlDPs, id)) {
                    anyInflight ||= this.getOneCachedData(id, options, cache);
                }
            }
        }

        let earliestTs: number | null = null;
        for (let c = 0; c < cache.length; c++) {
            if (typeof cache[c].ts === 'string') {
                cache[c].ts = parseInt(cache[c].ts as unknown as string, 10);
            }

            if (this.common?.loglevel === 'debug') {
                cache[c].date = new Date(cache[c].ts);
            }
            if (options.ack) {
                cache[c].ack = !!cache[c].ack;
            }
            if (typeof cache[c].val === 'number' && isFinite(cache[c].val as any) && options.round) {
                cache[c].val = Math.round((cache[c].val as number) * options.round) / options.round;
            }
            if (options.id && this.sqlDPs[options.id] && this.sqlDPs[options.id].type === 2) {
                // 2 === boolean
                cache[c].val = !!cache[c].val;
            }
            if (options.addId && !cache[c].id && options.id) {
                cache[c].id = options.id;
            }
            if (earliestTs === null || cache[c].ts < earliestTs) {
                earliestTs = cache[c].ts;
            }
        }

        // options.length = cache.length;
        callback(
            cache,
            !!options.returnNewestEntries && !!options.count && cache.length >= options.count,
            anyInflight,
            earliestTs,
        );
    }

    #getDataFromDB(
        query: string,
        options: ioBroker.GetHistoryOptions & { id: string | null; index: number | null },
        callback?: (err: Error | null, result?: (IobDataEntryEx & { date?: Date; id?: string })[]) => void,
    ): void {
        this.log.debug(query);

        this.borrowClientFromPool((err, client) => {
            if (err || !client) {
                this.returnClientToPool(client);
                callback?.(err || new Error('No client'));
                return;
            }
            client.execute<IobDataEntryEx & { date: Date; id?: string }>(query, (err, rows /* , fields */): void => {
                this.returnClientToPool(client);

                if (!err && rows) {
                    for (let c = 0; c < rows.length; c++) {
                        if (typeof rows[c].ts === 'string') {
                            rows[c].ts = parseInt(rows[c].ts as unknown as string, 10);
                        }

                        if (this.common?.loglevel === 'debug') {
                            rows[c].date = new Date(rows[c].ts);
                        }
                        if (options.ack) {
                            rows[c].ack = !!rows[c].ack;
                        }
                        if (typeof rows[c].val === 'number' && isFinite(rows[c].val as any) && options.round) {
                            rows[c].val = Math.round((rows[c].val as number) * options.round) / options.round;
                        }
                        if (options.id && this.sqlDPs[options.id] && this.sqlDPs[options.id].type === 2) {
                            // 2 === boolean
                            rows[c].val = !!rows[c].val;
                        }
                        if (options.addId && !rows[c].id && options.id) {
                            rows[c].id = options.id;
                        }
                    }
                }
                callback?.(err, rows);
            });
        });
    }

    getDataFromDB(
        table: TableName,
        options: ioBroker.GetHistoryOptions & { id: string | null; index: number | null },
        callback: (err: Error | null, result?: (IobDataEntryEx & { date?: Date; id?: string })[]) => void,
    ): void {
        const query = this.sqlFuncs!.getHistory(this.config.dbname, table, options);
        this.log.debug(query);
        if (!this.multiRequests) {
            if (this.tasks.length > MAX_TASKS) {
                this.log.error(`Cannot queue new requests, because more than ${MAX_TASKS}`);
                callback?.(new Error(`Cannot queue new requests, because more than ${MAX_TASKS}`));
            } else {
                this.tasks.push({ operation: 'select', query, options, callback });
                if (this.tasks.length === 1) {
                    this.processTasks();
                }
            }
        } else {
            this.#getDataFromDB(query, options, callback);
        }
    }

    getCounterDataFromDB(
        options: {
            id: string;
            index: number;
            start: number;
            end: number;
        },
        callback?: (err: Error | null, result?: (IobDataEntryEx & { date?: Date; id?: string })[]) => void,
    ): void {
        const query = this.sqlFuncs!.getCounterDiff(this.config.dbname, options);

        this.log.debug(query);

        if (!this.multiRequests) {
            if (this.tasks.length > MAX_TASKS) {
                const error = `Cannot queue new requests, because more than ${MAX_TASKS}`;
                this.log.error(error);
                callback?.(new Error(error));
            } else {
                this.tasks.push({ operation: 'select', query, options, callback });
                if (this.tasks.length === 1) {
                    this.processTasks();
                }
            }
        } else {
            this.#getDataFromDB(query, options, callback);
        }
    }

    getCounterDiff(msg: ioBroker.Message): void {
        const id: string = msg.message.id;
        const start: number = msg.message.options.start || 0;
        const end: number = msg.message.options.end || Date.now() + 5000000;

        if (!this.sqlDPs[id]) {
            this.sendTo(msg.from, msg.command, { result: [], step: null, error: 'Not enabled' }, msg.callback);
        } else {
            if (!this.sqlFuncs!.getCounterDiff) {
                this.sendTo(
                    msg.from,
                    msg.command,
                    { result: [], step: null, error: 'Counter option is not enabled for this type of SQL' },
                    msg.callback,
                );
            } else {
                const options = { id, start, end, index: this.sqlDPs[id].index };
                this.getCounterDataFromDB(options, (err, data) =>
                    sendResponseCounter(
                        this as unknown as ioBroker.Adapter,
                        msg,
                        options,
                        err?.toString() || (data as IobDataEntry[]) || [],
                    ),
                );
            }
        }
    }

    getHistorySql(msg: ioBroker.Message): void {
        const startTime = Date.now();

        if (!msg.message?.options) {
            return this.sendTo(
                msg.from,
                msg.command,
                {
                    error: 'Invalid call. No options for getHistory provided',
                },
                msg.callback,
            );
        }
        let ignoreNull: ioBroker.GetHistoryOptions['ignoreNull'];
        if (msg.message.options.ignoreNull === 'true') {
            ignoreNull = true;
        } // include nulls and replace them with last value
        if (msg.message.options.ignoreNull === 'false') {
            ignoreNull = false;
        } // include nulls
        if (msg.message.options.ignoreNull === '0') {
            ignoreNull = 0;
        } // include nulls and replace them with 0
        if (
            msg.message.options.ignoreNull !== true &&
            msg.message.options.ignoreNull !== false &&
            msg.message.options.ignoreNull !== 0
        ) {
            ignoreNull = false;
        }
        const logId: string = (msg.message.id ? msg.message.id : 'all') + Date.now() + Math.random();
        const options: ioBroker.GetHistoryOptions & { id: string | null; index: number | null } = {
            id: msg.message.id === '*' ? null : msg.message.id,
            start: msg.message.options.start,
            end: msg.message.options.end || Date.now() + 5000000,
            step: parseInt(msg.message.options.step, 10) || undefined,
            count: parseInt(msg.message.options.count, 10),
            ignoreNull,
            aggregate: msg.message.options.aggregate || 'average', // One of: max, min, average, total, none, on-change
            limit: parseInt(msg.message.options.limit, 10) || parseInt(msg.message.options.count, 10) || 2000,
            from: msg.message.options.from || false,
            q: msg.message.options.q || false,
            ack: msg.message.options.ack || false,
            addId: msg.message.options.addId || false,
            sessionId: msg.message.options.sessionId,
            returnNewestEntries: msg.message.options.returnNewestEntries || false,
            percentile:
                msg.message.options.aggregate === 'percentile'
                    ? parseInt(msg.message.options.percentile, 10) || 50
                    : undefined,
            quantile:
                msg.message.options.aggregate === 'quantile'
                    ? parseFloat(msg.message.options.quantile) || 0.5
                    : undefined,
            integralUnit:
                msg.message.options.aggregate === 'integral'
                    ? parseInt(msg.message.options.integralUnit, 10) || 60
                    : undefined,
            integralInterpolation:
                msg.message.options.aggregate === 'integral'
                    ? msg.message.options.integralInterpolation || 'none'
                    : null,
            removeBorderValues: msg.message.options.removeBorderValues || false,
            // ID in database
            index: msg.message.id === '*' ? null : this.sqlDPs[msg.message.id]?.index || null,
        };

        this.log.debug(`${logId} getHistory message: ${JSON.stringify(msg.message)}`);

        if (!options.count || isNaN(options.count)) {
            if (options.aggregate === 'none' || options.aggregate === 'onchange') {
                options.count = options.limit;
            } else {
                options.count = 500;
            }
        }

        try {
            if (options.start && typeof options.start !== 'number') {
                options.start = new Date(options.start).getTime();
            }
        } catch {
            return this.sendTo(
                msg.from,
                msg.command,
                {
                    error: `Invalid call. Start date ${JSON.stringify(options.start)} is not a valid date`,
                },
                msg.callback,
            );
        }

        try {
            if (options.end && typeof options.end !== 'number') {
                options.end = new Date(options.end).getTime();
            }
        } catch {
            return this.sendTo(
                msg.from,
                msg.command,
                {
                    error: `Invalid call. End date ${JSON.stringify(options.end)} is not a valid date`,
                },
                msg.callback,
            );
        }

        if (!options.start && options.count) {
            options.returnNewestEntries = true;
        }

        if (
            msg.message.options.round !== null &&
            msg.message.options.round !== undefined &&
            msg.message.options.round !== ''
        ) {
            msg.message.options.round = parseInt(msg.message.options.round, 10);
            if (!isFinite(msg.message.options.round) || msg.message.options.round < 0) {
                options.round = this.config.round === null ? undefined : this.config.round;
            } else {
                options.round = Math.pow(10, parseInt(msg.message.options.round, 10));
            }
        } else {
            options.round = this.config.round === null ? undefined : this.config.round;
        }

        if (options.id && this.aliasMap[options.id]) {
            options.id = this.aliasMap[options.id];
        }

        if ((options.aggregate === 'percentile' && options.percentile! < 0) || options.percentile! > 100) {
            this.log.error(`Invalid percentile value: ${options.percentile}, use 50 as default`);
            options.percentile = 50;
        }

        if ((options.aggregate === 'quantile' && options.quantile! < 0) || options.quantile! > 1) {
            this.log.error(`Invalid quantile value: ${options.quantile}, use 0.5 as default`);
            options.quantile = 0.5;
        }

        if (
            options.aggregate === 'integral' &&
            (typeof options.integralUnit !== 'number' || options.integralUnit <= 0)
        ) {
            this.log.error(`Invalid integralUnit value: ${options.integralUnit}, use 60s as default`);
            options.integralUnit = 60;
        }

        if (options.id) {
            this.sqlDPs[options.id] ||= {} as SQLPointConfig;
        }
        const debugLog = !!(
            (options.id && this.sqlDPs[options.id]?.config?.enableDebugLogs) ||
            this.config.enableDebugLogs
        );

        if (options.start! > options.end!) {
            const _end = options.end;
            options.end = options.start;
            options.start = _end;
        }

        if (!options.start && !options.count) {
            options.start = Date.now() - 86400000; // - 1 day
        }

        if (debugLog) {
            this.log.debug(`${logId} getHistory options final: ${JSON.stringify(options)}`);
        }

        if (options.id && this.sqlDPs[options.id].type === undefined && this.sqlDPs[options.id].dbType !== undefined) {
            const storageType = this.sqlDPs[options.id].config?.storageType;
            if (storageType) {
                if (storageTypes.indexOf(storageType) === this.sqlDPs[options.id].dbType) {
                    if (debugLog) {
                        this.log.debug(
                            `${logId} For getHistory for id ${options.id}: Type empty, use storageType dbType ${this.sqlDPs[options.id].dbType}`,
                        );
                    }
                    this.sqlDPs[options.id].type = this.sqlDPs[options.id].dbType;
                }
            } else {
                if (debugLog) {
                    this.log.debug(
                        `${logId} For getHistory for id ${options.id}: Type empty, use dbType ${this.sqlDPs[options.id].dbType}`,
                    );
                }
                this.sqlDPs[options.id].type = this.sqlDPs[options.id].dbType;
            }
        }
        if (options.id && this.sqlDPs[options.id].index === undefined) {
            // read or create in DB
            return this.getId(options.id, null, err => {
                if (err) {
                    this.log.warn(`Cannot get index of "${options.id}": ${err}`);
                    sendResponse(this as unknown as ioBroker.Adapter, msg, options.id!, options, [], startTime);
                } else {
                    this.getHistorySql(msg);
                }
            });
        }
        if (options.id && this.sqlDPs[options.id].type === undefined) {
            this.log.warn(
                `For getHistory for id "${options.id}": Type empty. Need to write data first. Index = ${this.sqlDPs[options.id].index}`,
            );
            sendResponse(
                this as unknown as ioBroker.Adapter,
                msg,
                options.id,
                options,
                'Please wait till next data record is logged and reload.',
                startTime,
            );
            return;
        }

        // if specific id requested
        if (options.id) {
            const type = this.sqlDPs[options.id].type;
            options.index = this.sqlDPs[options.id].index;

            this.getCachedData(options, (cacheData, isFull, includesInFlightData, earliestTs) => {
                if (debugLog) {
                    this.log.debug(`${logId} after getCachedData: length = ${cacheData.length}, isFull=${isFull}`);
                }

                // if all data read
                if (
                    isFull &&
                    cacheData.length &&
                    (options.aggregate === 'onchange' || !options.aggregate || options.aggregate === 'none')
                ) {
                    cacheData.sort(sortByTs);
                    if (options.count && cacheData.length > options.count && options.aggregate === 'none') {
                        cacheData.splice(0, cacheData.length - options.count);
                        if (debugLog) {
                            this.log.debug(`${logId} cut cacheData to ${options.count} values`);
                        }
                    }
                    this.log.debug(`${logId} Send: ${cacheData.length} values in: ${Date.now() - startTime}ms`);

                    this.sendTo(
                        msg.from,
                        msg.command,
                        {
                            result: cacheData,
                            step: null,
                            error: null,
                        },
                        msg.callback,
                    );
                } else {
                    const origEnd = options.end;
                    if (includesInFlightData && earliestTs) {
                        options.end = earliestTs;
                    }
                    // if not all data read
                    this.getDataFromDB(dbNames[type], options, (err, data) => {
                        if (!err && data) {
                            options.end = origEnd;
                            if (options.aggregate === 'none' && options.count && options.returnNewestEntries) {
                                cacheData = cacheData.reverse();
                                data = cacheData.concat(data);
                            } else {
                                data = data.concat(cacheData);
                            }
                            if (debugLog) {
                                this.log.debug(`${logId} after getDataFromDB: length = ${data.length}`);
                            }
                            if (
                                options.count &&
                                data.length > options.count &&
                                options.aggregate === 'none' &&
                                !options.returnNewestEntries
                            ) {
                                if (options.start) {
                                    for (let i = 0; i < data.length; i++) {
                                        if (data[i].ts < options.start) {
                                            data.splice(i, 1);
                                            i--;
                                        } else {
                                            break;
                                        }
                                    }
                                }
                                data.splice(options.count);
                                if (debugLog) {
                                    this.log.debug(`${logId} pre-cut data to ${options.count} oldest values`);
                                }
                            }

                            data.sort(sortByTs);
                        }
                        try {
                            sendResponse(
                                this as unknown as ioBroker.Adapter,
                                msg,
                                options.id!,
                                options,
                                err?.toString() || (data as IobDataEntry[]) || [],
                                startTime,
                                debugLog ? logId : undefined,
                            );
                        } catch (e) {
                            sendResponse(
                                this as unknown as ioBroker.Adapter,
                                msg,
                                options.id!,
                                options,
                                e.toString(),
                                startTime,
                            );
                        }
                    });
                }
            });
        } else {
            // if all IDs requested
            let rows: (IobDataEntryEx & { date?: Date; id?: string })[] = [];
            let count = 0;
            this.getCachedData(options, (cacheData, isFull, includesInFlightData, earliestTs) => {
                if (isFull && cacheData.length) {
                    cacheData.sort(sortByTs);
                    sendResponse(
                        this as unknown as ioBroker.Adapter,
                        msg,
                        '',
                        options,
                        cacheData as IobDataEntry[],
                        startTime,
                        debugLog ? logId : undefined,
                    );
                } else {
                    if (includesInFlightData && earliestTs) {
                        options.end = earliestTs;
                    }
                    for (let db = 0; db < dbNames.length; db++) {
                        count++;
                        this.getDataFromDB(dbNames[db], options, (err, data) => {
                            if (data) {
                                rows = rows.concat(data);
                            }
                            if (!--count) {
                                rows.sort(sortByTs);
                                try {
                                    sendResponse(
                                        this as unknown as ioBroker.Adapter,
                                        msg,
                                        '',
                                        options,
                                        rows as IobDataEntry[],
                                        startTime,
                                    );
                                } catch (e) {
                                    sendResponse(
                                        this as unknown as ioBroker.Adapter,
                                        msg,
                                        '',
                                        options,
                                        e.toString(),
                                        startTime,
                                    );
                                }
                            }
                        });
                    }
                }
            });
        }
    }

    update(id: string, state: ioBroker.State, cb?: (err?: Error | null) => void): void {
        // first try to find the value in not yet saved data
        let found = false;
        if (this.sqlDPs[id]) {
            const res = this.sqlDPs[id].list;
            if (res) {
                for (let i = res.length - 1; i >= 0; i--) {
                    if (res[i].state.ts === state.ts) {
                        if (state.val !== undefined) {
                            res[i].state.val = state.val;
                        }
                        if (state.q !== undefined && res[i].state.q !== undefined) {
                            res[i].state.q = state.q;
                        }
                        if (state.from !== undefined && res[i].from !== undefined) {
                            res[i].state.from = state.from;
                        }
                        if (state.ack !== undefined) {
                            res[i].state.ack = state.ack;
                        }
                        found = true;
                        break;
                    }
                }
            }
        }

        if (!found) {
            this.prepareTaskCheckTypeAndDbId(id, state, false, (err?: Error | null): void => {
                if (err) {
                    return cb?.(err);
                }

                const type = this.sqlDPs[id].type;

                const query = this.sqlFuncs!.update(
                    this.config.dbname,
                    this.sqlDPs[id].index,
                    state,
                    this.from[state.from],
                    dbNames[type],
                );

                if (!this.multiRequests) {
                    if (this.tasks.length > MAX_TASKS) {
                        const error = `Cannot queue new requests, because more than ${MAX_TASKS}`;
                        this.log.error(error);
                        cb?.(new Error(error));
                    } else {
                        this.tasks.push({ operation: 'query', query, id, callback: cb });
                        this.tasks.length === 1 && this.processTasks();
                    }
                } else {
                    this._executeQuery(query, id, cb);
                }
            });
        } else {
            cb?.();
        }
    }

    #delete(
        id: string,
        state: {
            ts?: number;
            start?: number;
            end?: number;
        },
        cb?: (err?: Error | null) => void,
    ): void {
        // first try to find the value in not yet saved data
        let found = false;
        if (this.sqlDPs[id]) {
            const res = this.sqlDPs[id].list;
            if (res) {
                if (!state.ts && !state.start && !state.end) {
                    this.sqlDPs[id].list = [];
                } else {
                    for (let i = res.length - 1; i >= 0; i--) {
                        if (state.start && state.end) {
                            if (res[i].state.ts >= state.start && res[i].state.ts <= state.end) {
                                res.splice(i, 1);
                            }
                        } else if (state.start) {
                            if (res[i].state.ts >= state.start) {
                                res.splice(i, 1);
                            }
                        } else if (state.end) {
                            if (res[i].state.ts <= state.end) {
                                res.splice(i, 1);
                            }
                        } else if (res[i].state.ts === state.ts) {
                            res.splice(i, 1);
                            found = true;
                            break;
                        }
                    }
                }
            }
        }

        if (!found) {
            this.prepareTaskCheckTypeAndDbId(id, state as unknown as IobDataEntryEx, false, err => {
                if (err) {
                    return cb?.(err);
                }

                const type = this.sqlDPs[id].type;

                let query;
                if (state.start && state.end) {
                    query = this.sqlFuncs!.deleteFromTable(
                        this.config.dbname,
                        dbNames[type],
                        this.sqlDPs[id].index,
                        state.start,
                        state.end,
                    );
                } else if (state.ts) {
                    query = this.sqlFuncs!.deleteFromTable(
                        this.config.dbname,
                        dbNames[type],
                        this.sqlDPs[id].index,
                        state.ts,
                    );
                } else {
                    // delete all entries for ID
                    query = this.sqlFuncs!.deleteFromTable(this.config.dbname, dbNames[type], this.sqlDPs[id].index);
                }

                if (!this.multiRequests) {
                    if (this.tasks.length > MAX_TASKS) {
                        const error = `Cannot queue new requests, because more than ${MAX_TASKS}`;
                        this.log.error(error);
                        cb?.(new Error(error));
                    } else {
                        this.tasks.push({ operation: 'query', query, id, callback: cb });
                        this.tasks.length === 1 && this.processTasks();
                    }
                } else {
                    this._executeQuery(query, id, cb);
                }
            });
        } else {
            cb?.();
        }
    }

    updateState(msg: ioBroker.Message): void {
        if (!msg.message) {
            this.log.error('updateState called with invalid data');
            return this.sendTo(
                msg.from,
                msg.command,
                {
                    error: `Invalid call: ${JSON.stringify(msg)}`,
                },
                msg.callback,
            );
        }
        let id;
        if (Array.isArray(msg.message)) {
            this.log.debug(`updateState ${msg.message.length} items`);
            for (let i = 0; i < msg.message.length; i++) {
                id = this.aliasMap[msg.message[i].id] ? this.aliasMap[msg.message[i].id] : msg.message[i].id;

                if (msg.message[i].state && typeof msg.message[i].state === 'object') {
                    this.update(id, msg.message[i].state);
                } else {
                    this.log.warn(`Invalid state for ${JSON.stringify(msg.message[i])}`);
                }
            }
        } else if (msg.message.state && Array.isArray(msg.message.state)) {
            this.log.debug(`updateState ${msg.message.state.length} items`);
            id = this.aliasMap[msg.message.id] ? this.aliasMap[msg.message.id] : msg.message.id;
            for (let j = 0; j < msg.message.state.length; j++) {
                if (msg.message.state[j] && typeof msg.message.state[j] === 'object') {
                    this.update(id, msg.message.state[j]);
                } else {
                    this.log.warn(`Invalid state for ${JSON.stringify(msg.message.state[j])}`);
                }
            }
        } else if (msg.message.id && msg.message.state && typeof msg.message.state === 'object') {
            this.log.debug('updateState 1 item');
            id = this.aliasMap[msg.message.id] ? this.aliasMap[msg.message.id] : msg.message.id;
            return this.update(id, msg.message.state, () =>
                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        success: true,
                        sqlConnected: !!this.clientPool,
                    },
                    msg.callback,
                ),
            );
        } else {
            this.log.error('updateState called with invalid data');
            return this.sendTo(
                msg.from,
                msg.command,
                {
                    error: `Invalid call: ${JSON.stringify(msg)}`,
                },
                msg.callback,
            );
        }

        this.sendTo(
            msg.from,
            msg.command,
            {
                success: true,
                sqlConnected: !!this.clientPool,
            },
            msg.callback,
        );
    }

    deleteHistoryEntry(msg: ioBroker.Message): void {
        if (!msg.message) {
            this.log.error('deleteHistoryEntry called with invalid data');
            return this.sendTo(
                msg.from,
                msg.command,
                {
                    error: `Invalid call: ${JSON.stringify(msg)}`,
                },
                msg.callback,
            );
        }
        let id;
        if (Array.isArray(msg.message)) {
            this.log.debug(`deleteHistoryEntry ${msg.message.length} items`);
            for (let i = 0; i < msg.message.length; i++) {
                id = this.aliasMap[msg.message[i].id] ? this.aliasMap[msg.message[i].id] : msg.message[i].id;

                // {id: 'blabla', ts: 892}
                if (msg.message[i].ts) {
                    this.#delete(id, { ts: msg.message[i].ts });
                } else if (msg.message[i].start) {
                    if (typeof msg.message[i].start === 'string') {
                        msg.message[i].start = new Date(msg.message[i].start).getTime();
                    }
                    if (typeof msg.message[i].end === 'string') {
                        msg.message[i].end = new Date(msg.message[i].end).getTime();
                    }
                    this.#delete(id, { start: msg.message[i].start, end: msg.message[i].end || Date.now() });
                } else if (
                    typeof msg.message[i].state === 'object' &&
                    msg.message[i].state &&
                    msg.message[i].state.ts
                ) {
                    this.#delete(id, { ts: msg.message[i].state.ts });
                } else if (
                    typeof msg.message[i].state === 'object' &&
                    msg.message[i].state &&
                    msg.message[i].state.start
                ) {
                    if (typeof msg.message[i].state.start === 'string') {
                        msg.message[i].state.start = new Date(msg.message[i].state.start).getTime();
                    }
                    if (typeof msg.message[i].state.end === 'string') {
                        msg.message[i].state.end = new Date(msg.message[i].state.end).getTime();
                    }
                    this.#delete(id, {
                        start: msg.message[i].state.start,
                        end: msg.message[i].state.end || Date.now(),
                    });
                } else {
                    this.log.warn(`Invalid state for ${JSON.stringify(msg.message[i])}`);
                }
            }
        } else if (msg.message.state && Array.isArray(msg.message.state)) {
            this.log.debug(`deleteHistoryEntry ${msg.message.state.length} items`);
            id = this.aliasMap[msg.message.id] ? this.aliasMap[msg.message.id] : msg.message.id;

            for (let j = 0; j < msg.message.state.length; j++) {
                if (msg.message.state[j] && typeof msg.message.state[j] === 'object') {
                    if (msg.message.state[j].ts) {
                        this.#delete(id, { ts: msg.message.state[j].ts });
                    } else if (msg.message.state[j].start) {
                        if (typeof msg.message.state[j].start === 'string') {
                            msg.message.state[j].start = new Date(msg.message.state[j].start).getTime();
                        }
                        if (typeof msg.message.state[j].end === 'string') {
                            msg.message.state[j].end = new Date(msg.message.state[j].end).getTime();
                        }
                        this.#delete(id, {
                            start: msg.message.state[j].start,
                            end: msg.message.state[j].end || Date.now(),
                        });
                    }
                } else if (msg.message.state[j] && typeof msg.message.state[j] === 'number') {
                    this.#delete(id, { ts: msg.message.state[j] });
                } else {
                    this.log.warn(`Invalid state for ${JSON.stringify(msg.message.state[j])}`);
                }
            }
        } else if (msg.message.ts && Array.isArray(msg.message.ts)) {
            this.log.debug(`deleteHistoryEntry ${msg.message.ts.length} items`);
            id = this.aliasMap[msg.message.id] ? this.aliasMap[msg.message.id] : msg.message.id;
            for (let j = 0; j < msg.message.ts.length; j++) {
                if (msg.message.ts[j] && typeof msg.message.ts[j] === 'number') {
                    this.#delete(id, { ts: msg.message.ts[j] });
                } else {
                    this.log.warn(`Invalid state for ${JSON.stringify(msg.message.ts[j])}`);
                }
            }
        } else if (msg.message.id && msg.message.state && typeof msg.message.state === 'object') {
            this.log.debug('deleteHistoryEntry 1 item');
            id = this.aliasMap[msg.message.id] ? this.aliasMap[msg.message.id] : msg.message.id;
            return this.#delete(id, { ts: msg.message.state.ts }, () =>
                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        success: true,
                        sqlConnected: !!this.clientPool,
                    },
                    msg.callback,
                ),
            );
        } else if (msg.message.id && msg.message.ts && typeof msg.message.ts === 'number') {
            this.log.debug('deleteHistoryEntry 1 item');
            id = this.aliasMap[msg.message.id] ? this.aliasMap[msg.message.id] : msg.message.id;
            return this.#delete(id, { ts: msg.message.ts }, () =>
                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        success: true,
                        sqlConnected: !!this.clientPool,
                    },
                    msg.callback,
                ),
            );
        } else {
            this.log.error('deleteHistoryEntry called with invalid data');
            return this.sendTo(msg.from, msg.command, { error: `Invalid call: ${JSON.stringify(msg)}` }, msg.callback);
        }

        this.sendTo(
            msg.from,
            msg.command,
            {
                success: true,
                sqlConnected: !!this.clientPool,
            },
            msg.callback,
        );
    }

    deleteStateAll(msg: ioBroker.Message): void {
        if (!msg.message) {
            this.log.error('deleteStateAll called with invalid data');
            return this.sendTo(
                msg.from,
                msg.command,
                {
                    error: `Invalid call: ${JSON.stringify(msg)}`,
                },
                msg.callback,
            );
        }
        let id;
        if (Array.isArray(msg.message)) {
            this.log.debug(`deleteStateAll ${msg.message.length} items`);
            for (let i = 0; i < msg.message.length; i++) {
                id = this.aliasMap[msg.message[i].id] ? this.aliasMap[msg.message[i].id] : msg.message[i].id;
                this.#delete(id, {});
            }
        } else if (msg.message.id) {
            this.log.debug('deleteStateAll 1 item');
            id = this.aliasMap[msg.message.id] ? this.aliasMap[msg.message.id] : msg.message.id;
            return this.#delete(id, {}, () =>
                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        success: true,
                        sqlConnected: !!this.clientPool,
                    },
                    msg.callback,
                ),
            );
        } else {
            this.log.error('deleteStateAll called with invalid data');
            return this.sendTo(msg.from, msg.command, { error: `Invalid call: ${JSON.stringify(msg)}` }, msg.callback);
        }

        this.sendTo(
            msg.from,
            msg.command,
            {
                success: true,
                sqlConnected: !!this.clientPool,
            },
            msg.callback,
        );
    }

    storeStatePushData(id: string, state: IobDataEntryEx, applyRules?: boolean): Promise<boolean> {
        if (!state || typeof state !== 'object') {
            throw new Error(`State ${JSON.stringify(state)} for ${id} is not valid`);
        }

        if (!this.sqlDPs[id]?.config) {
            if (applyRules) {
                throw new Error(`sql not enabled for ${id}, so can not apply the rules as requested`);
            }
            this.sqlDPs[id] ||= {} as SQLPointConfig;
            this.sqlDPs[id].realId = id;
        }
        return new Promise<boolean>((resolve, reject) => {
            if (applyRules) {
                this.pushHistory(id, state);
                resolve(true);
            } else {
                this.pushHelper(id, state, err => {
                    if (err) {
                        reject(
                            new Error(`Error writing state for ${id}: ${err.message}, Data: ${JSON.stringify(state)}`),
                        );
                    } else {
                        resolve(true);
                    }
                });
            }
        });
    }

    async storeState(msg: ioBroker.Message): Promise<void> {
        if (!msg.message) {
            this.log.error('storeState called with invalid data');
            return this.sendTo(
                msg.from,
                msg.command,
                {
                    error: `Invalid call: ${JSON.stringify(msg)}`,
                },
                msg.callback,
            );
        }

        const errors = [];
        let successCount = 0;
        if (Array.isArray(msg.message)) {
            this.log.debug(`storeState ${msg.message.length} items`);
            for (let i = 0; i < msg.message.length; i++) {
                const id = this.aliasMap[msg.message[i].id] ? this.aliasMap[msg.message[i].id] : msg.message[i].id;
                try {
                    await this.storeStatePushData(id, msg.message[i].state, msg.message[i].rules);
                    successCount++;
                } catch (err) {
                    errors.push(err.message);
                }
            }
        } else if (msg.message.id && Array.isArray(msg.message.state)) {
            this.log.debug(`storeState ${msg.message.state.length} items`);
            const id = this.aliasMap[msg.message.id] ? this.aliasMap[msg.message.id] : msg.message.id;
            for (let j = 0; j < msg.message.state.length; j++) {
                try {
                    await this.storeStatePushData(id, msg.message.state[j], msg.message.rules);
                    successCount++;
                } catch (err) {
                    errors.push(err.message);
                }
            }
        } else if (msg.message.id && msg.message.state) {
            this.log.debug('storeState 1 item');
            const id = this.aliasMap[msg.message.id] ? this.aliasMap[msg.message.id] : msg.message.id;
            try {
                await this.storeStatePushData(id, msg.message.state, msg.message.rules);
                successCount++;
            } catch (err) {
                errors.push(err.message);
            }
        } else {
            this.log.error('storeState called with invalid data');
            return this.sendTo(
                msg.from,
                msg.command,
                {
                    error: `Invalid call: ${JSON.stringify(msg)}`,
                },
                msg.callback,
            );
        }
        if (errors.length) {
            this.log.warn(`storeState executed with ${errors.length} errors: ${errors.join(', ')}`);
            return this.sendTo(
                msg.from,
                msg.command,
                {
                    error: `${errors.length} errors happened while storing data`,
                    errors: errors,
                    successCount,
                },
                msg.callback,
            );
        }

        this.log.debug(`storeState executed with ${successCount} states successfully`);

        this.sendTo(
            msg.from,
            msg.command,
            {
                success: true,
                successCount,
                sqlConnected: !!this.clientPool,
            },
            msg.callback,
        );
    }

    getDpOverview(msg: ioBroker.Message): void {
        const result: { [id: string]: { name: string; type?: 'number' | 'string' | 'boolean' } }[] = [];
        const query = this.sqlFuncs!.getIdSelect(this.config.dbname);
        this.log.info(query);

        this.borrowClientFromPool((err, client) => {
            if (err || !client) {
                this.returnClientToPool(client);
                return this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        error: `Cannot select ${query}: ${err || 'no client'}`,
                    },
                    msg.callback,
                );
            }
            client.execute<{ id: number; type: 0 | 1 | 2; name: string }>(query, (err, rows /* , fields */) => {
                if (err) {
                    this.returnClientToPool(client);

                    this.log.error(`Cannot select ${query}: ${err}`);

                    this.sendTo(
                        msg.from,
                        msg.command,
                        {
                            error: `Cannot select ${query}: ${err}`,
                        },
                        msg.callback,
                    );
                    return;
                }

                this.log.info(`Query result ${JSON.stringify(rows)}`);

                if (rows?.length) {
                    for (let r = 0; r < rows.length; r++) {
                        result[rows[r].type] ||= {};
                        result[rows[r].type][rows[r].id] = { name: rows[r].name };

                        switch (dbNames[rows[r].type]) {
                            case 'ts_number':
                                result[rows[r].type][rows[r].id].type = 'number';
                                break;

                            case 'ts_string':
                                result[rows[r].type][rows[r].id].type = 'string';
                                break;

                            case 'ts_bool':
                                result[rows[r].type][rows[r].id].type = 'boolean';
                                break;
                        }
                    }

                    this.log.info(`initialisation result: ${JSON.stringify(result)}`);
                    this.getFirstTsForIds(client, 0, result, msg);
                }
            });
        });
    }

    getFirstTsForIds(
        client: SQLClient,
        typeId: 0 | 1 | 2,
        resultData: { [id: string]: { name: string; type?: 'number' | 'string' | 'boolean'; ts?: number } }[],
        msg: ioBroker.Message,
    ): void {
        if (typeId < dbNames.length) {
            if (!resultData[typeId]) {
                this.getFirstTsForIds(client, ((typeId as number) + 1) as 0 | 1 | 2, resultData, msg);
            } else {
                const query = this.sqlFuncs!.getFirstTs(this.config.dbname, dbNames[typeId]);
                this.log.info(query);

                client.execute<{ id: number; ts: number }>(query, (err, rows /* , fields */) => {
                    if (err) {
                        this.returnClientToPool(client);

                        this.log.error(`Cannot select ${query}: ${err}`);
                        this.sendTo(
                            msg.from,
                            msg.command,
                            {
                                error: `Cannot select ${query}: ${err}`,
                            },
                            msg.callback,
                        );
                        return;
                    }

                    this.log.info(`Query result ${JSON.stringify(rows)}`);

                    if (rows?.length) {
                        for (let r = 0; r < rows.length; r++) {
                            if (resultData[typeId][rows[r].id]) {
                                resultData[typeId][rows[r].id].ts = rows[r].ts;
                            }
                        }
                    }

                    this.log.info(`enhanced result (${typeId}): ${JSON.stringify(resultData)}`);
                    this.dpOverviewTimeout = setTimeout(
                        (_client, typeId, resultData, msg) => {
                            this.dpOverviewTimeout = null;
                            this.getFirstTsForIds(_client, typeId as 0 | 1 | 2, resultData, msg);
                        },
                        5000,
                        client,
                        typeId + 1,
                        resultData,
                        msg,
                    );
                });
            }
        } else {
            this.returnClientToPool(client);

            this.log.info('consolidate data ...');
            const result: { [id: string]: { type: 'number' | 'string' | 'boolean' | 'undefined'; ts?: number } } = {};
            for (let ti = 0; ti < dbNames.length; ti++) {
                if (resultData[ti]) {
                    for (const index in resultData[ti]) {
                        if (!Object.prototype.hasOwnProperty.call(resultData[ti], index)) {
                            continue;
                        }

                        const id = resultData[ti][index].name;
                        if (!result[id]) {
                            result[id] = {
                                type: resultData[ti][index].type || 'undefined',
                                ts: resultData[ti][index].ts,
                            };
                        } else {
                            result[id].type = 'undefined';
                            if (
                                result[id].ts !== undefined &&
                                (resultData[ti][index].ts === undefined || resultData[ti][index].ts! < result[id].ts)
                            ) {
                                result[id].ts = resultData[ti][index].ts;
                            }
                        }
                    }
                }
            }
            this.log.info(`Result: ${JSON.stringify(result)}`);
            this.sendTo(
                msg.from,
                msg.command,
                {
                    success: true,
                    result: result,
                },
                msg.callback,
            );
        }
    }

    enableHistory(msg: ioBroker.Message): void {
        if (!msg.message?.id) {
            this.log.error('enableHistory called with invalid data');
            this.sendTo(msg.from, msg.command, { error: 'Invalid call' }, msg.callback);
            return;
        }

        const obj: ioBroker.StateObject = {} as ioBroker.StateObject;
        obj.common = {} as ioBroker.StateCommon;
        obj.common.custom = {};
        obj.common.custom[this.namespace] = msg.message.options || {};
        obj.common.custom[this.namespace].enabled = true;

        this.extendForeignObject(msg.message.id, obj, err => {
            if (err) {
                this.log.error(`enableHistory: ${err}`);
                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        error: err,
                    },
                    msg.callback,
                );
            } else {
                this.log.info(JSON.stringify(obj));
                this.sendTo(msg.from, msg.command, { success: true }, msg.callback);
            }
        });
    }

    disableHistory(msg: ioBroker.Message): void {
        if (!msg.message?.id) {
            this.log.error('disableHistory called with invalid data');
            return this.sendTo(
                msg.from,
                msg.command,
                {
                    error: 'Invalid call',
                },
                msg.callback,
            );
        }

        const obj: ioBroker.StateObject = {} as ioBroker.StateObject;
        obj.common = {} as ioBroker.StateCommon;
        obj.common.custom = {
            [this.namespace]: {
                enabled: false,
            },
        };

        this.extendForeignObject(msg.message.id, obj, err => {
            if (err) {
                this.log.error(`disableHistory: ${err}`);
                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        error: err,
                    },
                    msg.callback,
                );
            } else {
                this.log.info(JSON.stringify(obj));
                this.sendTo(msg.from, msg.command, { success: true }, msg.callback);
            }
        });
    }

    getEnabledDPs(msg: ioBroker.Message): void {
        const data: { [id: string]: SqlCustomConfigTyped } = {};
        for (const id in this.sqlDPs) {
            if (Object.prototype.hasOwnProperty.call(this.sqlDPs, id) && this.sqlDPs[id]?.config?.enabled) {
                data[this.sqlDPs[id].realId] = this.sqlDPs[id].config;
            }
        }

        this.sendTo(msg.from, msg.command, data, msg.callback);
    }

    getDockerConfigMySQL(config: SqlAdapterConfigTyped, dockerAutoImageUpdate?: boolean): ContainerConfig {
        config.dockerMysql ||= {
            enabled: false,
        };
        config.dbtype = 'mysql';
        config.dbname = 'iobroker';
        config.user = 'iobroker';
        config.password = 'iobroker';
        config.dockerMysql.port = parseInt((config.dockerMysql.port as string) || '3306', 10) || 3306;
        config.port = config.dockerMysql.port;
        config.multiRequests = true;
        config.maxConnections = 100;
        return {
            iobEnabled: true,
            iobMonitoringEnabled: true,
            iobAutoImageUpdate: !!dockerAutoImageUpdate,
            iobStopOnUnload: config.dockerMysql.stopIfInstanceStopped || false,

            // influxdb image: https://hub.docker.com/_/influxdb. Only version 2 is supported
            image: 'mysql:lts',
            ports: [
                {
                    hostPort: config.dockerMysql.port,
                    containerPort: 3306,
                    hostIP: config.dockerMysql.bind || '127.0.0.1', // only localhost to disable authentication and https safely
                },
            ],
            mounts: [
                {
                    source: 'mysql_data',
                    target: '/var/lib/mysql',
                    type: 'volume',
                    iobBackup: true,
                },
                {
                    source: 'mysql_config',
                    target: '/etc/mysql/conf.d',
                    type: 'volume',
                },
            ],
            networkMode: true, // take default name iob_influxdb_<instance>
            // influxdb v2 requires some environment variables to be set on first start
            environment: {
                MYSQL_ROOT_PASSWORD: config.dockerMysql.rootPassword || 'root_iobroker',
                MYSQL_DATABASE: 'iobroker',
                MYSQL_USER: 'iobroker',
                MYSQL_PASSWORD: 'iobroker',
                MYSQL_ALLOW_EMPTY_PASSWORD: 'false',
            },
        };
    }

    getDockerConfigPhpMyAdmin(config: SqlAdapterConfigTyped): ContainerConfig {
        config.dockerPhpMyAdmin ||= {
            enabled: false,
        };
        config.dockerPhpMyAdmin.port = parseInt((config.dockerPhpMyAdmin.port as string) || '8080', 10) || 8080;
        return {
            iobEnabled: config.dockerPhpMyAdmin.enabled !== false,
            iobMonitoringEnabled: true,
            iobAutoImageUpdate: !!config.dockerPhpMyAdmin.autoImageUpdate,
            // Stop docker too if influxdb is stopped
            iobStopOnUnload:
                config.dockerMysql?.stopIfInstanceStopped || config.dockerPhpMyAdmin.stopIfInstanceStopped || false,

            // influxdb image: https://hub.docker.com/_/influxdb. Only version 2 is supported
            image: 'phpmyadmin/latest',
            name: 'phpmyadmin',
            ports: [
                {
                    hostPort: config.dockerPhpMyAdmin.port,
                    containerPort: 8080,
                    hostIP: config.dockerPhpMyAdmin.bind || '0.0.0.0', // only localhost to disable authentication and https safely
                },
            ],
            networkMode: true, // take default name iob_influxdb_<instance>
            environment: {
                MYSQL_ROOT_PASSWORD: config.dockerMysql.rootPassword || 'root_iobroker',
                MYSQL_USER: 'iobroker',
                MYSQL_PASSWORD: 'iobroker',
                PMA_ABSOLUTE_URI: config.dockerPhpMyAdmin.absoluteUri || '',
                PMA_PORT: (config.dockerMysql.port || 3306).toString(),
                PMA_HOST: `iob_${this.namespace.replace(/[-.]/g, '_')}`,
            },
        };
    }

    normalizeAdapterConfig(config: SqlAdapterConfig): SqlAdapterConfigTyped {
        config.dbname ||= 'iobroker';

        if (config.writeNulls === undefined) {
            config.writeNulls = true;
        }

        config.retention = parseInt(config.retention as string, 10) || 0;
        if (config.retention === -1) {
            // Custom timeframe
            config.retention = (parseInt(config.customRetentionDuration as string, 10) || 0) * 24 * 60 * 60;
        }
        config.debounce = parseInt(config.debounce as string, 10) || 0;
        config.requestInterval =
            config.requestInterval === undefined || config.requestInterval === null || config.requestInterval === ''
                ? 0
                : parseInt(config.requestInterval as string, 10) || 0;

        if (config.changesRelogInterval !== null && config.changesRelogInterval !== undefined) {
            config.changesRelogInterval = parseInt(config.changesRelogInterval as string, 10);
        } else {
            config.changesRelogInterval = 0;
        }

        if (!clients[config.dbtype]) {
            this.log.error(`Unknown DB type: ${config.dbtype}`);
            this.stop?.();
        }
        if (config.multiRequests !== undefined && config.dbtype !== 'sqlite') {
            clients[config.dbtype].multiRequests = config.multiRequests;
        }
        if (config.maxConnections !== undefined && config.dbtype !== 'sqlite') {
            config.maxConnections = parseInt(config.maxConnections as string, 10);
            if (config.maxConnections !== 0 && !config.maxConnections) {
                config.maxConnections = 100;
            }
        } else {
            config.maxConnections = 1; // SQLite does not support multiple connections
        }

        if (config.changesMinDelta !== null && config.changesMinDelta !== undefined) {
            config.changesMinDelta = parseFloat(config.changesMinDelta.toString().replace(/,/g, '.'));
        } else {
            config.changesMinDelta = 0;
        }

        if (config.blockTime !== null && config.blockTime !== undefined) {
            config.blockTime = parseInt(config.blockTime as string, 10) || 0;
        } else {
            if (config.debounce !== null && config.debounce !== undefined) {
                config.debounce = parseInt(config.debounce as unknown as string, 10) || 0;
            } else {
                config.blockTime = 0;
            }
        }

        if (config.debounceTime !== null && config.debounceTime !== undefined) {
            config.debounceTime = parseInt(config.debounceTime as string, 10) || 0;
        } else {
            config.debounceTime = 0;
        }

        if (config.maxLength !== null && config.maxLength !== undefined) {
            config.maxLength = parseInt(config.maxLength as string, 10) || 0;
        } else {
            config.maxLength = 0;
        }

        this.multiRequests = clients[config.dbtype].multiRequests;
        if (!this.multiRequests) {
            config.writeNulls = false;
        }

        config.port = parseInt(config.port as string, 10) || 0;

        if (config.round !== null && config.round !== undefined && config.round !== '') {
            config.round = parseInt(config.round as string, 10);
            if (!isFinite(config.round) || config.round < 0) {
                config.round = null;
                this.log.info(`Invalid round value: ${config.round} - ignore, do not round values`);
            } else {
                config.round = Math.pow(10, config.round);
            }
        } else {
            config.round = null;
        }

        return config as SqlAdapterConfigTyped;
    }

    async createUserInDocker(): Promise<void> {
        const mySQLOptions: MySQLOptions = {
            host: this.config.host, // needed for PostgreSQL , MySQL
            user: 'root',
            password: this.config.dockerMysql.rootPassword || 'root_iobroker',
            port: this.config.port || undefined,
            ssl: this.config.encrypt
                ? {
                      rejectUnauthorized: !!this.config.rejectUnauthorized,
                  }
                : undefined,
        };
        const client = new MySQL2Client(mySQLOptions);
        await client.connectAsync();
        // Show all users
        const exists = await client.executeAsync<{ ex: 0 | 1 }>(
            `SELECT EXISTS(SELECT 1 FROM mysql.user WHERE user = 'iobroker') as "ex";`,
        );
        if (exists?.[0]?.ex !== 1) {
            // create user
            await client.executeAsync(`CREATE USER 'iobroker'@'%' IDENTIFIED BY 'iobroker';`);
            await client.executeAsync(`GRANT ALL PRIVILEGES ON * . * TO 'iobroker'@'%';`);
            await client.executeAsync(`FLUSH PRIVILEGES;`);
        }
        await client.disconnectAsync();
    }

    async main(): Promise<void> {
        this.setConnected(false);

        // set default history if not yet set
        try {
            const obj = await this.getForeignObjectAsync('system.config');
            if (obj?.common && !obj.common.defaultHistory) {
                obj.common.defaultHistory = this.namespace;
                await this.setForeignObjectAsync('system.config', obj);
                this.log.info(`Set default history instance to "${this.namespace}"`);
            }
        } catch (e) {
            this.log.error(`Cannot get system config: ${e}`);
        }
        // Normalize adapter config
        const config = this.normalizeAdapterConfig(this.config);

        this.sqlFuncs = SQLFuncs[config.dbtype];

        // Start docker if configured
        if (this.config.dockerMysql?.enabled) {
            const containerConfigs: ContainerConfig[] = [];
            containerConfigs.push(this.getDockerConfigMySQL(this.config, this.config.dockerMysql?.autoImageUpdate));

            if (this.config.dockerPhpMyAdmin) {
                containerConfigs.push(this.getDockerConfigPhpMyAdmin(this.config));
            }
            this.dockerManager = new DockerManager(this, undefined, containerConfigs);
            await this.dockerManager.allOwnContainersChecked();

            // Todo: Check that the user 'iobroker' exists
            await this.createUserInDocker();
        }

        if (config.dbtype === 'sqlite' || this.config.host) {
            this.connect(() => {
                // read all custom settings
                this.getObjectView('system', 'custom', {}, (err, doc) => {
                    let count = 0;
                    if (doc?.rows) {
                        for (let i = 0, l = doc.rows.length; i < l; i++) {
                            if (doc.rows[i].value?.[this.namespace]) {
                                let id: string = doc.rows[i].id;
                                const realId = id;
                                if (doc.rows[i].value[this.namespace] && doc.rows[i].value[this.namespace].aliasId) {
                                    this.aliasMap[id] = doc.rows[i].value[this.namespace].aliasId;
                                    this.log.debug(`Found Alias: ${id} --> ${this.aliasMap[id]}`);
                                    id = this.aliasMap[id];
                                }

                                let storedIndex: number | null = null;
                                let storedType: 0 | 1 | 2 | null = null;
                                if (this.sqlDPs[id] && this.sqlDPs[id].index !== undefined) {
                                    storedIndex = this.sqlDPs[id].index;
                                }
                                if (this.sqlDPs[id] && this.sqlDPs[id].dbType !== undefined) {
                                    storedType = this.sqlDPs[id].dbType;
                                }
                                const config = this.normalizeCustomConfig(doc.rows[i].value as SqlCustomConfig);

                                this.sqlDPs[id] ||= {} as SQLPointConfig;
                                const sqlDP = this.sqlDPs[id];
                                sqlDP.config = config;
                                if (storedIndex !== null) {
                                    sqlDP.index = storedIndex;
                                }
                                if (storedType !== null) {
                                    sqlDP.dbType = storedType;
                                }

                                if (!config || typeof config !== 'object' || config.enabled === false) {
                                    delete this.sqlDPs[id];
                                } else {
                                    count++;
                                    this.log.info(
                                        `enabled logging of ${id}, Alias=${id !== realId}, ${count} points now activated`,
                                    );

                                    // relogTimeout
                                    if (config.changesOnly && config.changesRelogInterval > 0) {
                                        if (sqlDP.relogTimeout) {
                                            clearTimeout(sqlDP.relogTimeout);
                                        }
                                        sqlDP.relogTimeout = setTimeout(
                                            _id => {
                                                this.sqlDPs[_id].relogTimeout = null;
                                                this.reLogHelper(_id);
                                            },
                                            sqlDP.config.changesRelogInterval * 500 * (1 + Math.random()),
                                            id,
                                        );
                                    }

                                    sqlDP.realId = realId;
                                    sqlDP.list ||= [];
                                    sqlDP.inFlight ||= {};

                                    // randomize lastCheck to avoid all datapoints to be checked at the same timepoint
                                    sqlDP.lastCheck = Date.now() - Math.floor(Math.random() * 21600000 /* 6 hours */);
                                }
                            }
                        }
                    }

                    if (this.config.writeNulls) {
                        this.writeNulls();
                    }

                    if (count < 200) {
                        Object.keys(this.sqlDPs).forEach(
                            id =>
                                this.sqlDPs[id]?.config &&
                                !this.sqlDPs[id].realId &&
                                this.log.warn(`No realID found for ${id}`),
                        );

                        this.subscribeForeignStates(
                            Object.keys(this.sqlDPs).filter(id => this.sqlDPs[id]?.config && this.sqlDPs[id].realId),
                        );
                    } else {
                        this.subscribeAll = true;
                        this.subscribeForeignStates('*');
                    }
                    this.subscribeForeignObjects('*');
                    this.log.debug('Initialization done');
                    this.setConnected(true);
                    this.processStartValues();

                    // store all buffered data every 10 minutes to not lost the data
                    this.bufferChecker = setInterval(() => this.storeCached(), 10 * 60000);
                });
            });
        }
    }
}

// If started as allInOne mode => return function to create instance
if (require.main !== module) {
    // Export the constructor in compact mode
    module.exports = (options: Partial<AdapterOptions> | undefined) => new SqlAdapter(options);
} else {
    // otherwise start the instance directly
    (() => new SqlAdapter())();
}
