export type DbType = 'sqlite' | 'postgresql' | 'mysql' | 'mssql';
export type TableName = 'ts_string' | 'ts_number' | 'ts_bool' | 'ts_counter';
export type StorageType = '' | 'String' | 'Number' | 'Boolean';
export interface SqlCustomConfigTyped {
    enabled: boolean;
    debounceTime: number;
    blockTime: number;
    changesOnly: boolean;
    changesRelogInterval: number;
    changesMinDelta: number;
    ignoreBelowNumber: number | null;
    ignoreAboveNumber: number | null;
    ignoreZero: boolean;
    disableSkippedValueLogging: boolean;
    storageType: StorageType | false;
    counter: boolean;
    aliasId: string;
    retention: number;
    customRetentionDuration: number;
    maxLength: number;
    round: number;
    enableDebugLogs: boolean;
    debounce: number;
    ignoreBelowZero: boolean;
}

export interface SqlCustomConfig extends SqlCustomConfigTyped {
    enabled: boolean | 'true';
    debounceTime: number | string;
    changesOnly: boolean | 'true';
    blockTime: number | string;
    changesRelogInterval: number | string;
    changesMinDelta: number | string;
    ignoreZero: boolean | 'true';
    enableDebugLogs: boolean | 'true';
    ignoreBelowNumber: number | string | null;
    ignoreAboveNumber: number | string | null;
    counter: boolean | 'true';
    disableSkippedValueLogging: boolean | 'true';
    retention: number | string;
    ignoreBelowZero: boolean | 'true';
    customRetentionDuration: number | string;
    maxLength: number | string;
    round: number | string;
    debounce: number | string;
}
export interface SqlAdapterConfigTyped {
    connLink: string;
    debounce: number;
    retention: number;
    host: string;
    port: number;
    user: string;
    password: string;
    dbtype: DbType;
    fileName: string;
    requestInterval: number;
    encrypt: boolean;
    round: number | null;
    dbname: string;
    multiRequests: boolean;
    maxConnections: number;
    changesRelogInterval: number;
    changesMinDelta: number;
    writeNulls: boolean;
    doNotCreateDatabase: boolean;
    maxLength: number;
    blockTime: number;
    debounceTime: number;
    disableSkippedValueLogging: boolean;
    enableDebugLogs: boolean;
    rejectUnauthorized: boolean;
    customRetentionDuration: number;
    dockerMysql: {
        enabled?: boolean;
        bind?: string;
        stopIfInstanceStopped?: true;
        port?: string | number;
        autoImageUpdate?: boolean;
        rootPassword?: string;
    };
    dockerPhpMyAdmin: {
        enabled?: boolean;
        bind?: string;
        stopIfInstanceStopped?: boolean;
        port?: string | number;
        autoImageUpdate?: boolean;
        absoluteUri?: string;
    };
}

export interface SqlAdapterConfig extends SqlAdapterConfigTyped {
    connLink: string;
    debounce: number | string;
    retention: number | string;
    host: string;
    port: number | string;
    user: string;
    password: string;
    dbtype: DbType;
    fileName: string;
    requestInterval: number | string;
    encrypt: boolean;
    round: number | string | null;
    dbname: string;
    multiRequests: boolean;
    maxConnections: number | string;
    changesRelogInterval: number | string;
    changesMinDelta: number | string;
    writeNulls: boolean;
    doNotCreateDatabase: boolean;
    maxLength: number | string;
    blockTime: number | string;
    debounceTime: number | string;
    disableSkippedValueLogging: boolean;
    enableDebugLogs: boolean;
    rejectUnauthorized: boolean;
    customRetentionDuration: number | string;
}
