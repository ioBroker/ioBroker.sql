import { ConnectionFactory } from './connection-factory';
import SQLClient from './sql-client';
import { SQLClientPool, type PoolConfig } from './sql-client-pool';

import type { ConnectionPool, IResult, Request, config as MSSQLOptions } from 'mssql';

export type { MSSQLOptions };

class MSSQLConnectionFactory extends ConnectionFactory {
    private ConnectionPool: typeof ConnectionPool | undefined;
    private Request: typeof Request | undefined;

    openConnection(options: MSSQLOptions, callback: (err: Error | null, connection?: ConnectionPool) => void): void {
        if (!this.ConnectionPool) {
            void import('mssql').then(mssql => {
                this.ConnectionPool = mssql.default.ConnectionPool;
                this.Request = mssql.default.Request;
                this.openConnection(options, callback);
            });
            return;
        }
        const pos = options.server.indexOf(':');
        if (pos !== -1) {
            options.port = parseInt(options.server.substring(pos + 1), 10);
            options.server = options.server.substring(0, pos);
        }
        options.pool ||= {};
        options.pool.min = 0;
        options.pool.max = 1;

        const connection = new this.ConnectionPool(options, (err?: string | null): void => {
            if (err) {
                callback(new Error(err));
            } else {
                callback(null, connection);
            }
        });
    }

    closeConnection(connection: ConnectionPool, callback: (err?: Error | null) => void): void {
        connection.close(callback);
    }

    execute<T>(
        connection: ConnectionPool,
        sql: string,
        callback: (err: Error | null | undefined, result?: Array<T>) => void,
    ): void {
        if (!this.Request) {
            throw new Error('Not initialized request');
        }
        const request = new this.Request(connection);
        request.query(sql, (err?: Error | null, result?: IResult<T>) => {
            callback(err, result?.recordset);
        });
    }
}

export class MSSQLClient extends SQLClient {
    constructor(options: MSSQLOptions) {
        super(options, new MSSQLConnectionFactory());
    }
}

export class MSSQLClientPool extends SQLClientPool {
    constructor(poolOptions: PoolConfig, sqlOptions: MSSQLOptions) {
        super(poolOptions, sqlOptions, new MSSQLConnectionFactory());
    }
}
