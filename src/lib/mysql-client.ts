import { ConnectionFactory, type SQLConnection } from './connection-factory';
import SQLClient from './sql-client';
import { SQLClientPool, type PoolConfig } from './sql-client-pool';

import type { Connection, ConnectionOptions as MySQLOptions } from 'mysql2';

export type { MySQLOptions };

class MySQL2ConnectionFactory extends ConnectionFactory {
    private createConnection: any;
    openConnection(options: MySQLOptions, callback: (err: Error | null, connection: Connection) => void): void {
        if (!this.createConnection) {
            void import('mysql2').then(mysql2 => {
                this.createConnection = mysql2.default.createConnection;
                this.openConnection(options, callback);
            });
            return;
        }

        const connection = this.createConnection(options);
        connection.connect((err: Error | null): void => callback(err, connection));
    }

    closeConnection(connection: Connection | null | undefined, callback: (error?: Error | null) => void): void {
        if (connection) {
            connection.end(callback);
        } else {
            callback?.(null);
        }
    }

    execute<T>(
        connection: SQLConnection,
        sql: string,
        callback: (err: Error | null | undefined, results?: Array<T>) => void,
    ): void {
        connection.execute(sql, (err: Error | null | undefined, results: Array<T>) => {
            if (err) {
                return callback(err);
            }
            return callback(null, results);
        });
    }
}

export class MySQL2Client extends SQLClient {
    constructor(sqlConnection: MySQLOptions) {
        super(sqlConnection, new MySQL2ConnectionFactory());
    }
}

export class MySQL2ClientPool extends SQLClientPool {
    constructor(poolOptions: PoolConfig, sqlOptions: MySQLOptions) {
        super(poolOptions, sqlOptions, new MySQL2ConnectionFactory());
    }
}
