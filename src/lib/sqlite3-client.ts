import { ConnectionFactory } from './connection-factory';
import SQLClient from './sql-client';
import { SQLClientPool, type PoolConfig } from './sql-client-pool';

import type { Database } from 'sqlite3';

type SQLite3Options = { fileName: string; mode?: number };

export type { SQLite3Options };

export class SQLite3ConnectionFactory extends ConnectionFactory {
    private Database: typeof Database | undefined;

    openConnection(options: SQLite3Options, callback: (err: Error | null, connection?: Database) => void): void {
        if (!this.Database) {
            void import('sqlite3').then(sqlite3 => {
                this.Database = sqlite3.default.Database;
                this.openConnection(options, callback);
            });
            return;
        }

        if (options.mode) {
            const db = new this.Database(options.fileName, options.mode, (err: Error | null): void => {
                if (err) {
                    callback(err);
                } else {
                    callback(null, db);
                }
            });
            return;
        }
        const db = new this.Database(options.fileName, (err: Error | null): void => {
            if (err) {
                callback(err);
            } else {
                callback(null, db);
            }
        });
    }

    closeConnection(db: Database, callback?: (err?: Error | null) => void): void {
        if (db) {
            db.close(callback);
        } else {
            callback?.();
        }
    }

    execute<T>(db: Database, sql: string, callback: (err: Error | null, result?: Array<T>) => void): void {
        db.all(sql, [], callback);
    }
}

export class SQLite3Client extends SQLClient {
    constructor(sqliteOptions: SQLite3Options) {
        super(sqliteOptions, new SQLite3ConnectionFactory());
    }
}

export class SQLite3ClientPool extends SQLClientPool {
    constructor(poolOptions: PoolConfig, sqliteOptions: SQLite3Options) {
        super(poolOptions, sqliteOptions, new SQLite3ConnectionFactory());
    }
}
