import { ConnectionFactory } from './connection-factory';
import SQLClient from './sql-client';
import { SQLClientPool, type PoolConfig } from './sql-client-pool';

import {
    importDriver,
    type SQLite3Database,
    type SQLite3DatabaseConstructor,
    type SQLite3Module,
} from './optional-drivers';

type SQLite3Options = { fileName: string; mode?: number };

export type { SQLite3Options };

export class SQLite3ConnectionFactory extends ConnectionFactory {
    private Database: SQLite3DatabaseConstructor | undefined;

    openConnection(options: SQLite3Options, callback: (err: Error | null, connection?: SQLite3Database) => void): void {
        if (!this.Database) {
            void importDriver<SQLite3Module>('sqlite3').then(
                sqlite3 => {
                    this.Database = sqlite3.default.Database;
                    this.openConnection(options, callback);
                },
                // sqlite3 is an optional dependency, so report a missing driver instead of
                // letting the rejection escape as an unhandled promise rejection
                e => callback(new Error(`Node.js DB driver "sqlite3" could not be loaded: ${e}`)),
            );
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

    closeConnection(db: SQLite3Database, callback?: (err?: Error | null) => void): void {
        if (db) {
            db.close(callback);
        } else {
            callback?.();
        }
    }

    execute<T>(db: SQLite3Database, sql: string, callback: (err: Error | null, result?: Array<T>) => void): void {
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
