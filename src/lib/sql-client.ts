import type { ConnectionFactory, SQLConnection } from './connection-factory';
import { EventEmitter } from 'node:events';

export default class SQLClient extends EventEmitter {
    private readonly options: any;
    private factory: ConnectionFactory;
    public pooled_at: number | null = null;
    public borrowed_at: number | null = null;
    public connected_at: number | null = null;
    private connection: SQLConnection;

    constructor(options: any, connectionFactory: ConnectionFactory) {
        super();
        this.options = options;
        this.factory = connectionFactory;
    }

    connect(callback?: (err?: Error) => void): void {
        if (!this.connection) {
            return this.factory.openConnection(this.options, (err, connection) => {
                if (err) {
                    callback?.(err);
                    return;
                }
                this.connection = connection;
                this.connected_at = Date.now();
                return callback?.();
            });
        }
        return callback?.();
    }

    disconnect(callback?: (err?: Error, connection?: SQLConnection) => void): void {
        if (this.connection) {
            this.factory.closeConnection(this.connection, err => {
                if (err) {
                    callback?.(err, this.connection);
                    return;
                }
                this.connection = null;
                this.connected_at = null;
                callback?.();
            });
            return;
        }
        return callback?.();
    }

    execute<T>(sql: string, callback: (err: Error | null, result?: Array<T>) => void): void {
        if (!this.connection) {
            return this.connect(err => {
                if (err) {
                    callback(err);
                } else {
                    this.execute(sql, callback);
                }
            });
        }
        this.factory.execute(this.connection, sql, (err?: Error | null, result?: Array<T>): void => {
            if (err) {
                callback(err);
            } else {
                callback(null, result);
            }
        });
    }
}
