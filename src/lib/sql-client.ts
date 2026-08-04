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
                callback?.();
            });
        }
        callback?.();
    }

    connectAsync(): Promise<void> {
        if (!this.connection) {
            return new Promise<void>((resolve, reject) =>
                this.factory.openConnection(this.options, (err, connection) => {
                    if (err) {
                        reject(err);
                    } else {
                        this.connection = connection;
                        this.connected_at = Date.now();
                        resolve();
                    }
                }),
            );
        }
        return Promise.resolve();
    }

    disconnect(callback?: (err?: Error) => void): void {
        if (this.connection) {
            this.factory.closeConnection(this.connection, err => {
                if (err) {
                    callback?.(err);
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

    disconnectAsync(): Promise<void> {
        if (this.connection) {
            return new Promise<void>((resolve, reject) =>
                this.factory.closeConnection(this.connection, err => {
                    if (err) {
                        reject(err);
                    } else {
                        this.connection = null;
                        this.connected_at = null;
                        resolve();
                    }
                }),
            );
        }
        return Promise.resolve();
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

    async executeAsync<T>(sql: string): Promise<Array<T> | undefined> {
        if (!this.connection) {
            await this.connectAsync();
        }
        return new Promise<Array<T> | undefined>((resolve, reject) =>
            this.factory.execute(this.connection, sql, (err?: Error | null, result?: Array<T>): void => {
                if (err) {
                    reject(err);
                } else {
                    resolve(result);
                }
            }),
        );
    }
}
