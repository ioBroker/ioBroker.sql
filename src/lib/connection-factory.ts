export type SQLConnection = any;

export abstract class ConnectionFactory {
    abstract openConnection(options: any, callback: (err: Error | null, connection?: SQLConnection) => void): void;
    abstract closeConnection(connection: SQLConnection, callback?: (err?: Error | null) => void): void;
    abstract execute<T>(
        connection: SQLConnection,
        sql: string,
        callback: (err: Error | null | undefined, results?: Array<T>) => void,
    ): void;
}
