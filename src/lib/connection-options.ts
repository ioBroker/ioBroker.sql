import type { MySQLOptions } from './optional-drivers';
import type { SqlAdapterConfigTyped } from '../types';

export type { MySQLOptions };

/**
 * Build the mysql2 connection options for a configuration.
 *
 * `socketPath` wins over host and port: mysql2 connects through the unix socket and ignores them,
 * so passing them on would only make the logged options and the error messages misleading. A unix
 * socket is the only way to reach a database on the Docker host from a container on a macvlan
 * network, and it is faster than TCP on the same machine. See
 * https://github.com/ioBroker/ioBroker.sql/issues/104
 *
 * This lives outside `main.ts` on purpose: importing `main.ts` pulls in `@iobroker/adapter-core`,
 * which calls `process.exit(10)` at module load time when js-controller cannot be resolved. That
 * makes anything importing it unusable as a plain unit test, because js-controller is not a
 * dependency of this repository - the integration suite installs it into `tmp/` at runtime.
 *
 * @param config the normalized adapter configuration
 */
export function buildMySQLOptions(config: SqlAdapterConfigTyped): MySQLOptions {
    const ssl = config.encrypt ? { rejectUnauthorized: !!config.rejectUnauthorized } : undefined;

    if (config.socketPath) {
        return {
            socketPath: config.socketPath,
            user: config.user || '',
            password: config.password || '',
            ssl,
        };
    }

    return {
        host: config.host, // needed for PostgreSQL , MySQL
        user: config.user || '',
        password: config.password || '',
        port: config.port || undefined,
        ssl,
    };
}
