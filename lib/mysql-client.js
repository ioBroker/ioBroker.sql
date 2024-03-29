// Generated by CoffeeScript 2.6.0
(function() {
    const ConnectionFactory = require('sql-client/lib/connection-factory').ConnectionFactory;
    const SQLClient         = require('sql-client/lib/sql-client').SQLClient;
    const SQLClientPool     = require('sql-client/lib/sql-client-pool').SQLClientPool;
    const mysql2 = require('mysql2');

    const MySQL2ConnectionFactory = class MySQL2ConnectionFactory extends ConnectionFactory {
        constructor() {
            super(...arguments);
            this.open_connection = this.open_connection.bind(this);
            this.close_connection = this.close_connection.bind(this);
        }

        open_connection(options, callback) {
            const opt = Object.assign({}, options);
            delete opt.max_idle;
            delete opt.max_active;
            delete opt.max_wait;
            delete opt.when_exhausted;
            delete opt.options;
            delete opt.server;

            let connection = mysql2.createConnection(opt);
            return connection.connect((err) => {
                return callback(err, connection);
            });
        }

        close_connection(connection, callback) {
            if (typeof (connection != null ? connection.end : void 0) === 'function') {
                return connection.end(callback);
            } else {
                return super.close_connection(connection, callback);
            }
        }

    };

    const MySQL2Client = class MySQL2Client extends SQLClient {
        constructor(...options) {

            super(...options, new MySQL2ConnectionFactory());
        }

    };

    const MySQL2ClientPool = class MySQL2ClientPool extends SQLClientPool {
        constructor(...options) {

            super(...options, new MySQL2ConnectionFactory());
        }

    };

    exports.MySQL2ConnectionFactory = MySQL2ConnectionFactory;

    exports.MySQL2Client = MySQL2Client;

    exports.MySQL2ClientPool = MySQL2ClientPool;

}).call(this);
