![Logo](admin/sql.png)
# ioBroker.sql

This adapter saves state history into SQL DB.

Supports PostgreSQL, mysql, Microsoft SQL Server and sqlite.
You can leave port 0 if default port is desired.

### MS-SQL:
Use ```localhost\instance``` for host and check that TCP/IP connections are enabled. 
https://msdn.microsoft.com/en-us/library/bb909712(v=vs.90).aspx

### SQLite:
is "file"-DB and cannot manage too many events. If you have a big amount of data use real DB, like PostgreSQL and co.

### MySQL:
You can install mysql on linux systems:

```
apt-get install mysql-server mysql-client 

mysql -uroot -p

CREATE USER 'iobroker‘@’%’ IDENTIFIED BY 'iobroker';
GRANT ALL PRIVILEGES ON * . * TO 'iobroker'@'%';
FLUSH PRIVILEGES;
```

If required edit */etc/mysql/my.cnf* to set bind to IP-Address for remote connect.

**Warning**: iobroker user is "admin". If required give limited rights to iobroker user.

## Structure of the DBs
Default Database name is "iobroker", but it can be changed in configuration.
### Sources
This table is a list of adapter's instances, that wrote the entries. (state.from)

| DB         | Name in query        |
|------------|----------------------|
| MS-SQL     | iobroker.dbo.sources |
| PostgreSQL | sources              |
| MySQL      | iobroker.sources     |
| SQLite     | sources              |

Structure:

| Field | Type                                       | Description                               |
|-------|--------------------------------------------|-------------------------------------------|
| id    | INTEGER NOT NULL PRIMARY KEY IDENTITY(1,1) | unique ID                                 |
| name  | varchar(255)                               | instance of adapter, that wrote the entry |

### Datapoints
This table is a list of datapoints. (IDs)

| DB         | Name in query           |
|------------|-------------------------|
| MS-SQL     | iobroker.dbo.datapoints |
| PostgreSQL | datapoints              |
| MySQL      | iobroker.datapoints     |
| SQLite     | datapoints              |

Structure:

| Field | Type                                       | Description                                     |
|-------|--------------------------------------------|-------------------------------------------------|
| id    | INTEGER NOT NULL PRIMARY KEY IDENTITY(1,1) | unique ID                                       |
| name  | varchar(255)                               | ID of variable, e.g. hm-rpc.0.JEQ283747.1.STATE |
| type  | integer                                    | 0 - number, 1 - string, 2 - boolean             |

## Custom queries
The user can execute custom queries on tables from javascript adapter:

```
sendTo('sql.0', 'query', 'SELECT * FROM datapoints', function (result) {
    if (result.error) {
        console.error(result.error);
    } else {
        // show result
         console.log('Rows: ' + JSON.stringify(result.result));
    }
});
```

## Changelog
### 0.1.3 (2016-03-08)
* (bluefox) do not print errors twice

### 0.1.2 (2015-12-22)
* (bluefox) fix MS-SQL port settings

### 0.1.1 (2015-12-19)
* (bluefox) fix error with double entries

### 0.1.0 (2015-12-14)
* (bluefox) support of strings

### 0.0.3 (2015-12-06)
* (smiling_Jack) Add demo Data ( todo: faster insert to db )
* (smiling_Jack) change aggregation (now same as history Adapter)
* (bluefox) bug fixing

### 0.0.2 (2015-12-06)
* (bluefox) allow only 1 client for SQLite

### 0.0.1 (2015-11-19)
* (bluefox) initial commit

## License

The MIT License (MIT)

Copyright (c) 2015 bluefox

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
