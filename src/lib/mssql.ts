import type { TableName } from '../types';

export function init(dbName: string, doNotCreateDatabase?: boolean): string[] {
    const commands = [
        `CREATE TABLE ${dbName}.dbo.sources     (id INTEGER NOT NULL PRIMARY KEY IDENTITY(1,1), name varchar(255));`,
        `CREATE TABLE ${dbName}.dbo.datapoints  (id INTEGER NOT NULL PRIMARY KEY IDENTITY(1,1), name varchar(255), type INTEGER);`,
        `CREATE TABLE ${dbName}.dbo.ts_number   (id INTEGER, ts BIGINT, val REAL, ack BIT, _from INTEGER, q INTEGER);`,
        `CREATE INDEX i_id on ${dbName}.dbo.ts_number (id, ts);`,
        `CREATE TABLE ${dbName}.dbo.ts_string   (id INTEGER, ts BIGINT, val TEXT, ack BIT, _from INTEGER, q INTEGER);`,
        `CREATE INDEX i_id on ${dbName}.dbo.ts_string (id, ts);`,
        `CREATE TABLE ${dbName}.dbo.ts_bool     (id INTEGER, ts BIGINT, val BIT,  ack BIT, _from INTEGER, q INTEGER);`,
        `CREATE INDEX i_id on ${dbName}.dbo.ts_bool (id, ts);`,
        `CREATE TABLE ${dbName}.dbo.ts_counter  (id INTEGER, ts BIGINT, val REAL);`,
        `CREATE INDEX i_id on ${dbName}.dbo.ts_counter (id, ts);`,
    ];
    if (!doNotCreateDatabase) {
        commands.unshift(`CREATE DATABASE ${dbName};`);
    }

    return commands;
}

export function destroy(dbName: string): string[] {
    return [
        `DROP TABLE ${dbName}.dbo.ts_counter;`,
        `DROP TABLE ${dbName}.dbo.ts_number;`,
        `DROP TABLE ${dbName}.dbo.ts_string;`,
        `DROP TABLE ${dbName}.dbo.ts_bool;`,
        `DROP TABLE ${dbName}.dbo.sources;`,
        `DROP TABLE ${dbName}.dbo.datapoints;`,
        `DROP DATABASE ${dbName};`,
        `DBCC FREEPROCCACHE;`,
    ];
}

export function getFirstTs(dbName: string, table: TableName): string {
    return `SELECT id, MIN(ts) AS ts FROM ${dbName}.dbo.${table} GROUP BY id;`;
}

export function insert(
    dbName: string,
    index: number,
    values: {
        table: TableName;
        state: { val: any; ts: number; ack?: boolean; q?: number };
        from?: number;
    }[],
): string {
    const insertValues: { [table: string]: string[] } = {};
    values.forEach(value => {
        // state, from, db
        insertValues[value.table] ||= [];

        if (!value.state || value.state.val === null || value.state.val === undefined) {
            value.state.val = 'NULL';
        } else if (value.table === 'ts_string') {
            value.state.val = `'${value.state.val.toString().replace(/'/g, '')}'`;
        } else if (value.table === 'ts_bool') {
            value.state.val = value.state.val ? 1 : 0;
        } else if (value.table === 'ts_number') {
            if (isNaN(value.state.val)) {
                value.state.val = 'NULL';
            }
        }

        if (value.table === 'ts_counter') {
            insertValues[value.table].push(`(${index}, ${value.state.ts}, ${value.state.val})`);
        } else {
            insertValues[value.table].push(
                `(${index}, ${value.state.ts}, ${value.state.val}, ${value.state.ack ? 1 : 0}, ${value.from || 0}, ${value.state.q || 0})`,
            );
        }
    });

    const query: string[] = [];
    for (const table in insertValues) {
        if (table === 'ts_counter') {
            while (insertValues[table].length) {
                query.push(
                    `INSERT INTO ${dbName}.dbo.ts_counter (id, ts, val) VALUES ${insertValues[table].splice(0, 500).join(',')};`,
                );
            }
        } else {
            while (insertValues[table].length) {
                query.push(
                    `INSERT INTO ${dbName}.dbo.${table} (id, ts, val, ack, _from, q) VALUES ${insertValues[table].splice(0, 500).join(',')};`,
                );
            }
        }
    }

    return query.join(' ');
}

export function retention(dbName: string, index: number, table: TableName, retention: number): string {
    const d = new Date();
    d.setSeconds(-retention);
    let query = `DELETE FROM ${dbName}.dbo.${table} WHERE`;
    query += ` id=${index}`;
    query += ` AND ts < ${d.getTime()}`;
    query += ';';
    return query;
}

export function getIdSelect(dbName: string, name?: string): string {
    if (!name) {
        return `SELECT id, type, name FROM ${dbName}.dbo.datapoints;`;
    }
    return `SELECT id, type, name FROM ${dbName}.dbo.datapoints WHERE name='${name}';`;
}

export function getIdInsert(dbName: string, name: string, type: 0 | 1 | 2): string {
    return `INSERT INTO ${dbName}.dbo.datapoints (name, type) VALUES('${name}', ${type});`;
}

export function getIdUpdate(dbName: string, id: number, type: 0 | 1 | 2): string {
    return `UPDATE ${dbName}.dbo.datapoints SET type=${type} WHERE id=${id};`;
}

export function getFromSelect(dbName: string, name?: string): string {
    if (name) {
        return `SELECT id FROM ${dbName}.dbo.sources WHERE name='${name}';`;
    }
    return `SELECT id, name FROM ${dbName}.dbo.sources;`;
}

export function getFromInsert(dbName: string, values: string): string {
    return `INSERT INTO ${dbName}.dbo.sources (name) VALUES('${values}');`;
}

export function getCounterDiff(
    dbName: string,
    options: {
        index: number;
        start: number;
        end: number;
    },
): string {
    // Take first real value after start
    const subQueryStart = `SELECT TOP 1 val, ts FROM ${dbName}.dbo.ts_number WHERE ${dbName}.dbo.ts_number.id=${options.index} AND ${dbName}.dbo.ts_number.ts>=${options.start} AND ${dbName}.dbo.ts_number.ts<${options.end} AND ${dbName}.dbo.ts_number.val IS NOT NULL ORDER BY ${dbName}.dbo.ts_number.ts ASC`;
    // Take last real value before the end
    const subQueryEnd = `SELECT TOP 1 val, ts FROM ${dbName}.dbo.ts_number WHERE ${dbName}.dbo.ts_number.id=${options.index} AND ${dbName}.dbo.ts_number.ts>=${options.start} AND ${dbName}.dbo.ts_number.ts<${options.end} AND ${dbName}.dbo.ts_number.val IS NOT NULL ORDER BY ${dbName}.dbo.ts_number.ts DESC`;
    // Take last value before start
    const subQueryFirst = `SELECT TOP 1 val, ts FROM ${dbName}.dbo.ts_number WHERE ${dbName}.dbo.ts_number.id=${options.index} AND ${dbName}.dbo.ts_number.ts< ${options.start} ORDER BY ${dbName}.dbo.ts_number.ts DESC`;
    // Take next value after end
    const subQueryLast = `SELECT TOP 1 val, ts FROM ${dbName}.dbo.ts_number WHERE ${dbName}.dbo.ts_number.id=${options.index} AND ${dbName}.dbo.ts_number.ts>=${options.end} ORDER BY ${dbName}.dbo.ts_number.ts ASC`;
    // get values from counters where counter values changed
    const subQueryCounterChanges = `SELECT val, ts FROM ${dbName}.dbo.ts_counter WHERE ${dbName}.dbo.ts_number.id=${options.index} AND ${dbName}.dbo.ts_number.ts>${options.start} AND ${dbName}.dbo.ts_number.ts<${options.end} AND ${dbName}.dbo.ts_number.val IS NOT NULL ORDER BY ${dbName}.dbo.ts_number.ts ASC`;

    return (
        `${subQueryFirst} ` +
        `UNION ALL (${subQueryStart}) a ` +
        `UNION ALL (${subQueryEnd}) b ` +
        `UNION ALL (${subQueryLast}) c` +
        `UNION ALL (${subQueryCounterChanges}) d`
    );
}

export function getHistory(
    dbName: string,
    table: string,
    options: ioBroker.GetHistoryOptions & { index: number | null },
): string {
    let query = 'SELECT * FROM (SELECT ';
    if ((!options.start && options.count) || (options.aggregate === 'none' && options.count)) {
        query += ` TOP ${options.count}`;
    }
    query += ` ts, val${options.index !== null ? `, ${table}.id as id` : ''}${options.ack ? `, ack` : ''}${
        options.from ? `, ${dbName}.dbo.sources.name as 'from'` : ''
    }${options.q ? `, q` : ''} FROM ${dbName}.dbo.${table}`;

    if (options.from) {
        query += ` INNER JOIN ${dbName}.dbo.sources ON ${dbName}.dbo.sources.id=${dbName}.dbo.${table}._from`;
    }

    let where = '';

    if (options.index !== null) {
        where += ` ${dbName}.dbo.${table}.id=${options.index}`;
    }
    if (options.end) {
        where += `${where ? ` AND` : ''} ${dbName}.dbo.${table}.ts < ${options.end}`;
    }
    if (options.start) {
        where += `${where ? ` AND` : ''} ${dbName}.dbo.${table}.ts >= ${options.start}`;
    }
    if ((!options.start && options.count) || (options.aggregate === 'none' && options.count)) {
        where += ` ORDER BY ts`;

        if (
            (!options.start && options.count) ||
            (options.aggregate === 'none' && options.count && options.returnNewestEntries)
        ) {
            where += ` DESC`;
        } else {
            where += ` ASC`;
        }
    }
    where += `) AS t`;
    if (options.start) {
        // add last value before start
        let subQuery;
        let subWhere;
        subQuery = ` SELECT TOP 1 ts, val${options.index !== null ? `, ${table}.id as id` : ''}${options.ack ? `, ack` : ''}${
            options.from ? `, ${dbName}.dbo.sources.name as 'from'` : ''
        }${options.q ? `, q` : ''} FROM ${dbName}.dbo.${table}`;
        if (options.from) {
            subQuery += ` INNER JOIN ${dbName}.dbo.sources ON ${dbName}.dbo.sources.id=${dbName}.dbo.${table}._from`;
        }
        subWhere = '';
        if (options.index !== null) {
            subWhere += ` ${dbName}.dbo.${table}.id=${options.index}`;
        }
        if (options.ignoreNull) {
            //subWhere += (subWhere ? ` AND` : '') + ` val <> NULL`;
        }
        subWhere += `${subWhere ? ` AND` : ''} ${dbName}.dbo.${table}.ts < ${options.start}`;
        if (subWhere) {
            subQuery += ` WHERE ${subWhere}`;
        }
        subQuery += ` ORDER BY ${dbName}.dbo.${table}.ts DESC`;
        where += ` UNION ALL SELECT * FROM (${subQuery}) a`;

        // add next value after end
        subQuery = ` SELECT TOP 1 ts, val${options.index !== null ? `, ${table}.id as id` : ''}${options.ack ? `, ack` : ''}${
            options.from ? `, ${dbName}.dbo.sources.name as 'from'` : ''
        }${options.q ? `, q` : ''} FROM ${dbName}.dbo.${table}`;
        if (options.from) {
            subQuery += ` INNER JOIN ${dbName}.dbo.sources ON ${dbName}.dbo.sources.id=${dbName}.dbo.${table}._from`;
        }
        subWhere = '';
        if (options.index !== null) {
            subWhere += ` ${dbName}.dbo.${table}.id=${options.index}`;
        }
        if (options.ignoreNull) {
            //subWhere += (subWhere ? ` AND` : '') + ` val <> NULL`;
        }
        subWhere += `${subWhere ? ` AND` : ''} ${dbName}.dbo.${table}.ts >= ${options.end}`;
        if (subWhere) {
            subQuery += ` WHERE ${subWhere}`;
        }
        subQuery += ` ORDER BY ${dbName}.dbo.${table}.ts ASC`;
        where += ` UNION ALL SELECT * FROM (${subQuery}) b`;
    }

    if (where) {
        query += ` WHERE ${where}`;
    }

    query += ` ORDER BY ts`;
    if (
        (!options.start && options.count) ||
        (options.aggregate === 'none' && options.count && options.returnNewestEntries)
    ) {
        query += ` DESC`;
    } else {
        query += ` ASC`;
    }
    query += `;`;

    return query;
}

export function deleteFromTable(dbName: string, table: TableName, index: number, start?: number, end?: number): string {
    let query = `DELETE FROM ${dbName}.dbo.${table} WHERE`;
    query += ` id=${index}`;
    if (start && end) {
        query += ` AND ${dbName}.dbo.${table}.ts>=${start} AND ${dbName}.dbo.${table}.ts<=${end}`;
    } else if (start) {
        query += ` AND ${dbName}.dbo.${table}.ts=${start}`;
    }

    query += ';';

    return query;
}

export function update(
    dbName: string,
    index: number,
    state: { val: number | string | boolean | null | undefined; ts: number; q?: number; ack?: boolean },
    from: number,
    table: TableName,
): string {
    if (!state || state.val === null || state.val === undefined) {
        state.val = 'NULL';
    } else if (table === 'ts_bool') {
        state.val = state.val ? 1 : 0;
    } else if (table === 'ts_string') {
        state.val = `'${state.val.toString().replace(/'/g, '')}'`;
    }

    let query = `UPDATE ${dbName}.dbo.${table} SET `;
    const vals: string[] = [];
    if (state.val !== undefined) {
        vals.push(`val=${state.val}`);
    }
    if (state.q !== undefined) {
        vals.push(`q=${state.q}`);
    }
    if (from !== undefined) {
        vals.push(`_from=${from}`);
    }
    if (state.ack !== undefined) {
        vals.push(`ack=${state.ack ? 1 : 0}`);
    }
    query += vals.join(', ');
    query += ` WHERE id=${index} AND ts=${state.ts};`;

    return query;
}
