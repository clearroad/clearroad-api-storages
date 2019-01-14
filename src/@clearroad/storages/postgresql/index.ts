/* tslint:disable:no-console */
import {
  getQueue, promiseToQueue,
  IJioStorage, IQueue, IClearRoadOptions,
  IJioQueryOptions, IJioSimpleQuery, IJioComplexQuery,
  queryPortalType,
  queryGroupingReference
} from '@clearroad/api';
import { jIO } from 'jio';

import { Client, Pool, ClientConfig, PoolClient, QueryResult } from 'pg';

/**
 * @internal
 */
export const idKey = '_id';
/**
 * @internal
 */
export const valueKey = 'value';
/**
 * @internal
 */
export const createdAtKey = 'createdAt';
/**
 * @internal
 */
export const updatedAtKey = 'updatedAt';
/**
 * @internal
 */
export const defaultDocumentsCollection = 'documents';
/**
 * @internal
 */
export const defaultAttachmentsCollection = 'attachments';

export interface IPostgreSQLStorageOptions extends ClientConfig {
  type: 'postgresql';
  /**
   * Database name.
   */
  database: string;
  /**
   * Table name for all documents.
   */
  documentsTableName?: string;
  /**
   * Table name for attachments.
   */
  attachmentsTableName?: string;
  /**
   * Add created/updatedAt timestamps for every document.
   * Enabled by default for both
   */
  timestamps?: boolean;
}

interface IPostgreSQLDocument {
  [idKey]: string;
  [valueKey]: {};
  [createdAtKey]?: Date;
  [updatedAtKey]?: Date;
}

interface IPostgreSQLAttachment {
  [idKey]: string;
  name: string;
  [valueKey]: string;
  [createdAtKey]?: Date;
  [updatedAtKey]?: Date;
}

const keyToDBField = (key: string) => key === 'modification_date' ? createdAtKey : `${valueKey} ->> '${key}'`;

const parseSimpleQuery = (query: IJioSimpleQuery, key = '') => {
  return `${keyToDBField(key)} ${query.operator || '='} '${query.value}'`;
};

const parseComplexQuery = (query: IJioComplexQuery) => {
  return `(${query.query_list.map(subquery => parseQuery(subquery, query.key)).join(` ${query.operator} `)})`;
};

/**
 * @internal
 * @param parsed
 * @param key
 */
export const parseQuery = (parsed: IJioSimpleQuery|IJioComplexQuery, key?: string) => {
  if (parsed.type === 'complex') {
    return parseComplexQuery(parsed as IJioComplexQuery);
  }
  else {
    return parseSimpleQuery(parsed as IJioSimpleQuery, parsed.key || key);
  }
};

const createDatabase = (databaseName: string) => `CREATE DATABASE "${databaseName}"`;

const createDocumentsTable = (tableName: string, timestamps: boolean) => {
  return `CREATE TABLE IF NOT EXISTS ${tableName} (
    id SERIAL PRIMARY KEY,
    ${idKey} VARCHAR(255) NOT NULL,
    ${valueKey} jsonb${timestamps ? `, ${createdAtKey} TIMESTAMPTZ DEFAULT Now() , ${updatedAtKey} TIMESTAMPTZ` : ''}
  )`;
};

const createAttachmentsTable = (tableName: string, timestamps: boolean) => {
  return `CREATE TABLE IF NOT EXISTS ${tableName} (
    id SERIAL PRIMARY KEY,
    ${idKey} VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    ${valueKey} TEXT${timestamps ? `, ${createdAtKey} TIMESTAMPTZ DEFAULT Now() , ${updatedAtKey} TIMESTAMPTZ` : ''}
  )`;
};

const indexName = (tableName: string, fields: string[]) => `${tableName}_index_${fields.join('_')}`;

const indexTable = (tableName: string, fields: string[], name?: string) => {
  return `CREATE INDEX ${name || indexName(tableName, fields)} ON ${tableName} (${fields.join(', ')})`;
};

/**
 * @internal
 */
export const resultAsJson = (doc: IPostgreSQLDocument) => doc[valueKey];

/**
 * Execute queries within a transaction
 * @internal
 * @param client
 * @param transactions
 */
export const safeTransaction = async (client: PoolClient, transactions: () => Promise<any>) => {
  try {
    await client.query('BEGIN');
    await transactions();
    await client.query('COMMIT');
  }
  catch (error) {
    console.error(error);
    await client.query('ROLLBACK');
  }
  finally {
    client.release();
  }
};

/**
 * @internal
 */
export const safeQuery = async <T>(client: PoolClient, query: () => Promise<QueryResult>) => {
  let result = {
    rows: [] as T[],
    rowCount: 0
  };
  try {
    result = await query();
  }
  catch (error) {
    console.error(error);
  }
  finally {
    client.release();
  }
  return result;
};

const requireOptionHost = (options: IPostgreSQLStorageOptions) => {
  if (typeof options.host !== 'string' || !options.host) {
    throw new Error('"host" must be a non-empty string');
  }
};

const requireOptionDatabase = (options: IPostgreSQLStorageOptions) => {
  if (typeof options.database !== 'string' || !options.database) {
    throw new Error('"database" must be a non-empty string');
  }
};

const requireOptionTableNames = (options: IPostgreSQLStorageOptions) => {
  if (!options.documentsTableName) {
    options.documentsTableName = defaultDocumentsCollection;
  }
  if (!options.attachmentsTableName) {
    options.attachmentsTableName = defaultAttachmentsCollection;
  }
};

const queryWhere = (options: IJioQueryOptions) => {
  if (options.query) {
    const parsed = jIO.QueryFactory.create(options.query);
    return ` WHERE ${parseQuery(parsed)}`;
  }
  return '';
};

const querySort = (options: IJioQueryOptions) => {
  if (options.sort_on) {
    return ` ORDER BY ${(options.sort_on || []).map(values => {
      return `${keyToDBField(values[0])} ${values[1] === 'ascending' ? 'ASC' : 'DESC'}`;
    }).join(', ')}`;
  }
  return '';
};

const queryLimit = (options: IJioQueryOptions) => {
  return options.limit ? ` LIMIT ${options.limit[1] || 100} OFFSET ${options.limit[0] || 0}` : '';
};

const queryParseDocument = (document: IPostgreSQLDocument, includeDoc: boolean, selectList: string[]) => {
  const value: any = {
    id: document[idKey]
  };
  const doc = resultAsJson(document);
  if (includeDoc) {
    value.doc = doc;
  }
  else if (selectList.length) {
    value.value = {};
    selectList.forEach(key => value.value[key] = doc[key]);
  }
  return value;
};

/**
 * @internal
 */
export class PostgreSQLStorage implements IJioStorage {
  private _dbPromise: IQueue<Pool>;
  private _pool: Pool;
  private _documentsTable: string;
  private _attachmentsTable: string;
  private _timestamps = true;

  /**
   * Initiate a PostgreSQL Storage.
   * @param options Storage options
   */
  constructor(options: IPostgreSQLStorageOptions) {
    requireOptionHost(options);
    requireOptionDatabase(options);
    requireOptionTableNames(options);
    this._documentsTable = options.documentsTableName!;
    this._attachmentsTable = options.attachmentsTableName!;
    if (options.timestamps === false) {
      this._timestamps = false;
    }
    this._dbPromise = this.initDb(options);
  }

  /**
   * @internal
   */
  private initDb(options: IPostgreSQLStorageOptions) {
    const database = options.database;
    // connect to default postgres database to create our database
    options.database = 'postgres';

    return getQueue()
      .push(() => new Client(options))
      .push(async client => {
        await client.connect();
        try {
          await client.query(createDatabase(database));
        }
        catch (err) {} // ignore errors if database already exists
        await client.end();
      })
      .push(() => {
        options.database = database;
        return this.initTables(options);
      });
  }

  private initTables(options: IPostgreSQLStorageOptions) {
    const pool = new Pool(options);
    return getQueue()
      .push(() => promiseToQueue(pool.connect()))
      .push(client => {
        return safeTransaction(client, () => {
          return Promise.all([
            client.query(createDocumentsTable(this._documentsTable, this._timestamps)),
            client.query(createAttachmentsTable(this._attachmentsTable, this._timestamps))
          ]);
        });
      })
      .push(() => promiseToQueue(pool.connect()))
      .push(client => {
        return safeTransaction(client, () => {
          return Promise.all([
            // create indexes on id keys
            client.query(indexTable(this._documentsTable, [idKey])).catch(() => {}),
            client.query(indexTable(this._attachmentsTable, [idKey])).catch(() => {}),
            // indexes the most common fields when doing a query
            client.query(indexTable(this._documentsTable, [
              `${valueKey} ->> ${queryPortalType}`,
              `${valueKey} ->> ${queryGroupingReference}`
            ], 'documents_index_queries')).catch(() => {}),
            // create index on id key + name for attachments
            client.query(indexTable(this._attachmentsTable, [idKey, 'name'])).catch(() => {})
          ]);
        });
      })
      .push(() => this._pool = pool);
  }

  /**
   * Get an active client. Call `client.end()` to release when done querying
   * @internal
   */
  private client() {
    return getQueue()
      .push(() => this._pool ? this._pool : this._dbPromise)
      .push(pool => promiseToQueue(pool.connect()));
  }

  /**
   * Execute a transaction to modify data.
   * @internal
   * @param sql
   * @param values
   */
  private executeTransaction(sql: string, values?: string[]) {
    return this.client().push(client => {
      return promiseToQueue(safeTransaction(client, () => {
        return client.query(sql, values);
      }));
    });
  }

  /**
   * Execute a query.
   * @internal
   * @param sql
   * @param values
   */
  private executeQuery<T>(sql: string, values?: string[]) {
    return this.client().push(client => {
      return promiseToQueue(safeQuery<T>(client, () => {
        return client.query(sql, values);
      }));
    });
  }

  get(id: string) {
    return this.executeQuery<IPostgreSQLDocument>(
      `SELECT * FROM ${this._documentsTable} WHERE ${idKey}=$1`,
      [id]
    ).push(result => {
      return result.rows.length ? resultAsJson(result.rows[0]) : null;
    });
  }

  put(id: string, data: any) {
    return this.get(id)
      .push(document => {
        if (!document) {
          return this.executeTransaction(
            `INSERT INTO ${this._documentsTable} (${idKey}, ${valueKey}) VALUES ($1, $2)`,
            [id, JSON.stringify(data)]
          );
        }

        let update = `UPDATE ${this._documentsTable} SET ${valueKey}=$2 WHERE ${idKey}=$1`;
        if (this._timestamps) {
          update = `UPDATE ${this._documentsTable} SET ${valueKey}=$2, ${updatedAtKey}=Now() WHERE ${idKey}=$1`;
        }
        return this.executeTransaction(update, [id, JSON.stringify(data)]);
      })
      .push(() => {
        return id;
      });
  }

  remove(id: string) {
    return this.executeTransaction(
      `DELETE FROM ${this._documentsTable} WHERE ${idKey}=$1`,
      [id]
    ).push(() => {
      return id;
    });
  }

  getAttachment(id: string, name: string) {
    return this.executeQuery<IPostgreSQLAttachment>(
      `SELECT * FROM ${this._attachmentsTable} WHERE ${idKey}=$1 AND name=$2`,
      [id, name]
    ).push(result => {
      if (result.rows.length) {
        return jIO.util.dataURItoBlob(result.rows[0][valueKey]);
      }
      throw new jIO.util.jIOError(
        `Cannot find attachment: ${id}`,
        404
      );
    });
  }

  putAttachment(id: string, name: string, blob: Blob) {
    return getQueue()
      .push(() => {
        return jIO.util.readBlobAsDataURL(blob);
      })
      .push(data => {
        return this.executeTransaction(
          `INSERT INTO ${this._attachmentsTable} (${idKey}, name, ${valueKey}) VALUES ($1, $2, $3)`,
          [id, name, data.target.result]
        );
      });
  }

  removeAttachment(id: string, name: string) {
    return this.executeTransaction(
      `DELETE FROM ${this._attachmentsTable} WHERE ${idKey}=$1 AND name=$2`,
      [id, name]
    ).push(() => {
      return id;
    });
  }

  allAttachments(id: string) {
    return this.executeQuery<IPostgreSQLAttachment>(
      `SELECT * FROM ${this._attachmentsTable} WHERE ${idKey}=$1`,
      [id]
    ).push(result => {
      const attachments = {};
      if (result.rows.length) {
        result.rows.forEach(document => {
          attachments[document.name] = {};
        });
      }
      return attachments;
    });
  }

  hasCapacity() {
    return true;
  }

  buildQuery(options: IJioQueryOptions = {query: ''}) {
    const where = queryWhere(options);
    const sort = querySort(options);
    const limit = queryLimit(options);
    const selectList = (options.select_list || []).slice();
    const sql = `SELECT * FROM ${this._documentsTable}${where}${sort}${limit}`;

    return this.executeQuery<IPostgreSQLDocument>(sql).push(result => {
      return result.rows.map(document => queryParseDocument(document, options.include_docs || false, selectList));
    });
  }
}

export interface IPostgreSQLOptions extends IClearRoadOptions {
  localStorage: IPostgreSQLStorageOptions;
  /**
   * PostgreSQL supports JSON queries
   */
  useQueryStorage?: false;
}

const storageName = 'postgresql';
jIO.addStorage(storageName, PostgreSQLStorage);
export default storageName;
