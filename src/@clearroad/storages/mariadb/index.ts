/* tslint:disable:no-console */
const mariadb = require('mariadb');
import {
  jIO, getQueue, promiseToQueue,
  IJioStorage, IQueue,
  IJioQueryOptions
} from '@clearroad/api';

/**
 * @internal
 */
export interface IConnection {
  beginTransaction: () => Promise<void>;
  commit: () => Promise<void>;
  rollback: () => Promise<void>;
  end: () => Promise<void>;

  query: <T>(sql: any, values?: any) => Promise<T>;
}

/**
 * @internal
 */
export interface IPool {
  getConnection: () => Promise<IConnection>;
}

/**
 * @internal
 */
export const defaultDocumentsCollection = 'documents';
/**
 * @internal
 */
export const defaultAttachmentsCollection = 'attachments';

export interface IMariaDBStorageOptions {
  type: 'mariadb';
  /**
   * IP address or DNS of the database server.
   */
  host: string;
  /**
   * Database server port number
   */
  port?: number;
  /**
   * User to access database
   */
  user?: string;
  /**
   * User password
   */
  password?: string;
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
}

const primaryKey = '_id';
const valueKey = 'value';

interface IMariaDBDocument {
  [primaryKey]: string;
  [valueKey]: string;
}

const createDatabase = (databaseName: string) => `CREATE DATABASE IF NOT EXISTS \`${databaseName}\``;

// const useDatabase = (databaseName: string) => `USE \`${databaseName}\``;

const createDocumentsTable = (tableName: string) => {
  return `CREATE TABLE IF NOT EXISTS ${tableName} (
    ${primaryKey} VARCHAR(255) NOT NULL,
    ${valueKey} TEXT,
    PRIMARY KEY (${primaryKey})
  )`;
};

const createAttachmentsTable = (tableName: string) => {
  return `CREATE TABLE IF NOT EXISTS ${tableName} (
    ${primaryKey} VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    ${valueKey} TEXT,
    PRIMARY KEY (${primaryKey})
  )`;
};

const getIndexes = (tableName: string, indexName: string) => {
  return `SELECT * FROM information_schema.statistics
    WHERE TABLE_SCHEMA=database()
      AND TABLE_NAME="${tableName}"
      AND INDEX_NAME="${indexName}"`;
};

const attachmentsTableIndexName = 'compound';

const indexAttachmentsTable = (tableName: string) => {
  return `ALTER TABLE ${tableName} ADD INDEX ${attachmentsTableIndexName}(${primaryKey}, name)`;
};

/**
 * @internal
 */
export const resultAsJson = (doc: IMariaDBDocument) => {
  const data = JSON.parse(doc[valueKey] || '{}');
  data.id = doc[primaryKey];
  return data;
};

/**
 * Execute queries within a transaction
 * @internal
 * @param connection
 * @param transactions
 */
export const safeTransaction = async (connection: IConnection, transactions: () => Promise<any>) => {
  try {
    await connection.beginTransaction();
    await transactions();
    await connection.commit();
  }
  catch (error) {
    console.error(error);
    await connection.rollback();
  }
  return connection.end();
};

/**
 * @internal
 */
export const safeQuery = async <T>(connection: IConnection, query: () => Promise<T>) => {
  try {
    const results = await query();
    await connection.end();
    return results;
  }
  catch (error) {
    console.error(error);
  }
  return null;
};

/**
 * @internal
 */
export class MariaDBStorage implements IJioStorage {
  private _dbPromise: IQueue<IPool>;
  private _pool: IPool;
  private _documentsTable: string;
  private _attachmentsTable: string;

  /**
   * Initiate a MariaDB Storage.
   * @param options Storage options
   */
  constructor(options: IMariaDBStorageOptions) {
    if (typeof options.host !== 'string' || !options.host) {
      throw new Error('"host" must be a non-empty string');
    }
    if (typeof options.database !== 'string' || !options.database) {
      throw new Error('"database" must be a non-empty string');
    }
    if (!options.documentsTableName) {
      options.documentsTableName = defaultDocumentsCollection;
    }
    this._documentsTable = options.documentsTableName;
    if (!options.attachmentsTableName) {
      options.attachmentsTableName = defaultAttachmentsCollection;
    }
    this._attachmentsTable = options.attachmentsTableName;
    this._dbPromise = this.initDb(options);
  }

  /**
   * @internal
   */
  private initDb(options: IMariaDBStorageOptions) {
    const database = options.database;
    delete options.database;

    return getQueue()
      .push(() => mariadb.createConnection(options))
      .push((connection: IConnection) => safeTransaction(connection, () => {
        return connection.query(createDatabase(database));
      }))
      .push(() => {
        options.database = database;
        return this.initTables(options);
      });
  }

  private initTables(options: IMariaDBStorageOptions) {
    const pool: IPool = mariadb.createPool(options);
    return getQueue()
      .push(() => promiseToQueue(pool.getConnection()))
      .push(async connection => {
        await safeTransaction(connection, () => {
          return Promise.all([
            connection.query(createDocumentsTable(this._documentsTable)),
            connection.query(createAttachmentsTable(this._attachmentsTable))
          ]);
        });

        const results = await safeQuery<any[]>(connection, () => {
          return connection.query(getIndexes(this._attachmentsTable, attachmentsTableIndexName));
        });
        if (results && !results.length) {
          await safeTransaction(connection, () => {
            return connection.query(indexAttachmentsTable(this._attachmentsTable));
          });
        }
      })
      .push(() => {
        console.log('pool ready');
        return this._pool = pool;
      });
  }

  /**
   * Get an active connection. Call `connection.end()` to release when done querying
   * @internal
   */
  private connection() {
    return getQueue()
      .push(() => this._pool ? this._pool : this._dbPromise)
      .push(pool => promiseToQueue(pool.getConnection()));
  }

  /**
   * Execute a query within a transaction
   * @internal
   * @param sql
   * @param values
   */
  private executeTransaction(sql: any, values?: any) {
    return this.connection().push(connection => {
      return promiseToQueue(safeTransaction(connection, () => {
        return connection.query(sql, values);
      }));
    });
  }

  /**
   * Execute a query of type "SELECT". Use `executeTransaction` if updating the data
   * @internal
   * @param sql
   * @param values
   */
  private executeQuery<T>(sql: any, values?: any) {
    return this.connection().push(connection => {
      return promiseToQueue(safeQuery<T>(connection, () => {
        return connection.query(sql, values);
      }));
    });
  }

  get(id: string) {
    return promiseToQueue(this.executeQuery<IMariaDBDocument[]>({
      namedPlaceholders: true,
      sql: `SELECT * FROM ${this._documentsTable} WHERE ${primaryKey}=:id`
    }, {id})).push(results => {
      return results ? resultAsJson(results[0]) : {};
    });
  }

  put(id: string, data: any) {
    return this.get(id)
      .push(document => {
        if (!document) {
          data.id = id;
          return promiseToQueue(this.executeTransaction(
            `INSERT INTO ${this._documentsTable} VALUE(?, ?)`,
            [id, JSON.stringify(data)]
          ));
        }

        return promiseToQueue(this.executeTransaction({
          namedPlaceholders: true,
          sql: `UPDATE ${this._documentsTable} SET ${valueKey}=:data WHERE ${primaryKey}=:id`
        }, {id, data}));
      })
      .push(() => {
        return id;
      });
  }

  remove(id: string) {
    return promiseToQueue(this.executeTransaction({
      namedPlaceholders: true,
      sql: `DELETE FROM ${this._documentsTable} WHERE ${primaryKey}=:id`
    }, {id})).push(() => {
      return id;
    });
  }

  getAttachment: (id: string, name: string, options?: any) => IQueue<any>;
  putAttachment: (id: string, name: string, blob: Blob) => IQueue<any>;
  removeAttachment: (id: string, name: string) => IQueue<string>;
  allAttachments: (id: string) => IQueue<any>;

  /**
   * MariaDB can not search on json objects, therefore we can only list the documents
   * @param name The name of the capacity
   */
  hasCapacity(name: string) {
    return name === 'list' || name === 'include' ||
      name === 'select' || name === 'limit';
  }

  buildQuery(options: IJioQueryOptions = {query: ''}) {
    const defaultIdKey = 'id';
    let selectList: string[] = [];
    if (options.select_list) {
      selectList = options.select_list;
      // make sure 'id' is returned
      selectList.push(defaultIdKey);
    }
    let limit = '';
    if (options.limit) {
      limit = `LIMIT ${options.limit[1] || 100} OFFSET ${options.limit[0] || 0}`;
    }

    return promiseToQueue(this.executeQuery<IMariaDBDocument[]>({
      sql: `SELECT * FROM ${this._documentsTable} ${limit}`
    })).push(documents => {
      return documents ? documents.map(document => {
        const value: any = {
          id: document[primaryKey]
        };
        const doc = resultAsJson(document);
        if (options.include_docs) {
          value.doc = doc;
        }
        else if (options.select_list) {
          value.value = {};
          selectList.filter(key => key !== defaultIdKey).forEach(key => value.value[key] = doc[key]);
        }
        return value;
      }) : [];
    });
  }
}

const storageName = 'mariadb';
jIO.addStorage(storageName, MariaDBStorage);
export default storageName;
