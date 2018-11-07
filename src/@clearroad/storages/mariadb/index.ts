/* tslint:disable:no-console */
import {
  getQueue, promiseToQueue,
  IJioStorage, IQueue, IClearRoadOptions,
  IJioQueryOptions
} from '@clearroad/api';
import { jIO } from 'jio';

const mariadb = require('mariadb');

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

const idKey = '_id';
const valueKey = 'value';

interface IMariaDBDocument {
  [idKey]: string;
  [valueKey]: string;
}

interface IMariaDBAttachment {
  [idKey]: string;
  name: string;
}

const createDatabase = (databaseName: string) => `CREATE DATABASE IF NOT EXISTS \`${databaseName}\``;

const createDocumentsTable = (tableName: string) => {
  return `CREATE TABLE IF NOT EXISTS ${tableName} (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    ${idKey} VARCHAR(255) NOT NULL,
    ${valueKey} TEXT
  )`;
};

const createAttachmentsTable = (tableName: string) => {
  return `CREATE TABLE IF NOT EXISTS ${tableName} (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    ${idKey} VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    ${valueKey} TEXT
  )`;
};

const indexName = (tableName: string, fields: string[]) => `${tableName}_index_${fields.join('_')}`;

const indexTable = (tableName: string, fields: string[]) => {
  return `ALTER TABLE ${tableName} ADD INDEX ${indexName(tableName, fields)}(${fields.join(', ')})`;
};

/**
 * @internal
 */
export const resultAsJson = (doc: IMariaDBDocument) => JSON.parse(doc[valueKey] || '{}');

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
      .push((connection: IConnection) => {
        return promiseToQueue(safeTransaction(connection, () => {
          return connection.query(createDatabase(database));
        }));
      })
      .push(() => {
        options.database = database;
        return this.initTables(options);
      });
  }

  private initTables(options: IMariaDBStorageOptions) {
    const pool: IPool = mariadb.createPool(options);
    return getQueue()
      .push(() => promiseToQueue(pool.getConnection()))
      .push(connection => {
        return safeTransaction(connection, () => {
          return Promise.all([
            connection.query(createDocumentsTable(this._documentsTable)),
            connection.query(createAttachmentsTable(this._attachmentsTable))
          ]);
        });
      })
      .push(() => promiseToQueue(pool.getConnection()))
      .push(connection => {
        return safeTransaction(connection, () => {
          return Promise.all([
            // create indexes on id keys
            connection.query(indexTable(this._documentsTable, [idKey])).catch(() => {}),
            connection.query(indexTable(this._attachmentsTable, [idKey])).catch(() => {}),
            // create index on id key + name for attachments
            connection.query(indexTable(this._attachmentsTable, [idKey, 'name'])).catch(() => {})
          ]);
        });
      })
      .push(() => this._pool = pool);
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
   * Execute a query
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
    return this.executeQuery<IMariaDBDocument[]>({
      namedPlaceholders: true,
      sql: `SELECT * FROM ${this._documentsTable} WHERE ${idKey}=:id`
    }, {id}).push(results => {
      return results && results.length ? resultAsJson(results[0]) : null;
    });
  }

  put(id: string, data: any) {
    return this.get(id)
      .push(document => {
        if (!document) {
          return this.executeQuery(
            `INSERT INTO ${this._documentsTable} VALUES (NULL, ?, ?)`,
            [id, JSON.stringify(data)]
          );
        }

        return this.executeQuery({
          namedPlaceholders: true,
          sql: `UPDATE ${this._documentsTable} SET ${valueKey}=:data WHERE ${idKey}=:id`
        }, {id, data: JSON.stringify(data)});
      })
      .push(() => {
        return id;
      });
  }

  remove(id: string) {
    return this.executeQuery({
      namedPlaceholders: true,
      sql: `DELETE FROM ${this._documentsTable} WHERE ${idKey}=:id`
    }, {id}).push(() => {
      return id;
    });
  }

  getAttachment(id: string, name: string) {
    return this.executeQuery<IMariaDBDocument[]>({
      namedPlaceholders: true,
      sql: `SELECT * FROM ${this._attachmentsTable} WHERE ${idKey}=:id AND name=:name`
    }, {id, name}).push(results => {
      if (results && results.length) {
        return jIO.util.dataURItoBlob(results[0][valueKey]);
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
        return this.executeQuery(
          `INSERT INTO ${this._attachmentsTable} VALUES (NULL, ?, ?, ?)`,
          [id, name, data.target.result]
        );
      });
  }

  removeAttachment(id: string, name: string) {
    return this.executeQuery({
      namedPlaceholders: true,
      sql: `DELETE FROM ${this._attachmentsTable} WHERE ${idKey}=:id AND name=:name`
    }, {id, name}).push(() => {
      return id;
    });
  }

  allAttachments(id: string) {
    return this.executeQuery<IMariaDBAttachment[]>({
      namedPlaceholders: true,
      sql: `SELECT * FROM ${this._attachmentsTable} WHERE ${idKey}=:id`
    }, {id}).push(documents => {
      const attachments = {};
      if (documents && documents.length) {
        documents.forEach(document => {
          attachments[document.name] = {};
        });
      }
      return attachments;
    });
  }

  /**
   * MariaDB can not search on json objects, therefore we can only list the documents
   * @param name The name of the capacity
   */
  hasCapacity(name: string) {
    return name === 'list' || name === 'include' ||
      name === 'select' || name === 'limit';
  }

  buildQuery(options: IJioQueryOptions = {query: ''}) {
    // LIMIT / OFFSET
    let limit = '';
    if (options.limit) {
      limit = `LIMIT ${options.limit[1] || 100} OFFSET ${options.limit[0] || 0}`;
    }
    const selectList = (options.select_list || []).slice();

    return this.executeQuery<IMariaDBDocument[]>({
      sql: `SELECT * FROM ${this._documentsTable} ${limit}`
    }).push(documents => {
      return documents ? documents.map(document => {
        const value: any = {
          id: document[idKey]
        };
        const doc = resultAsJson(document);
        if (options.include_docs) {
          value.doc = doc;
        }
        else if (options.select_list) {
          value.value = {};
          selectList.forEach(key => value.value[key] = doc[key]);
        }
        return value;
      }) : [];
    });
  }
}

export interface IMariaDBOptions extends IClearRoadOptions {
  localStorage: IMariaDBStorageOptions;
  /**
   * MariaDB does NOT support JSON queries
   */
  useQueryStorage: true;
}

const storageName = 'mariadb';
jIO.addStorage(storageName, MariaDBStorage);
export default storageName;
