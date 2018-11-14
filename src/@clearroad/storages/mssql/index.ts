/* tslint:disable:no-console */
import {
  getQueue, promiseToQueue,
  IJioStorage, IQueue, IClearRoadOptions,
  IJioQueryOptions
} from '@clearroad/api';
import { jIO } from 'jio';

import { ConnectionPool, config, Request, Transaction, VarChar } from 'mssql';

/**
 * @internal
 */
export const defaultDocumentsCollection = 'documents';
/**
 * @internal
 */
export const defaultAttachmentsCollection = 'attachments';

export interface IMSSQLStorageOptions extends config {
  type: 'mssql';
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

const createDatabase = (databaseName: string) => `CREATE DATABASE "${databaseName}"`;

const createDocumentsTable = (tableName: string) => {
  return `CREATE TABLE "${tableName}" (
    id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
    ${idKey} VARCHAR(255) NOT NULL,
    ${valueKey} TEXT
  )`;
};

const createAttachmentsTable = (tableName: string) => {
  return `CREATE TABLE "${tableName}" (
    id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
    ${idKey} VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    ${valueKey} TEXT
  )`;
};

const indexName = (tableName: string, fields: string[]) => `${tableName}_index_${fields.join('_')}`;

const indexTable = (tableName: string, fields: string[]) => {
  return `CREATE INDEX ${indexName(tableName, fields)} ON ${tableName} (${fields.join(', ')})`;
};

/**
 * @internal
 */
export const resultAsJson = doc => JSON.parse(doc[valueKey] || '{}');

/**
 * Execute queries within a transaction
 * @internal
 * @param connection
 * @param transactions
 */
export const safeTransaction = async (pool: ConnectionPool, transactions: (request: Request) => Promise<any>) => {
  const transaction = new Transaction(pool);
  try {
    await transaction.begin();
    const request = new Request(transaction);
    await transactions(request);
    await transaction.commit();
  }
  catch (error) {
    console.error(error);
    await transaction.rollback();
  }
};

/**
 * @internal
 */
export class MSSQLStorage implements IJioStorage {
  private _dbPromise: IQueue<ConnectionPool>;
  private _pool: ConnectionPool;
  private _documentsTable: string;
  private _attachmentsTable: string;

  /**
   * Initiate a MSSQL Storage.
   * @param options Storage options
   */
  constructor(options: IMSSQLStorageOptions) {
    if (typeof options.server !== 'string' || !options.server) {
      throw new Error('"server" must be a non-empty string');
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
  private initDb(options: IMSSQLStorageOptions) {
    const database = options.database;
    delete options.database;

    return getQueue()
      .push(() => new ConnectionPool(options))
      .push(async pool => {
        try {
          await pool.connect();
          const request = pool.request();
          await request.query(createDatabase(database));
        }
        catch (_err) {}
        finally {
          await pool.close();
        }
      })
      .push(() => {
        options.database = database;
        return this.initTables(options);
      });
  }

  private initTables(options: IMSSQLStorageOptions) {
    const pool = new ConnectionPool(options);
    return getQueue()
      .push(() => promiseToQueue(pool.connect()))
      .push(async () => {
        const request = pool.request();
        await (request.query(createDocumentsTable(this._documentsTable)).catch(() => {}));
        await (request.query(createAttachmentsTable(this._attachmentsTable)).catch(() => {}));

        // create indexes on id keys
        await (request.query(indexTable(this._documentsTable, [idKey])).catch(() => {}));
        await (request.query(indexTable(this._attachmentsTable, [idKey])).catch(() => {}));
        // create index on id key + name for attachments
        await (request.query(indexTable(this._attachmentsTable, [idKey, 'name'])).catch(() => {}));
      })
      .push(() => this._pool = pool);
  }

  /**
   * Get the current active pool
   * @internal
   */
  private pool() {
    return getQueue().push(() => this._pool ? this._pool : this._dbPromise);
  }

  /**
   * Prepare a request
   * @internal
   */
  private request() {
    return this.pool().push(pool => pool.request());
  }

  /**
   * Execute a transaction.
   * @internal
   * @param sql
   * @param values
   */
  private executeTransaction(sql: string, values: {[key: string]: string} = {}) {
    return this.pool().push(pool => {
      return promiseToQueue(safeTransaction(pool, request => {
        Object.keys(values).forEach(key => {
          request.input(key, VarChar, values[key]);
        });
        return request.query(sql);
      }));
    });
  }

  /**
   * Execute a query.
   * @internal
   * @param sql
   * @param values
   */
  private executeQuery(sql: string, values: {[key: string]: string} = {}) {
    return this.request().push(request => {
      Object.keys(values).forEach(key => {
        request.input(key, VarChar, values[key]);
      });
      return promiseToQueue(request.query(sql));
    });
  }

  get(id: string) {
    return this.executeQuery(
      `SELECT * FROM ${this._documentsTable} WHERE ${idKey}=@id`,
      {id}
    ).push(result => {
      return result && result.recordset.length ? resultAsJson(result.recordset[0]) : null;
    });
  }

  put(id: string, data: any) {
    return this.get(id)
      .push(document => {
        if (!document) {
          return this.executeTransaction(
            `INSERT INTO ${this._documentsTable} (${idKey}, ${valueKey}) VALUES (@id, @data)`,
            {id, data: JSON.stringify(data)}
          );
        }

        return this.executeTransaction(
          `UPDATE ${this._documentsTable} SET ${valueKey}=@data WHERE ${idKey}=@id`,
          {id, data: JSON.stringify(data)}
        );
      })
      .push(() => {
        return id;
      });
  }

  remove(id: string) {
    return this.executeTransaction(
      `DELETE FROM ${this._documentsTable} WHERE ${idKey}=@id`,
      {id}
    ).push(() => {
      return id;
    });
  }

  getAttachment(id: string, name: string) {
    return this.executeQuery(
      `SELECT * FROM ${this._attachmentsTable} WHERE ${idKey}=@id AND name=@name`,
      {id, name}
    ).push(result => {
      if (result.recordset && result.recordset.length) {
        return jIO.util.dataURItoBlob(result.recordset[0][valueKey]);
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
          `INSERT INTO ${this._attachmentsTable} (${idKey}, name, ${valueKey}) VALUES (@id, @name, @data)`,
          {id, name, data: data.target.result}
        );
      });
  }

  removeAttachment(id: string, name: string) {
    return this.executeTransaction(
      `DELETE FROM ${this._attachmentsTable} WHERE ${idKey}=@id AND name=@name`,
      {id, name}
    ).push(() => {
      return id;
    });
  }

  allAttachments(id: string) {
    return this.executeQuery(
      `SELECT * FROM ${this._attachmentsTable} WHERE ${idKey}=@id`,
      {id}
    ).push(result => {
      const attachments = {};
      if (result.recordset && result.recordset.length) {
        result.recordset.forEach(document => {
          attachments[document.name] = {};
        });
      }
      return attachments;
    });
  }

  /**
   * MSSQL can not search on json objects, therefore we can only list the documents
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
      limit = `OFFSET ${options.limit[0] || 0} ROWS FETCH NEXT ${options.limit[1] || 100} ROWS ONLY`;
    }
    const selectList = options.select_list || [];

    return this.executeQuery(
      `SELECT * FROM ${this._documentsTable} ${limit}`
    ).push(result => {
      return result.recordset.map(document => {
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
      });
    });
  }
}

export interface IMSSQLOptions extends IClearRoadOptions {
  localStorage: IMSSQLStorageOptions;
  /**
   * Microsoft SQL Server does NOT support JSON queries
   */
  useQueryStorage: true;
}

const storageName = 'mssql';
jIO.addStorage(storageName, MSSQLStorage);
export default storageName;
