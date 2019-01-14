import {
  getQueue, promiseToQueue,
  IJioStorage, IQueue, IClearRoadOptions,
  IJioQueryOptions, IJioSimpleQuery, IJioComplexQuery,
  queryPortalType, queryGroupingReference
} from '@clearroad/api';
import { jIO } from 'jio';

import { MongoClient, Db, Collection, FindOneOptions, Cursor, MongoClientOptions } from 'mongodb';

/**
 * _id is used internally by MongoDB
 * @internal
 */
export const idKey = '__id';
/**
 * @internal
 */
export const valueKey = 'doc';
/**
 * @internal
 */
export const createdAtKey = 'createdAt';
/**
 * @internal
 */
export const updatedAtKey = 'updatedAt';

const keyToDBField = (key: string) => key === 'modification_date' ? createdAtKey : `${valueKey}.${key}`;
const valueToDBValue = (key: string, value: any) => key === 'modification_date' ? new Date(value) : value;

/* tslint:disable:cyclomatic-complexity */
const queryOperator = (operator: string) => {
  switch (operator) {
    case '!=':
      return '$ne';
    case '<':
      return '$lt';
    case '<=':
      return '$lte';
    case '>':
      return '$gt';
    case '>=':
      return '$gte';
    default:
      return '$eq';
  }
};
/* tslint:enable:cyclomatic-complexity */

const parseSimpleQuery = (query: IJioSimpleQuery, key = '') => {
  return {
    [keyToDBField(key)]: {
      [queryOperator(query.operator || '=')]: valueToDBValue(key, query.value)
    }
  };
};

const parseComplexQuery = (query: IJioComplexQuery) => {
  let operator = '';
  switch (query.operator) {
    case 'AND':
      operator = '$and';
      break;
    case 'OR':
      operator = '$or';
      break;
    case 'NOT': // TODO: not tested
      operator = '$not';
      break;
  }
  return {
    [operator]: query.query_list.map(subquery => parseQuery(subquery, query.key))
  };
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

const defaultDocumentCollection = 'Documents';
const defaultAttachmentsCollection = 'Attachments';

export interface IMongoDBStorageOptions {
  type: 'mongodb';
  /**
   * Url connection string. Something like mongodb://<user>:<password>@<host>:<port>
   */
  url: string;
  /**
   * mongodb options
   */
  clientOptions?: MongoClientOptions;
  /**
   * Database name.
   */
  database: string;
  /**
   * Collection name for all documents.
   */
  documentsCollectionName?: string;
  /**
   * Collection name for attachments.
   */
  attachmentsCollectionName?: string;
  /**
   * Add created/updatedAt timestamps for every document.
   * Enabled by default for both
   */
  timestamps?: boolean;
}

/**
 * @internal
 */
export const now = () => new Date();

const requireOptionUrl = (options: IMongoDBStorageOptions) => {
  if (typeof options.url !== 'string' || !options.url) {
    throw new Error('"url" must be a non-empty string');
  }
};

const requireOptionDatabase = (options: IMongoDBStorageOptions) => {
  if (typeof options.database !== 'string' || !options.database) {
    throw new Error('"database" must be a non-empty string');
  }
};

const requireOptionCollectionNames = (options: IMongoDBStorageOptions) => {
  if (!options.documentsCollectionName) {
    options.documentsCollectionName = defaultDocumentCollection;
  }
  if (!options.attachmentsCollectionName) {
    options.attachmentsCollectionName = defaultAttachmentsCollection;
  }
};

const queryParseQuery = (options: IJioQueryOptions) => {
  if (options.query) {
    const parsed = jIO.QueryFactory.create(options.query);
    return parseQuery(parsed);
  }
  return {};
};

const querySort = (options: IJioQueryOptions) => {
  if (options.sort_on) {
    const sortOn = (options.sort_on || []).map(values => [keyToDBField(values[0]), values[1]]);
    // sorting on undefined field will return nothing, make sure to sort on id as well as fallback
    sortOn.push([idKey, 'descending']);
    return sortOn;
  }
  return undefined;
};

const queryLimit = (options: IJioQueryOptions) => {
  if (options.limit) {
    return {
      skip: options.limit[0] || 0,
      limit: options.limit[1] || 100
    };
  }
  return {};
};

const queryProjection = (selectList: string[]) => {
  if (selectList.length) {
    return selectList.reduce((prev, key) => {
      prev[keyToDBField(key)] = 1;
      return prev;
    }, {[idKey]: 1});
  }
  return undefined;
};

const queryParseDocument = (document: any, includeDoc: boolean, selectList: string[]) => {
  const value: any = {
    id: document[idKey]
  };
  if (includeDoc) {
    value.doc = document[valueKey];
  }
  else if (selectList.length) {
    value.value = {};
    selectList.forEach(key => value.value[key] = document[valueKey][key]);
  }
  return value;
};

/**
 * @internal
 */
export class MongoDBStorage implements IJioStorage {
  private _dbPromise: IQueue<Db>;
  private _db: Db;
  private _documentsCollection: Collection;
  private _attachmentsCollection: Collection;
  private _timestamps = true;

  /**
   * Initiate a MongoDB Storage.
   * @param options Storage options
   */
  constructor(options: IMongoDBStorageOptions) {
    requireOptionUrl(options);
    requireOptionDatabase(options);
    requireOptionCollectionNames(options);
    if (options.timestamps === false) {
      this._timestamps = false;
    }
    this._dbPromise = this.initDb(options);
  }

  /**
   * @internal
   */
  private initDb(options: IMongoDBStorageOptions) {
    return getQueue()
      .push(() => {
        return promiseToQueue(MongoClient.connect(options.url, options.clientOptions));
      })
      .push(client => {
        this._db = client.db(options.database);
        this._documentsCollection = this._db.collection(options.documentsCollectionName!);
        this._attachmentsCollection = this._db.collection(options.attachmentsCollectionName!);
        return Promise.all([
          this._documentsCollection.createIndex({
            [idKey]: 1
          }),
          // indexes the most common fields when doing a query
          this._documentsCollection.createIndex({
            [`${valueKey}.${queryPortalType}`]: 1,
            [`${valueKey}.${queryGroupingReference}`]: 1
          }),
          // no need to create index for [idKey] only since MongoDB handles it as well from compound index
          this._attachmentsCollection.createIndex({
            [idKey]: 1,
            name: 1
          })
        ]);
      })
      .push(() => this._db);
  }

  /**
   * @internal
   */
  private db() {
    return getQueue().push(() => {
      if (!this._db) {
        return this._dbPromise;
      }
      return this._db;
    });
  }

  get(id: string) {
    return this.db()
      .push(() => {
        return promiseToQueue(this._documentsCollection.findOne({
          [idKey]: id
        }));
      })
      .push(document => {
        return document ? document[valueKey] : null;
      });
  }

  put(id: string, data: any) {
    return this.db()
      .push(() => {
        return this.get(id);
      })
      .push((document: any): IQueue<any> => {
        const update = {[valueKey]: data};

        if (!document) {
          if (this._timestamps) {
            update[createdAtKey] = now();
          }
          return promiseToQueue(this._documentsCollection.insertOne({
            [idKey]: id,
            ...update
          }));
        }

        if (this._timestamps) {
          update[updatedAtKey] = now();
        }
        return promiseToQueue(this._documentsCollection.updateOne({
          [idKey]: id
        }, {
          $set: update
        }));
      })
      .push(() => id);
  }

  remove(id: string) {
    return this.db()
      .push(() => {
        return promiseToQueue(this._documentsCollection.deleteOne({
          [idKey]: id
        }));
      })
      .push(() => id);
  }

  getAttachment(id: string, name: string) {
    return this.db()
      .push(() => {
        return promiseToQueue(this._attachmentsCollection.findOne({
          [idKey]: id,
          name
        }));
      })
      .push(document => {
        if (document) {
          return jIO.util.dataURItoBlob(document[valueKey]);
        }
        throw new jIO.util.jIOError(
          `Cannot find attachment: ${id}`,
          404
        );
      });
  }

  putAttachment(id: string, name: string, blob: Blob) {
    return this.db()
      .push(() => {
        return jIO.util.readBlobAsDataURL(blob);
      })
      .push(data => {
        const update = {[valueKey]: data.target.result};
        if (this._timestamps) {
          update[createdAtKey] = now();
        }
        return promiseToQueue(this._attachmentsCollection.insertOne({
          [idKey]: id,
          name,
          ...update
        }));
      });
  }

  removeAttachment(id: string, name: string) {
    return this.db()
      .push(() => {
        return promiseToQueue(this._attachmentsCollection.deleteOne({
          [idKey]: id,
          name
        }));
      })
      .push(() => id);
  }

  allAttachments(id: string) {
    return this.db()
      .push(() => {
        return this._attachmentsCollection.find({
          [idKey]: id
        });
      })
      .push((documents: Cursor) => {
        const attachments = {};
        documents.forEach(document => {
          attachments[document.name] = {};
        });
        return attachments;
      });
  }

  hasCapacity() {
    return true;
  }

  buildQuery(options: IJioQueryOptions = {query: ''}) {
    const parsedQuery = queryParseQuery(options);
    const findOptions: FindOneOptions = {};
    findOptions.sort = querySort(options);
    const selectList = (options.select_list || []).slice();
    findOptions.projection = queryProjection(selectList);
    const find = {...findOptions, ...queryLimit(options)};

    return this.db()
      .push(() => {
        return promiseToQueue(this._documentsCollection.find(parsedQuery, find).toArray());
      })
      .push(documents => {
        return documents.map(document => queryParseDocument(document, options.include_docs || false, selectList));
      });
  }
}

export interface IMongoDBOptions extends IClearRoadOptions {
  localStorage: IMongoDBStorageOptions;
  /**
   * MongoDB supports JSON queries
   */
  useQueryStorage?: false;
}

const storageName = 'mongodb';
jIO.addStorage(storageName, MongoDBStorage);
export default storageName;
