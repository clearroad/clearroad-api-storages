const RSVP = require('rsvp');
import {
  jIO, getQueue, promiseToQueue,
  IJioStorage, IQueue, IClearRoadOptions,
  IJioQueryOptions, IJioSimpleQuery, IJioComplexQuery
} from '@clearroad/api';

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

const parseSimpleQuery = (query: IJioSimpleQuery, key = '') => {
  let operator: string;
  switch (query.operator || '=') {
    case '!=':
      operator = '$ne';
      break;
    case '<':
      operator = '$lt';
      break;
    case '<=':
      operator = '$lte';
      break;
    case '>':
      operator = '$gt';
      break;
    case '>=':
      operator = '$gte';
      break;
    default:
      operator = '$eq';
  }
  return {
    [`${valueKey}.${key}`]: {
      [operator]: query.value
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
}

/**
 * @internal
 */
export class MongoDBStorage implements IJioStorage {
  private _dbPromise: IQueue<Db>;
  private _db: Db;
  private _documentsCollection: Collection;
  private _attachmentsCollection: Collection;

  /**
   * Initiate a MongoDB Storage.
   * @param options Storage options
   */
  constructor(options: IMongoDBStorageOptions) {
    if (typeof options.url !== 'string' || !options.url) {
      throw new Error('"url" must be a non-empty string');
    }
    if (typeof options.database !== 'string' || !options.database) {
      throw new Error('"database" must be a non-empty string');
    }
    if (!options.documentsCollectionName) {
      options.documentsCollectionName = defaultDocumentCollection;
    }
    if (!options.attachmentsCollectionName) {
      options.attachmentsCollectionName = defaultAttachmentsCollection;
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
        return RSVP.all([
          this._documentsCollection.createIndex({
            [idKey]: 1
          }),
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
        if (document) {
          return document[valueKey];
        }
        return document;
      });
  }

  put(id: string, data: any) {
    return this.db()
      .push(() => {
        return this.get(id);
      })
      .push((document: any): IQueue<any> => {
        if (!document) {
          return promiseToQueue(this._documentsCollection.insertOne({
            [idKey]: id,
            [valueKey]: data
          }));
        }
        return promiseToQueue(this._documentsCollection.updateOne({
          [idKey]: id
        }, {
          $set: {
            [valueKey]: data
          }
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
        return promiseToQueue(this._attachmentsCollection.insertOne({
          [idKey]: id,
          name,
          [valueKey]: data.target.result
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
    let parsedQuery: any = {};
    if (options.query) {
      const parsed = jIO.QueryFactory.create(options.query);
      parsedQuery = parseQuery(parsed);
    }
    const findOptions: FindOneOptions = {};
    if (options.sort_on) {
      const sortOn = (options.sort_on || []).map(values => [`${valueKey}.${values[0]}`, values[1]]);
      // sorting on undefined field will return nothing, make sure to sort on id as well as fallback
      sortOn.push([idKey, 'descending']);
      findOptions.sort = sortOn;
    }
    if (options.limit) {
      findOptions.skip = options.limit[0] || 0;
      findOptions.limit = options.limit[1] || 100;
    }
    const selectList = (options.select_list || []).slice();

    return this.db()
      .push(() => {
        return promiseToQueue(this._documentsCollection.find(parsedQuery, findOptions).toArray());
      })
      .push(documents => {
        return documents.map(document => {
          const value: any = {
            id: document[idKey]
          };
          if (options.include_docs) {
            value.doc = document[valueKey];
          }
          else if (options.select_list) {
            value.value = {};
            selectList.forEach(key => value.value[key] = document[valueKey][key]);
          }
          return value;
        });
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
