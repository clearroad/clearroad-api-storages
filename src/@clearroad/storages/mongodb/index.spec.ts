import { expect } from 'chai';
import * as sinon from 'sinon';
import 'mocha';

import { IJioQueryOptions } from '@clearroad/api';
const jioImport = require('@clearroad/api/dist/node/lib/jio.js');
const addStorageStub = sinon.stub(jioImport.jIO, 'addStorage');

import * as mongodb from 'mongodb';

import * as specs from './index';
import storageName, { MongoDBStorage, parseQuery, IMongoDBStorageOptions, primaryKey, valueKey } from './index';

let stubs: sinon.SinonStub[] = [];

class FakeQueue {
  private result;
  push(callback) {
    this.result = callback(this.result);
    return this;
  }
}

class FakeMongoClient {
  db() {
    return new FakeDb();
  }
}

class FakeDb {
  collection() {
    return new FakeCollection();
  }
}

class FakeCollection {
  createIndex() {}
  findOne() {}
  find() {}
  insertOne() {}
  updateOne() {}
  deleteOne() {}
}

const options: IMongoDBStorageOptions = {
  database: 'database',
  type: 'mongodb',
  url: 'url'
};

describe(storageName, () => {
  beforeEach(() => {
    stubs = [];
    stubs.push(sinon.stub(mongodb.MongoClient, 'connect').returns(new FakeMongoClient()));
  });

  afterEach(() => {
    stubs.forEach(stub => stub.restore());
  });

  it('should add the storage', () => {
    expect(addStorageStub.calledWith(storageName, MongoDBStorage)).to.equal(true);
  });

  describe('parseQuery', () => {
    it('should parse complex queries', () => {
      const date = new Date();
      const query = `portal_type:("Billing Period Message" OR "Road Account Message" OR "Road Event Message" OR "Road Message" OR "Road Report Request") AND grouping_reference:"data" AND modification_date: != "${date.toJSON()}"`;
      const parsed = jioImport.jIO.QueryFactory.create(query);
      expect(parseQuery(parsed)).to.deep.equal({
        $and: [{
          $or: [
            {'doc.portal_type': {$eq: 'Billing Period Message'}},
            {'doc.portal_type': {$eq: 'Road Account Message'}},
            {'doc.portal_type': {$eq: 'Road Event Message'}},
            {'doc.portal_type': {$eq: 'Road Message'}},
            {'doc.portal_type': {$eq: 'Road Report Request'}}
          ]
        }, {
          'doc.grouping_reference': {$eq: 'data'}
        }, {
          'doc.modification_date': {$ne: date.toJSON()}
        }]
      });
    });

    it('should parse simple queries', () => {
      const query = 'date1: >= 1 AND date2: > 2 AND date3: < 3 AND date4: <= 4';
      const parsed = jioImport.jIO.QueryFactory.create(query);
      expect(parseQuery(parsed)).to.deep.equal({
        $and: [
          {'doc.date1': {$gte: '1'}},
          {'doc.date2': {$gt: '2'}},
          {'doc.date3': {$lt: '3'}},
          {'doc.date4': {$lte: '4'}}
        ]
      });
    });
  });

  describe('MongoDBStorage', () => {
    describe('constructor', () => {
      const fakeOptions: any = {};

      beforeEach(() => {
        stubs.push(sinon.stub(MongoDBStorage.prototype as any, 'initDb'));
      });

      describe('without a "url"', () => {
        it('should throw an error', () => {
          expect(() => new MongoDBStorage(fakeOptions)).to.throw('"url" must be a non-empty string');
        });
      });

      describe('with a "url', () => {
        beforeEach(() => {
          fakeOptions.url = 'mongodb://';
        });

        describe('without a "database"', () => {
          it('should throw an error', () => {
            expect(() => new MongoDBStorage(fakeOptions)).to.throw('"database" must be a non-empty string');
          });
        });

        describe('with a "database', () => {
          beforeEach(() => {
            fakeOptions.database = 'database';
          });

          it('should call init', () => {
            new MongoDBStorage(fakeOptions);
            expect((MongoDBStorage.prototype as any).initDb.called).to.equal(true);
          });
        });
      });
    });

    describe('.initDb', () => {
      it('should create a document collection', async () => {
        const storage: any = new MongoDBStorage(options);
        await storage._dbPromise;
        expect(storage._documentsCollection instanceof FakeCollection).to.equal(true);
      });

      it('should create a attachment collection', async () => {
        const storage: any = new MongoDBStorage(options);
        await storage._dbPromise;
        expect(storage._attachmentsCollection instanceof FakeCollection).to.equal(true);
      });
    });

    describe('.get', () => {
      let storage: MongoDBStorage;
      const id = 'id';
      let stub: sinon.SinonStub;

      beforeEach(() => {
        storage = new MongoDBStorage(options);
        stubs.push(sinon.stub((storage as any), 'db').returns(new FakeQueue()));

        (storage as any)._documentsCollection = new FakeCollection();
        stub = sinon.stub((storage as any)._documentsCollection, 'findOne').returns(new FakeQueue());
        stubs.push(stub);
      });

      it('should find by id', () => {
        storage.get(id);
        expect(stub.calledWith({[primaryKey]: id})).to.equal(true);
      });
    });

    describe('.put', () => {
      let storage: MongoDBStorage;
      const id = 'id';
      const data = {id: 1};
      let updateStub: sinon.SinonStub;
      let insertStub: sinon.SinonStub;

      beforeEach(async () => {
        storage = new MongoDBStorage(options);
        await (storage as any)._dbPromise;
        stubs.push(sinon.stub((storage as any), 'db').returns(new FakeQueue()));

        (storage as any)._documentsCollection = new FakeCollection();
        updateStub = sinon.stub((storage as any)._documentsCollection, 'updateOne').returns(new FakeQueue());
        stubs.push(updateStub);
        insertStub = sinon.stub((storage as any)._documentsCollection, 'insertOne').returns(new FakeQueue());
        stubs.push(insertStub);
      });

      describe('document exists', () => {
        beforeEach(() => {
          stubs.push(sinon.stub(storage, 'get').returns({}));
        });

        it('should update data', () => {
          storage.put(id, data);
          expect(updateStub.calledWith({
            [primaryKey]: id
          }, {
            $set: {[valueKey]: data}
          })).to.equal(true);
        });
      });

      describe('document does not exist', () => {
        beforeEach(() => {
          stubs.push(sinon.stub(storage, 'get').returns(null));
        });

        it('should insert data', () => {
          storage.put(id, data);
          expect(insertStub.calledWith({
            [primaryKey]: id,
            [valueKey]: data
          })).to.equal(true);
        });
      });
    });

    describe('.remove', () => {
      let storage: MongoDBStorage;
      const id = 'id';
      let stub: sinon.SinonStub;

      beforeEach(() => {
        storage = new MongoDBStorage(options);
        stubs.push(sinon.stub((storage as any), 'db').returns(new FakeQueue()));

        (storage as any)._documentsCollection = new FakeCollection();
        stub = sinon.stub((storage as any)._documentsCollection, 'deleteOne').returns(new FakeQueue());
        stubs.push(stub);
      });

      it('should remove by id', () => {
        storage.remove(id);
        expect(stub.calledWith({[primaryKey]: id})).to.equal(true);
      });
    });

    describe('.getAttachment', () => {
      let storage: MongoDBStorage;
      const id = 'id';
      const name = 'name';

      beforeEach(() => {
        storage = new MongoDBStorage(options);
        stubs.push(sinon.stub((storage as any), 'db').returns(new FakeQueue()));

        (storage as any)._attachmentsCollection = new FakeCollection();
      });

      describe('attachment found', () => {
        beforeEach(() => {
          stubs.push(sinon.stub((storage as any)._attachmentsCollection, 'findOne').returns({
            [valueKey]: {
              buffer: new ArrayBuffer(0)
            }
          }));
        });

        it('should return a Blob', () => {
          const attachment = storage.getAttachment(id, name);
          expect((attachment as any).result instanceof jioImport.Blob).to.equal(true);
        });
      });

      describe('attachment not found', () => {
        beforeEach(() => {
          stubs.push(sinon.stub((storage as any)._attachmentsCollection, 'findOne').returns(null));
        });

        it('should throw an error', () => {
          expect(() => storage.getAttachment(id, name)).to.throw(`Cannot find attachment: ${id}`);
        });
      });
    });

    describe('.putAttachment', () => {
      let storage: MongoDBStorage;
      const id = 'id';
      const data = new jioImport.Blob([]);
      let stub: sinon.SinonStub;

      beforeEach(() => {
        storage = new MongoDBStorage(options);
        stubs.push(sinon.stub((storage as any), 'db').returns(new FakeQueue()));

        (storage as any)._attachmentsCollection = new FakeCollection();
        stub = sinon.stub((storage as any)._attachmentsCollection, 'insertOne').returns(new FakeQueue());
        stubs.push(stub);
        stubs.push(sinon.stub(jioImport.jIO.util, 'readBlobAsArrayBuffer').returns({
          target: {result: new ArrayBuffer(0)}
        }));
      });

      it('should insert data', () => {
        storage.putAttachment(id, '', data);
        expect(stub.called).to.equal(true);
      });
    });

    describe('.removeAttachment', () => {
      let storage: MongoDBStorage;
      const id = 'id';
      const name = 'name';
      let stub: sinon.SinonStub;

      beforeEach(() => {
        storage = new MongoDBStorage(options);
        stubs.push(sinon.stub((storage as any), 'db').returns(new FakeQueue()));

        (storage as any)._attachmentsCollection = new FakeCollection();
        stub = sinon.stub((storage as any)._attachmentsCollection, 'deleteOne').returns(new FakeQueue());
        stubs.push(stub);
      });

      it('should remove by id', () => {
        storage.removeAttachment(id, name);
        expect(stub.calledWith({[primaryKey]: id, name})).to.equal(true);
      });
    });

    describe('.allAttachments', () => {
      let storage: MongoDBStorage;
      const id = 'id';
      const attachments = [{
        name: 'attachment 1'
      }];

      beforeEach(() => {
        storage = new MongoDBStorage(options);
        stubs.push(sinon.stub((storage as any), 'db').returns(new FakeQueue()));

        (storage as any)._attachmentsCollection = new FakeCollection();

        stubs.push(sinon.stub((storage as any)._attachmentsCollection, 'find').returns(attachments));
      });

      it('should return a list of attachments', () => {
        const results = storage.allAttachments(id);
        expect((results as any).result).to.deep.equal({
          [attachments[0].name]: {}
        });
      });
    });

    describe('.hasCapacity', () => {
      it('should have all capacities', () => {
        const storage = new MongoDBStorage(options);
        expect(storage.hasCapacity()).to.equal(true);
      });
    });

    describe('.buildQuery', () => {
      let storage: MongoDBStorage;
      let params: IJioQueryOptions;
      const results = [{
        [primaryKey]: 1,
        [valueKey]: {
          title: 'title',
          description: 'description'
        }
      }];
      let findStub: sinon.SinonStub;

      beforeEach(() => {
        stubs.push(sinon.stub(MongoDBStorage.prototype as any, 'initDb'));

        storage = new MongoDBStorage(options);
        stubs.push(sinon.stub((storage as any), 'db').returns(new FakeQueue()));

        const collection = new FakeCollection();
        findStub = sinon.stub(collection, 'find').returns({
          toArray: () => results
        });
        stubs.push(findStub);
        (storage as any)._documentsCollection = collection;

        stubs.push(sinon.stub(specs, 'parseQuery').returns({}));

        params = {
          query: 'portal_type: "File"'
        };
      });

      describe('with "limit"', () => {
        beforeEach(() => {
          params.limit = [0, 10];
        });

        it('should set limit', () => {
          storage.buildQuery(params);
          expect(findStub.calledWith({}, {
            skip: params.limit![0],
            limit: params.limit![1]
          })).to.equal(true);
        });
      });

      describe('with "sort_on"', () => {
        beforeEach(() => {
          params.sort_on = [['title', 'ascending']];
        });

        it('should set sort', () => {
          storage.buildQuery(params);
          expect(findStub.calledWith({}, {
            sort: [['doc.title', 'ascending'], [primaryKey, 'descending']]
          })).to.equal(true);
        });
      });

      describe('with "include_docs"', () => {
        beforeEach(() => {
          params.include_docs = true;
        });

        it('should include "doc" in the result', () => {
          const data: any = storage.buildQuery(params);
          expect(data.result).to.deep.equal(results.map(result => {
            return {
              id: 1,
              doc: result[valueKey]
            };
          }));
        });
      });

      describe('with "select_list"', () => {
        beforeEach(() => {
          params.select_list = ['title'];
        });

        it('should include "value" in the result', () => {
          const data: any = storage.buildQuery(params);
          expect(data.result).to.deep.equal(results.map(result => {
            return {
              id: 1,
              value: {
                title: result[valueKey].title
              }
            };
          }));
        });
      });
    });
  });
});

addStorageStub.restore();