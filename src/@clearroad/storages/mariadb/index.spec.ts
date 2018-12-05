import { expect } from 'chai';
import * as sinon from 'sinon';
import 'mocha';

import { IJioQueryOptions } from '@clearroad/api';
import * as jioImport from 'jio';
const addStorageStub = sinon.stub(jioImport.jIO, 'addStorage');

const mariadb = require('mariadb');

import * as specs from './index';
import storageName, {
  MariaDBStorage, IMariaDBStorageOptions,
  defaultDocumentsCollection, defaultAttachmentsCollection,
  IConnection, IPool, safeTransaction, safeQuery,
  resultAsJson, valueKey
} from './index';

let stubs: sinon.SinonStub[] = [];

class FakeQueue {
  private result;
  push(callback) {
    if (this.result instanceof FakeQueue) {
      return this.result.push(callback);
    }
    this.result = callback(this.result);
    return this;
  }
}

class FakePool implements IPool {
  getConnection() {
    return Promise.resolve(new FakeConnection());
  }
}

class FakeConnection implements IConnection {
  beginTransaction() {
    return Promise.resolve();
  }
  commit() {
    return Promise.resolve();
  }
  rollback() {
    return Promise.resolve();
  }
  end() {
    return Promise.resolve();
  }
  query<T>() {
    return Promise.resolve({} as T);
  }
}

const options: IMariaDBStorageOptions = {
  database: 'database',
  type: 'mariadb',
  host: 'url'
};

const connectionStub = (storage: MariaDBStorage) => {
  const queue = new FakeQueue();
  const connection = new FakeConnection();
  queue.push(() => connection);
  stubs.push(sinon.stub((storage as any), 'connection').returns(queue));
  return connection;
};

describe(storageName, () => {
  beforeEach(() => {
    stubs = [];
    stubs.push(sinon.stub(mariadb, 'createPool').returns(new FakePool()));
    stubs.push(sinon.stub(mariadb, 'createConnection').returns(new FakeConnection()));
  });

  afterEach(() => {
    stubs.forEach(stub => stub.restore());
  });

  it('should add the storage', () => {
    expect(addStorageStub.calledWith(storageName, MariaDBStorage)).to.equal(true);
  });

  describe('resultAsJson', () => {
    it('should parse the value as object', () => {
      const object = {
        title: 'title',
        description: 'description'
      };
      const value = {
        _id: 'id',
        value: JSON.stringify(object)
      };
      expect(resultAsJson(value)).to.deep.equal(object);
    });
  });

  describe('safeTransaction', () => {
    let connection: IConnection;
    let rollbackStub: sinon.SinonStub;

    beforeEach(() => {
      connection = new FakeConnection();
      rollbackStub = sinon.stub(connection, 'rollback').returns(Promise.resolve());
      stubs.push(rollbackStub);
    });

    describe('success', () => {
      it('should not rollback', async () => {
        await safeTransaction(connection, () => Promise.resolve());
        expect(rollbackStub.called).to.equal(false);
      });
    });

    describe('failure', () => {
      it('should rollback', async () => {
        await safeTransaction(connection, () => Promise.reject());
        expect(rollbackStub.called).to.equal(true);
      });
    });
  });

  describe('safeQuery', () => {
    let connection: IConnection;

    beforeEach(() => {
      connection = new FakeConnection();
    });

    describe('success', () => {
      it('should return the results', async () => {
        const results = [{
          id: 1
        }];
        const res = await safeQuery(connection, () => Promise.resolve(results));
        expect(res).to.deep.equal(results);
      });
    });

    describe('failure', () => {
      it('should return the null', async () => {
        const res = await safeQuery(connection, () => Promise.reject());
        expect(res).to.equal(null);
      });
    });
  });

  describe('MariaDBStorage', () => {
    beforeEach(() => {
      stubs.push(sinon.stub(specs, 'safeTransaction').callsFake((_conn, transactions) => transactions()));
      stubs.push(sinon.stub(specs, 'safeQuery').callsFake((_conn, query) => query()));
    });

    describe('constructor', () => {
      const fakeOptions: any = {};

      beforeEach(() => {
        stubs.push(sinon.stub(MariaDBStorage.prototype as any, 'initDb'));
      });

      describe('without a "host"', () => {
        it('should throw an error', () => {
          expect(() => new MariaDBStorage(fakeOptions)).to.throw('"host" must be a non-empty string');
        });
      });

      describe('with a "host', () => {
        beforeEach(() => {
          fakeOptions.host = 'mysql://';
        });

        describe('without a "database"', () => {
          it('should throw an error', () => {
            expect(() => new MariaDBStorage(fakeOptions)).to.throw('"database" must be a non-empty string');
          });
        });

        describe('with a "database', () => {
          beforeEach(() => {
            fakeOptions.database = 'database';
          });

          it('should call init', () => {
            new MariaDBStorage(fakeOptions);
            expect((MariaDBStorage.prototype as any).initDb.called).to.equal(true);
          });

          describe('with "timestamps', () => {
            beforeEach(() => {
              fakeOptions.timestamps = true;
            });

            it('should enable timestamps', () => {
              const storage = new MariaDBStorage(fakeOptions);
              expect((storage as any)._timestamps).to.equal(true);
            });
          });

          describe('without "timestamps', () => {
            beforeEach(() => {
              fakeOptions.timestamps = false;
            });

            it('should disable timestamps', () => {
              const storage = new MariaDBStorage(fakeOptions);
              expect((storage as any)._timestamps).to.equal(false);
            });
          });
        });
      });
    });

    describe('.initDb', () => {
      it('should create a pool', async () => {
        const storage: any = new MariaDBStorage(options);
        await storage._dbPromise;
        expect(storage._pool instanceof FakePool).to.equal(true);
      });
    });

    describe('.get', () => {
      let storage: MariaDBStorage;
      const id = 'id';
      let stub: sinon.SinonStub;

      beforeEach(() => {
        stubs.push(sinon.stub(specs, 'resultAsJson').callsFake(val => val));
        storage = new MariaDBStorage(options);
        const connection = connectionStub(storage);
        stub = sinon.stub(connection, 'query').returns([{}]);
        stubs.push(stub);
      });

      it('should find by id', () => {
        storage.get(id);
        expect(stub.calledWith({
          namedPlaceholders: true,
          sql: `SELECT * FROM ${defaultDocumentsCollection} WHERE _id=:id`
        }, {id})).to.equal(true);
      });

      describe('document found', () => {
        const document = {
          [valueKey]: 1
        };

        beforeEach(() => {
          stub.returns([document]);
        });

        it('should return the document', () => {
          const res: any = storage.get(id);
          expect(res.result).to.deep.equal(document);
        });
      });

      describe('document not found', () => {
        beforeEach(() => {
          stub.returns([]);
        });

        it('should return the document', () => {
          const res: any = storage.get(id);
          expect(res.result).to.equal(null);
        });
      });
    });

    describe('.put', () => {
      let storage: MariaDBStorage;
      const id = 'id';
      const data = {test: 1};
      let stub: sinon.SinonStub;

      beforeEach(async () => {
        storage = new MariaDBStorage(options);
        await (storage as any)._dbPromise;
        const connection = connectionStub(storage);

        stub = sinon.stub(connection, 'query').returns([]);
        stubs.push(stub);
      });

      describe('document exists', () => {
        beforeEach(() => {
          const queue = new FakeQueue();
          queue.push(() => data);
          stubs.push(sinon.stub(storage, 'get').returns(queue));
        });

        describe('with timestamps', () => {
          beforeEach(() => {
            (storage as any)._timestamps = true;
          });

          it('should update data', () => {
            storage.put(id, data);
            expect(stub.calledWith({
              namedPlaceholders: true,
              sql: `UPDATE ${defaultDocumentsCollection} SET value=:data, updatedAt=CURRENT_TIMESTAMP WHERE _id=:id`
            }, {id, data: JSON.stringify(data)})).to.equal(true);
          });
        });

        describe('without timestamps', () => {
          beforeEach(() => {
            (storage as any)._timestamps = false;
          });

          it('should update data', () => {
            storage.put(id, data);
            expect(stub.calledWith({
              namedPlaceholders: true,
              sql: `UPDATE ${defaultDocumentsCollection} SET value=:data WHERE _id=:id`
            }, {id, data: JSON.stringify(data)})).to.equal(true);
          });
        });
      });

      describe('document does not exist', () => {
        beforeEach(() => {
          stubs.push(sinon.stub(storage, 'get').returns(new FakeQueue()));
        });

        describe('with timestamps', () => {
          beforeEach(() => {
            (storage as any)._timestamps = true;
          });

          it('should insert data', () => {
            storage.put(id, data);
            expect(stub.calledWith(
              `INSERT INTO ${defaultDocumentsCollection} VALUES (NULL, ?, ?, CURRENT_TIMESTAMP, NULL)`,
              [id, JSON.stringify(data)]
            )).to.equal(true);
          });
        });

        describe('without timestamps', () => {
          beforeEach(() => {
            (storage as any)._timestamps = false;
          });

          it('should insert data', () => {
            storage.put(id, data);
            expect(stub.calledWith(
              `INSERT INTO ${defaultDocumentsCollection} VALUES (NULL, ?, ?)`,
              [id, JSON.stringify(data)]
            )).to.equal(true);
          });
        });
      });
    });

    describe('.remove', () => {
      let storage: MariaDBStorage;
      const id = 'id';
      let stub: sinon.SinonStub;

      beforeEach(() => {
        storage = new MariaDBStorage(options);
        const connection = connectionStub(storage);

        stub = sinon.stub(connection, 'query').returns(new FakeQueue());
        stubs.push(stub);
      });

      it('should remove by id', () => {
        storage.remove(id);
        expect(stub.calledWith({
          namedPlaceholders: true,
          sql: `DELETE FROM ${defaultDocumentsCollection} WHERE _id=:id`
        }, {id})).to.equal(true);
      });
    });

    describe('.getAttachment', () => {
      let storage: MariaDBStorage;
      const id = 'id';
      const name = 'name';
      const attachment = 'attachment';
      let stub: sinon.SinonStub;

      beforeEach(() => {
        stubs.push(sinon.stub(specs, 'resultAsJson').callsFake(val => val));
        stubs.push(sinon.stub(jioImport.jIO.util, 'dataURItoBlob').returns(attachment));

        storage = new MariaDBStorage(options);
      });

      describe('attachment found', () => {
        beforeEach(() => {
          const connection = connectionStub(storage);
          stub = sinon.stub(connection, 'query').returns([{}]);
          stubs.push(stub);
        });

        it('should return the attachment', () => {
          const result: any = storage.getAttachment(id, name);
          expect(result.result).to.equal(attachment);
        });
      });

      describe('attachment not found', () => {
        beforeEach(() => {
          const connection = connectionStub(storage);
          stub = sinon.stub(connection, 'query').returns([]);
          stubs.push(stub);
        });

        it('should throw an error', () => {
          expect(() => storage.getAttachment(id, name)).to.throw(`Cannot find attachment: ${id}`);
        });
      });
    });

    describe('.putAttachment', () => {
      let storage: MariaDBStorage;
      const id = 'id';
      const name = 'name';
      const data: any = {
        target: {result: {}}
      };
      let stub: sinon.SinonStub;

      beforeEach(async () => {
        storage = new MariaDBStorage(options);
        await (storage as any)._dbPromise;
        const connection = connectionStub(storage);

        stub = sinon.stub(connection, 'query').returns([]);
        stubs.push(stub);
        stubs.push(sinon.stub(jioImport.jIO.util, 'readBlobAsDataURL').returns(data));
      });

      describe('with timestamps', () => {
        beforeEach(() => {
          (storage as any)._timestamps = true;
        });

        it('should insert data', async () => {
          await storage.putAttachment(id, name, data);
          expect(stub.calledWith(
            `INSERT INTO ${defaultAttachmentsCollection} VALUES (NULL, ?, ?, ?, CURRENT_TIMESTAMP, NULL)`,
            [id, name, data.target.result]
          )).to.equal(true);
        });
      });

      describe('without timestamps', () => {
        beforeEach(() => {
          (storage as any)._timestamps = false;
        });

        it('should insert data', async () => {
          await storage.putAttachment(id, name, data);
          expect(stub.calledWith(
            `INSERT INTO ${defaultAttachmentsCollection} VALUES (NULL, ?, ?, ?)`,
            [id, name, data.target.result]
          )).to.equal(true);
        });
      });
    });

    describe('.removeAttachment', () => {
      let storage: MariaDBStorage;
      const id = 'id';
      const name = 'name';
      let stub: sinon.SinonStub;

      beforeEach(() => {
        storage = new MariaDBStorage(options);
        const connection = connectionStub(storage);

        stub = sinon.stub(connection, 'query').returns([]);
        stubs.push(stub);
      });

      it('should remove by id', () => {
        storage.removeAttachment(id, name);
        expect(stub.calledWith({
          namedPlaceholders: true,
          sql: `DELETE FROM ${defaultAttachmentsCollection} WHERE _id=:id AND name=:name`
        }, {id, name})).to.equal(true);
      });
    });

    describe('.allAttachments', () => {
      let storage: MariaDBStorage;
      const id = 'id';
      const attachments = [{
        name: 'attachment 1'
      }];

      beforeEach(() => {
        storage = new MariaDBStorage(options);
        const connection = connectionStub(storage);

        stubs.push(sinon.stub(connection, 'query').returns(attachments));
      });

      it('should return a list of attachments', () => {
        const results = storage.allAttachments(id);
        expect((results as any).result).to.deep.equal({
          [attachments[0].name]: {}
        });
      });
    });

    describe('.hasCapacity', () => {
      it('should support list', () => {
        const storage = new MariaDBStorage(options);
        expect(storage.hasCapacity('list')).to.equal(true);
      });

      it('should not support queries', () => {
        const storage = new MariaDBStorage(options);
        expect(storage.hasCapacity('query')).to.equal(false);
      });
    });

    describe('.buildQuery', () => {
      let storage: MariaDBStorage;
      let params: IJioQueryOptions;
      const results = [{
        _id: 1,
        title: 'title',
        description: 'description'
      }];
      let stub: sinon.SinonStub;

      beforeEach(() => {
        stubs.push(sinon.stub(MariaDBStorage.prototype as any, 'initDb'));
        stubs.push(sinon.stub(specs, 'resultAsJson').callsFake(val => val));

        storage = new MariaDBStorage(options);
        const connection = connectionStub(storage);

        stub = sinon.stub(connection, 'query').returns(results);
        stubs.push(stub);

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
          expect(stub.calledWith({
            sql: `SELECT * FROM ${defaultDocumentsCollection} LIMIT ${params.limit![1]} OFFSET ${params.limit![0]}`
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
              doc: result
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
                title: result.title
              }
            };
          }));
        });
      });
    });
  });
});

addStorageStub.restore();
