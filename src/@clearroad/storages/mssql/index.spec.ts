import { expect } from 'chai';
import * as sinon from 'sinon';
import 'mocha';

import { IJioQueryOptions } from '@clearroad/api';
import * as jioImport from 'jio';
const addStorageStub = sinon.stub(jioImport.jIO, 'addStorage');

import * as mssql from 'mssql';

import * as specs from './index';
import storageName, {
  MSSQLStorage, IMSSQLStorageOptions,
  defaultDocumentsCollection, defaultAttachmentsCollection,
  safeTransaction,
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

class FakePool {
  request() {
    return new FakeRequest();
  }
  connect() {
    return Promise.resolve();
  }
  close() {
    return Promise.resolve();
  }
}

class FakeRequest {
  query() {
    return Promise.resolve();
  }
  input() {}
}

class FakeTransaction extends FakeRequest {
  begin() {
    return Promise.resolve();
  }
  commit() {
    return Promise.resolve();
  }
  rollback() {
    return Promise.resolve();
  }
  query() {
    return Promise.resolve();
  }
}

const options: IMSSQLStorageOptions = {
  database: 'database',
  type: 'mssql',
  server: 'url'
};

const requestStub = (storage: MSSQLStorage, request: FakeRequest) => {
  const poolQueue = new FakeQueue();
  const pool = new FakePool();
  stubs.push(sinon.stub(pool, 'request').returns(request));
  poolQueue.push(() => pool);
  stubs.push(sinon.stub((storage as any), 'pool').returns(poolQueue));
};

describe(storageName, () => {
  let transaction: FakeTransaction;

  beforeEach(() => {
    stubs = [];
    stubs.push(sinon.stub(mssql, 'ConnectionPool').returns(new FakePool()));
    transaction = new FakeTransaction();
    stubs.push(sinon.stub(mssql, 'Transaction').returns(transaction));
    stubs.push(sinon.stub(mssql, 'Request').returns(new FakeRequest()));
  });

  afterEach(() => {
    stubs.forEach(stub => stub.restore());
  });

  it('should add the storage', () => {
    expect(addStorageStub.calledWith(storageName, MSSQLStorage)).to.equal(true);
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
    let rollbackStub: sinon.SinonStub;

    beforeEach(() => {
      rollbackStub = sinon.stub(transaction, 'rollback').returns(Promise.resolve());
      stubs.push(rollbackStub);
    });

    describe('success', () => {
      it('should not rollback', async () => {
        await safeTransaction(new FakePool() as any, () => Promise.resolve());
        expect(rollbackStub.called).to.equal(false);
      });
    });

    describe('failure', () => {
      it('should rollback', async () => {
        await safeTransaction(new FakePool() as any, () => Promise.reject());
        expect(rollbackStub.called).to.equal(true);
      });
    });
  });

  describe('MSSQLStorage', () => {
    let request: FakeRequest;

    beforeEach(() => {
      request = new FakeRequest();
      (request as any).id = 'from safe tr';
      stubs.push(sinon.stub(specs, 'safeTransaction').callsFake((_pool, transactions) => transactions(request)));
    });

    describe('constructor', () => {
      const fakeOptions: any = {};

      beforeEach(() => {
        stubs.push(sinon.stub(MSSQLStorage.prototype as any, 'initDb'));
      });

      describe('without a "server"', () => {
        it('should throw an error', () => {
          expect(() => new MSSQLStorage(fakeOptions)).to.throw('"server" must be a non-empty string');
        });
      });

      describe('with a "server', () => {
        beforeEach(() => {
          fakeOptions.server = 'mssql://';
        });

        describe('without a "database"', () => {
          it('should throw an error', () => {
            expect(() => new MSSQLStorage(fakeOptions)).to.throw('"database" must be a non-empty string');
          });
        });

        describe('with a "database', () => {
          beforeEach(() => {
            fakeOptions.database = 'database';
          });

          it('should call init', () => {
            new MSSQLStorage(fakeOptions);
            expect((MSSQLStorage.prototype as any).initDb.called).to.equal(true);
          });

          describe('with "timestamps', () => {
            beforeEach(() => {
              fakeOptions.timestamps = true;
            });

            it('should enable timestamps', () => {
              const storage = new MSSQLStorage(fakeOptions);
              expect((storage as any)._timestamps).to.equal(true);
            });
          });

          describe('without "timestamps', () => {
            beforeEach(() => {
              fakeOptions.timestamps = false;
            });

            it('should disable timestamps', () => {
              const storage = new MSSQLStorage(fakeOptions);
              expect((storage as any)._timestamps).to.equal(false);
            });
          });
        });
      });
    });

    describe('.initDb', () => {
      it('should create a pool', async () => {
        const storage: any = new MSSQLStorage(options);
        await storage._dbPromise;
        expect(storage._pool instanceof FakePool).to.equal(true);
      });
    });

    describe('.get', () => {
      let storage: MSSQLStorage;
      const id = 'id';
      let stub: sinon.SinonStub;

      beforeEach(() => {
        stubs.push(sinon.stub(specs, 'resultAsJson').callsFake(val => val));

        storage = new MSSQLStorage(options);
        requestStub(storage, request);

        stub = sinon.stub(request, 'query').returns({recordset: [{}]});
        stubs.push(stub);
      });

      it('should find by id', () => {
        storage.get(id);
        expect(stub.calledWith(
          `SELECT * FROM ${defaultDocumentsCollection} WHERE _id=@id`
        )).to.equal(true);
      });

      describe('document found', () => {
        const document = {
          [valueKey]: 1
        };

        beforeEach(() => {
          stub.returns({recordset: [document]});
        });

        it('should return the document', () => {
          const res: any = storage.get(id);
          expect(res.result).to.deep.equal(document);
        });
      });

      describe('document not found', () => {
        beforeEach(() => {
          stub.returns({recordset: []});
        });

        it('should return the document', () => {
          const res: any = storage.get(id);
          expect(res.result).to.equal(null);
        });
      });
    });

    describe('.put', () => {
      let storage: MSSQLStorage;
      const id = 'id';
      const data = {test: 1};
      let stub: sinon.SinonStub;

      beforeEach(async () => {
        storage = new MSSQLStorage(options);
        await (storage as any)._dbPromise;
        requestStub(storage, request);

        stub = sinon.stub(request, 'query').returns({recordset: []});
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
            expect(stub.calledWith(
              `UPDATE ${defaultDocumentsCollection} SET value=@data, updatedAt=GETDATE() WHERE _id=@id`
            )).to.equal(true);
          });
        });

        describe('without timestamps', () => {
          beforeEach(() => {
            (storage as any)._timestamps = false;
          });

          it('should update data', () => {
            storage.put(id, data);
            expect(stub.calledWith(
              `UPDATE ${defaultDocumentsCollection} SET value=@data WHERE _id=@id`
            )).to.equal(true);
          });
        });
      });

      describe('document does not exist', () => {
        beforeEach(() => {
          stubs.push(sinon.stub(storage, 'get').returns(new FakeQueue()));
        });

        it('should insert data', () => {
          storage.put(id, data);
          expect(stub.calledWith(
            `INSERT INTO ${defaultDocumentsCollection} (_id, value) VALUES (@id, @data)`
          )).to.equal(true);
        });
      });
    });

    describe('.remove', () => {
      let storage: MSSQLStorage;
      const id = 'id';
      let stub: sinon.SinonStub;

      beforeEach(() => {
        storage = new MSSQLStorage(options);
        requestStub(storage, request);

        stub = sinon.stub(request, 'query').returns(new FakeQueue());
        stubs.push(stub);
      });

      it('should remove by id', () => {
        storage.remove(id);
        expect(stub.calledWith(
          `DELETE FROM ${defaultDocumentsCollection} WHERE _id=@id`
        )).to.equal(true);
      });
    });

    describe('.getAttachment', () => {
      let storage: MSSQLStorage;
      const id = 'id';
      const name = 'name';
      const attachment = 'attachment';
      let stub: sinon.SinonStub;

      beforeEach(() => {
        stubs.push(sinon.stub(specs, 'resultAsJson').callsFake(val => val));
        stubs.push(sinon.stub(jioImport.jIO.util, 'dataURItoBlob').returns(attachment));

        storage = new MSSQLStorage(options);
        requestStub(storage, request);
      });

      describe('attachment found', () => {
        beforeEach(() => {
          stub = sinon.stub(request, 'query').returns({recordset: [{}]});
          stubs.push(stub);
        });

        it('should return the attachment', () => {
          const result: any = storage.getAttachment(id, name);
          expect(result.result).to.equal(attachment);
        });
      });

      describe('attachment not found', () => {
        beforeEach(() => {
          stub = sinon.stub(request, 'query').returns([]);
          stubs.push(stub);
        });

        it('should throw an error', () => {
          expect(() => storage.getAttachment(id, name)).to.throw(`Cannot find attachment: ${id}`);
        });
      });
    });

    describe('.putAttachment', () => {
      let storage: MSSQLStorage;
      const id = 'id';
      const name = 'name';
      const data: any = {
        target: {result: {}}
      };
      let stub: sinon.SinonStub;

      beforeEach(async () => {
        storage = new MSSQLStorage(options);
        await (storage as any)._dbPromise;
        requestStub(storage, request);

        stub = sinon.stub(request, 'query').returns([]);
        stubs.push(stub);
        stubs.push(sinon.stub(jioImport.jIO.util, 'readBlobAsDataURL').returns(data));
      });

      it('should insert data', async () => {
        await storage.putAttachment(id, name, data);
        expect(stub.calledWith(
          `INSERT INTO ${defaultAttachmentsCollection} (_id, name, value) VALUES (@id, @name, @data)`
        )).to.equal(true);
      });
    });

    describe('.removeAttachment', () => {
      let storage: MSSQLStorage;
      const id = 'id';
      const name = 'name';
      let stub: sinon.SinonStub;

      beforeEach(() => {
        storage = new MSSQLStorage(options);
        requestStub(storage, request);

        stub = sinon.stub(request, 'query').returns([]);
        stubs.push(stub);
      });

      it('should remove by id', () => {
        storage.removeAttachment(id, name);
        expect(stub.calledWith(
          `DELETE FROM ${defaultAttachmentsCollection} WHERE _id=@id AND name=@name`
        )).to.equal(true);
      });
    });

    describe('.allAttachments', () => {
      let storage: MSSQLStorage;
      const id = 'id';
      const attachments = [{
        name: 'attachment 1'
      }];

      beforeEach(() => {
        storage = new MSSQLStorage(options);
        requestStub(storage, request);

        stubs.push(sinon.stub(request, 'query').returns({recordset: attachments}));
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
        const storage = new MSSQLStorage(options);
        expect(storage.hasCapacity('list')).to.equal(true);
      });

      it('should not support queries', () => {
        const storage = new MSSQLStorage(options);
        expect(storage.hasCapacity('query')).to.equal(false);
      });
    });

    describe('.buildQuery', () => {
      let storage: MSSQLStorage;
      let params: IJioQueryOptions;
      const results = [{
        _id: 1,
        title: 'title',
        description: 'description'
      }];
      let stub: sinon.SinonStub;

      beforeEach(() => {
        stubs.push(sinon.stub(MSSQLStorage.prototype as any, 'initDb'));
        stubs.push(sinon.stub(specs, 'resultAsJson').callsFake(val => val));

        storage = new MSSQLStorage(options);
        requestStub(storage, request);

        stub = sinon.stub(request, 'query').returns({recordset: results});
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
          expect(stub.calledWith(
            `SELECT * FROM ${defaultDocumentsCollection} OFFSET ${params.limit![0]} ROWS FETCH NEXT ${params.limit![1]} ROWS ONLY`
          )).to.equal(true);
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
