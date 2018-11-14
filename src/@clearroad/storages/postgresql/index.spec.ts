import { expect } from 'chai';
import * as sinon from 'sinon';
import 'mocha';

import { IJioQueryOptions } from '@clearroad/api';
import * as jioImport from 'jio';
const addStorageStub = sinon.stub(jioImport.jIO, 'addStorage');

import * as pg from 'pg';

import * as specs from './index';
import storageName, {
  PostgreSQLStorage, IPostgreSQLStorageOptions,
  defaultDocumentsCollection, defaultAttachmentsCollection,
  parseQuery, safeTransaction, safeQuery,
  resultAsJson
} from './index';

let stubs: sinon.SinonStub[] = [];

class FakeQueue {
  private result;
  push(callback) {
    this.result = callback(this.result);
    return this;
  }
}

class FakePool {
  connect() {
    return Promise.resolve(new FakeClient());
  }
}

class FakeClient {
  connect() {
    return Promise.resolve();
  }
  release() {
    return;
  }
  end() {
    return Promise.resolve();
  }
  query() {
    return Promise.resolve({rows: []});
  }
}

const options: IPostgreSQLStorageOptions = {
  database: 'database',
  type: 'postgresql',
  host: 'url'
};

const clientStub = (storage: PostgreSQLStorage) => {
  const queue = new FakeQueue();
  const client = new FakeClient();
  queue.push(() => client);
  stubs.push(sinon.stub((storage as any), 'client').returns(queue));
  return client;
};

describe(storageName, () => {
  beforeEach(() => {
    stubs = [];
    stubs.push(sinon.stub(pg, 'Pool').returns(new FakePool()));
    stubs.push(sinon.stub(pg, 'Client').returns(new FakeClient()));
  });

  afterEach(() => {
    stubs.forEach(stub => stub.restore());
  });

  it('should add the storage', () => {
    expect(addStorageStub.calledWith(storageName, PostgreSQLStorage)).to.equal(true);
  });

  describe('parseQuery', () => {
    it('should parse complex queries', () => {
      const date = new Date();
      const query = `portal_type:("Billing Period Message" OR "Road Account Message" OR "Road Event Message" OR "Road Message" OR "Road Report Request") AND grouping_reference:"data" AND modification_date: != "${date.toJSON()}"`;
      const parsed = jioImport.jIO.QueryFactory.create(query);
      expect(parseQuery(parsed)).to.equal(
        '(' +
          '(' +
            "value ->> 'portal_type' = 'Billing Period Message' " +
            "OR value ->> 'portal_type' = 'Road Account Message' " +
            "OR value ->> 'portal_type' = 'Road Event Message' " +
            "OR value ->> 'portal_type' = 'Road Message' " +
            "OR value ->> 'portal_type' = 'Road Report Request'" +
          ') ' +
          "AND value ->> 'grouping_reference' = 'data' " +
          `AND value ->> 'modification_date' != '${date.toJSON()}'` +
        ')'
      );
    });

    it('should parse simple queries', () => {
      const query = 'date1: >= 1 AND date2: > 2 AND date3: < 3 AND date4: <= 4';
      const parsed = jioImport.jIO.QueryFactory.create(query);
      expect(parseQuery(parsed)).to.equal(
        "(value ->> 'date1' >= '1' AND value ->> 'date2' > '2' AND value ->> 'date3' < '3' AND value ->> 'date4' <= '4')"
      );
    });
  });

  describe('resultAsJson', () => {
    it('should parse the value as object', () => {
      const object = {
        title: 'title',
        description: 'description'
      };
      const value = {
        _id: 'id',
        value: object
      };
      expect(resultAsJson(value)).to.deep.equal(object);
    });
  });

  describe('safeTransaction', () => {
    let client;
    let queryStub: sinon.SinonStub;

    beforeEach(() => {
      client = new FakeClient();
      queryStub = sinon.stub(client, 'query');
      stubs.push(queryStub);
    });

    describe('success', () => {
      it('should not rollback', async () => {
        await safeTransaction(client, () => Promise.resolve());
        expect(queryStub.calledWith('ROLLBACK')).to.equal(false);
      });
    });

    describe('failure', () => {
      it('should rollback', async () => {
        await safeTransaction(client, () => Promise.reject());
        expect(queryStub.calledWith('ROLLBACK')).to.equal(true);
      });
    });
  });

  describe('safeQuery', () => {
    let client;

    beforeEach(() => {
      client = new FakeClient();
      stubs.push(sinon.stub(client, 'query'));
    });

    describe('success', () => {
      it('should return the results', async () => {
        const result: any = {
          rows: [{
            id: 1
          }]
        };
        const res = await safeQuery(client, () => Promise.resolve(result));
        expect(res).to.deep.equal(result);
      });
    });

    describe('failure', () => {
      it('should return empty result', async () => {
        const res = await safeQuery(client, () => Promise.reject());
        expect(res.rows.length).to.equal(0);
      });
    });
  });

  describe('PostgreSQLStorage', () => {
    beforeEach(() => {
      stubs.push(sinon.stub(specs, 'safeTransaction').callsFake((_conn, transactions) => transactions()));
      stubs.push(sinon.stub(specs, 'safeQuery').callsFake((_conn, query) => query()));
    });

    describe('constructor', () => {
      const fakeOptions: any = {};

      beforeEach(() => {
        stubs.push(sinon.stub(PostgreSQLStorage.prototype as any, 'initDb'));
      });

      describe('without a "host"', () => {
        it('should throw an error', () => {
          expect(() => new PostgreSQLStorage(fakeOptions)).to.throw('"host" must be a non-empty string');
        });
      });

      describe('with a "host', () => {
        beforeEach(() => {
          fakeOptions.host = 'postgres://';
        });

        describe('without a "database"', () => {
          it('should throw an error', () => {
            expect(() => new PostgreSQLStorage(fakeOptions)).to.throw('"database" must be a non-empty string');
          });
        });

        describe('with a "database', () => {
          beforeEach(() => {
            fakeOptions.database = 'database';
          });

          it('should call init', () => {
            new PostgreSQLStorage(fakeOptions);
            expect((PostgreSQLStorage.prototype as any).initDb.called).to.equal(true);
          });
        });
      });
    });

    describe('.initDb', () => {
      it('should create a pool', async () => {
        const storage: any = new PostgreSQLStorage(options);
        await storage._dbPromise;
        expect(storage._pool instanceof FakePool).to.equal(true);
      });
    });

    describe('.get', () => {
      let storage: PostgreSQLStorage;
      const id = 'id';
      let stub: sinon.SinonStub;

      beforeEach(() => {
        stubs.push(sinon.stub(specs, 'resultAsJson').callsFake(val => val));

        storage = new PostgreSQLStorage(options);
        const client = clientStub(storage);

        stub = sinon.stub(client, 'query').returns({rows: [{}]});
        stubs.push(stub);
      });

      it('should find by id', () => {
        storage.get(id);
        expect(stub.calledWith(
          `SELECT * FROM ${defaultDocumentsCollection} WHERE _id=$1`,
          [id]
        )).to.equal(true);
      });
    });

    describe('.put', () => {
      let storage: PostgreSQLStorage;
      const id = 'id';
      const data = {test: 1};
      let stub: sinon.SinonStub;

      beforeEach(async () => {
        storage = new PostgreSQLStorage(options);
        await (storage as any)._dbPromise;
        const client = clientStub(storage);

        stub = sinon.stub(client, 'query').returns({});
        stubs.push(stub);
      });

      describe('document exists', () => {
        beforeEach(() => {
          const queue = new FakeQueue();
          queue.push(() => data);
          stubs.push(sinon.stub(storage, 'get').returns(queue));
        });

        it('should update data', () => {
          storage.put(id, data);
          expect(stub.calledWith(
            `UPDATE ${defaultDocumentsCollection} SET value=$2 WHERE _id=$1`,
            [id, JSON.stringify(data)]
          )).to.equal(true);
        });
      });

      describe('document does not exist', () => {
        beforeEach(() => {
          stubs.push(sinon.stub(storage, 'get').returns(new FakeQueue()));
        });

        it('should insert data', () => {
          storage.put(id, data);
          expect(stub.calledWith(
            `INSERT INTO ${defaultDocumentsCollection} (_id, value) VALUES ($1, $2)`,
            [id, JSON.stringify(data)]
          )).to.equal(true);
        });
      });
    });

    describe('.remove', () => {
      let storage: PostgreSQLStorage;
      const id = 'id';
      let stub: sinon.SinonStub;

      beforeEach(() => {
        storage = new PostgreSQLStorage(options);
        const client = clientStub(storage);

        stub = sinon.stub(client, 'query').returns(new FakeQueue());
        stubs.push(stub);
      });

      it('should remove by id', () => {
        storage.remove(id);
        expect(stub.calledWith(
          `DELETE FROM ${defaultDocumentsCollection} WHERE _id=$1`,
          [id]
        )).to.equal(true);
      });
    });

    describe('.getAttachment', () => {
      let storage: PostgreSQLStorage;
      const id = 'id';
      const name = 'name';
      const attachment = 'attachment';
      let stub: sinon.SinonStub;

      beforeEach(() => {
        stubs.push(sinon.stub(specs, 'resultAsJson').callsFake(val => val));
        stubs.push(sinon.stub(jioImport.jIO.util, 'dataURItoBlob').returns(attachment));

        storage = new PostgreSQLStorage(options);
      });

      describe('attachment found', () => {
        beforeEach(() => {
          const client = clientStub(storage);
          stub = sinon.stub(client, 'query').returns({rows: [{}]});
          stubs.push(stub);
        });

        it('should return the attachment', () => {
          const result: any = storage.getAttachment(id, name);
          expect(result.result).to.equal(attachment);
        });
      });

      describe('attachment not found', () => {
        beforeEach(() => {
          const client = clientStub(storage);
          stub = sinon.stub(client, 'query').returns({rows: []});
          stubs.push(stub);
        });

        it('should throw an error', () => {
          expect(() => storage.getAttachment(id, name)).to.throw(`Cannot find attachment: ${id}`);
        });
      });
    });

    describe('.putAttachment', () => {
      let storage: PostgreSQLStorage;
      const id = 'id';
      const name = 'name';
      const data: any = {
        target: {result: {}}
      };
      let stub: sinon.SinonStub;

      beforeEach(async () => {
        storage = new PostgreSQLStorage(options);
        await (storage as any)._dbPromise;
        const client = clientStub(storage);

        stub = sinon.stub(client, 'query').returns([]);
        stubs.push(stub);
        stubs.push(sinon.stub(jioImport.jIO.util, 'readBlobAsDataURL').returns(data));
      });

      it('should insert data', async () => {
        await storage.putAttachment(id, name, data);
        expect(stub.calledWith(
          `INSERT INTO ${defaultAttachmentsCollection} (_id, name, value) VALUES ($1, $2, $3)`,
          [id, name, data.target.result]
        )).to.equal(true);
      });
    });

    describe('.removeAttachment', () => {
      let storage: PostgreSQLStorage;
      const id = 'id';
      const name = 'name';
      let stub: sinon.SinonStub;

      beforeEach(() => {
        storage = new PostgreSQLStorage(options);
        const client = clientStub(storage);

        stub = sinon.stub(client, 'query').returns([]);
        stubs.push(stub);
      });

      it('should remove by id', () => {
        storage.removeAttachment(id, name);
        expect(stub.calledWith(
          `DELETE FROM ${defaultAttachmentsCollection} WHERE _id=$1 AND name=$2`,
          [id, name]
        )).to.equal(true);
      });
    });

    describe('.allAttachments', () => {
      let storage: PostgreSQLStorage;
      const id = 'id';
      const attachments = [{
        name: 'attachment 1'
      }];

      beforeEach(() => {
        storage = new PostgreSQLStorage(options);
        const client = clientStub(storage);

        stubs.push(sinon.stub(client, 'query').returns({rows: attachments}));
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
        const storage = new PostgreSQLStorage(options);
        expect(storage.hasCapacity()).to.equal(true);
      });
    });

    describe('.buildQuery', () => {
      let storage: PostgreSQLStorage;
      let params: IJioQueryOptions;
      const results = [{
        _id: 1,
        title: 'title',
        description: 'description'
      }];
      let stub: sinon.SinonStub;

      beforeEach(() => {
        stubs.push(sinon.stub(specs, 'resultAsJson').callsFake(val => val));

        storage = new PostgreSQLStorage(options);
        const client = clientStub(storage);

        stub = sinon.stub(client, 'query').returns({rows: results});
        stubs.push(stub);

        params = {
          query: ''
        };
      });

      describe('with "query"', () => {
        beforeEach(() => {
          params.query = `'portal_type: "File"'`;
          stubs.push(sinon.stub(specs, 'parseQuery').returns('query'));
        });

        it('should set WHERE clause', () => {
          storage.buildQuery(params);
          expect(stub.calledWith(
            `SELECT * FROM ${defaultDocumentsCollection} WHERE query`
          )).to.equal(true);
        });
      });

      describe('with "limit"', () => {
        beforeEach(() => {
          params.limit = [0, 10];
        });

        it('should set LIMIT and OFFSET clauses', () => {
          storage.buildQuery(params);
          expect(stub.calledWith(
            `SELECT * FROM ${defaultDocumentsCollection} LIMIT ${params.limit![1]} OFFSET ${params.limit![0]}`
          )).to.equal(true);
        });
      });

      describe('with "sort_on"', () => {
        beforeEach(() => {
          params.sort_on = [['title', 'ascending']];
        });

        it('should set ORDER BY clause', () => {
          storage.buildQuery(params);
          expect(stub.calledWith(
            `SELECT * FROM ${defaultDocumentsCollection} ORDER BY value ->> 'title' ASC`
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
