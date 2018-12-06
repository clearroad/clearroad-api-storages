# ClearRoad API Storage MongoDB

Note: 5 databases will be created to store the data, all prefixed with the `database` value (see below).
Make sure you use credentials that have the rights to create databases.

## Install

```sh
npm install @clearroad/api-storage-mongodb
```

## Usage

1. Import the library:
> Using with es6 / TypeScript
```javascript
import { ClearRoad } from '@clearroad/api';
import storage from '@clearroad/api-storage-mongodb';
```

> Using with require
```javascript
const ClearRoad = require('@clearroad/api').ClearRoad;
const storage = require('@clearroad/api-storage-mongodb').default;
```

2. Create a `ClearRoad` instance:

```javascript
const options = {
  localStorage: {
    type: storage,
    url: 'mongodb://user:password@host.com:27017',
    database: 'MyDatabase'
  }
};
const cr = new ClearRoad('url', 'accessToken', options);
```

### Options

Property | Type | Description | Required
-------- | ---- | ----------- | --------
localStorage.type | `string` | Connector type. Use `mongodb` | Yes
localStorage.url | `string` | Database url connection string | Yes
localStorage.database | `string` | Database name | Yes
localStorage.clientOptions | `MongoClientOptions` (see [connect options](https://mongodb.github.io/node-mongodb-native/api-generated/mongoclient.html#connect)) | Additional configuration for connection | No
localStorage.documentsCollectionName | `string` | Database collection name to storage all documents. Default is `Documents` | No
localStorage.attachmentsCollectionName | `string` | Database table name to storage all attachments. Default is `Attachments` | No
localStorage.timestamps | `boolean` | Add `createdAt` and `updateAt` fields on each row. Default is `true` | No
