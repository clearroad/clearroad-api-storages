# ClearRoad API Storage Microsoft SQL Server

Note: 5 databases will be created to store the data, all prefixed with the `database` value (see below).
Make sure you use credentials that have the rights to create databases.

## Install

```sh
npm install @clearroad/api-storage-mssql
```

## Usage

1. Import the library:
> Using with es6 / TypeScript
```javascript
import { ClearRoad } from '@clearroad/api';
import storage from '@clearroad/api-storage-mssql';
```

> Using with require
```javascript
const ClearRoad = require('@clearroad/api').ClearRoad;
const storage = require('@clearroad/api-storage-mssql').default;
```

2. Create a `ClearRoad` instance:

```javascript
const options = {
  localStorage: {
    type: storage,
    server: 'host.com',
    user: 'user',
    password: 'passowrd',
    database: 'MyDatabase'
  },
  useQueryStorage: true // we cannot query directly with the storage, so wrap in a query storage
};
const cr = new ClearRoad('url', 'accessToken', options);
```
