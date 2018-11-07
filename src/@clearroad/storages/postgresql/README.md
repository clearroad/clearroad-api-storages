# ClearRoad API Storage PostgreSQL

Note: 5 databases will be created to store the data, all prefixed with the `database` value see below.
Make sure you use credentials that have the rights to create databases.

## Install

```sh
npm install @clearroad/api-storage-postgresql
```

## Usage

1. Import the library:
> Using with es6 / TypeScript
```javascript
import { ClearRoad } from '@clearroad/api';
import storage from '@clearroad/api-storage-postgresql';
```

> Using with require
```javascript
const ClearRoad = require('@clearroad/api').ClearRoad;
const storage = require('@clearroad/api-storage-postgresql').default;
```

2. Create a `ClearRoad` instance:

```javascript
const options = {
  localStorage: {
    type: storage,
    host: 'host.com',
    user: 'user',
    password: 'passowrd',
    database: 'MyDatabase'
  }
};
const cr = new ClearRoad('url', 'accessToken', options);
```