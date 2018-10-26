# ClearRoad API Storage MariaDB / MySQL

Note: 5 databases will be created to store the data, all prefixed with the `database` value see below.
Make sure you use credentials that have the rights to create databases.

## Install

```sh
npm install @clearroad/api-storage-mariadb
```

## Usage

Using with es6 / TypeScript:

```javascript
import { ClearRoad } from '@clearroad/api';
import storage from '@clearroad/api-storage-mariadb';

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

Using with Node.js:

```javascript
const ClearRoad = require('@clearroad/api').ClearRoad;
const storage = require('@clearroad/api-storage-mariadb').default;

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
