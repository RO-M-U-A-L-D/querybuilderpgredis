# querybuilderpgredis

A combined **PostgreSQL + Redis** query builder and caching library for **Node.js**.
Provides a simple, chainable API for building and executing SQL queries, with optional Redis caching.
Framework-agnostic, but integrates seamlessly with **Total.js** if present.

---

## Features

* Single initialization for PostgreSQL and Redis
* Chainable query builder API via `DATA`
* Full CRUD support (`find`, `list`, `insert`, `update`, `remove`, `check`, `count`, `scalar`, `query`)
* Safe parameter binding to prevent SQL injection
* Flexible filter types: `where`, `in`, `or`, `between`, `search`, `permit`, etc.
* Automatic schema support from connection string
* Redis-based query result caching
* Works with or without Total.js

---

## Installation

```bash
npm install querybuilderpgredis
```

---

## Initialization

```javascript
const { init, DATA } = require('querybuilderpgredis');

init(
  'default',                                          // Connection name
  'postgresql://user:password@localhost:5432/dbname', // PostgreSQL connection string
  10,                                                 // Pool size (optional)
  (err, query) => {                                   // Error handler (optional)
    console.error('PostgreSQL Error:', err);
    console.error('Query:', query);
  },
  {
    host: 'localhost',                                // Redis host
    port: 6379,                                       // Redis port
    password: 'your-redis-password'                   // Redis password (optional)
  }
);
```

---

## Usage

### Find a Record

```javascript
DATA.find('tbl_user')
  .where('id', 1234)
  .callback(console.log);
```

### List with Filters

```javascript
DATA.list('orders')
  .where('status', 'paid')
  .sort('created_desc')
  .take(20)
  .skip(0)
  .callback((err, res) => {
    console.log(res.items, res.count);
  });
```

### Insert

```javascript
DATA.insert('products', { name: 'Drone X1', price: 1999 })
  .returning('id')
  .callback((err, id) => console.log('Inserted ID:', id));
```

### Update

```javascript
DATA.update('products', { price: 1899 })
  .where('id', 5)
  .callback((err, affected) => console.log('Rows updated:', affected));
```

### Remove

```javascript
DATA.remove('products')
  .where('id', 10)
  .callback((err, affected) => console.log('Rows deleted:', affected));
```

### Scalar Queries

```javascript
DATA.scalar('sales', 'sum', 'amount')
  .callback((err, total) => {
    console.log('Total sales:', total);
  });
```

---

## Redis Caching Example

```javascript
DATA.cache('users:active', 60, (next) => {
  DATA.find('users')
    .where('active', true)
    .callback(next);
}, (err, data) => {
  console.log('Cached active users:', data);
});
```

---

## Connection String Attributes

Example:

```
postgresql://user:password@localhost:5432/database?schema=parking&pooling=2
```

* `schema` — Default database schema (string)
* `pooling` — Default connection pool size (number, overrides init pooling parameter)

---

## License

MIT License
