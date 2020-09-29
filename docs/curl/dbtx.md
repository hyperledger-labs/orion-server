# Database Administration Transaction

To create or delete a database, we needs to submit a database administration transaction. 

When the database node boots up for the first time, it would create a default database called `bdb` and 3 system databases named `_users`, `_dbs`, `_config`. The `bdb` database can be used to submit data transactions whereas system databases are internal to the `bdb` server. The user cannot directly read or write to the system databases.

To create or delete a database, we need to issue a  `POST /db {txPayload}` wher `txPayload` contains information about the database to be created and deleted. 

## Creation of a Database

We can create a new database to store data/states by issuing a database administration transaction. The following curl command can be used to create two new databases named `db1` and `db2`: 

```json
 curl \
   -H "Content-Type: application/json" \
   -X POST http://127.0.0.1:6001/db \
   --data '{
    "payload": {
		"userID": "admin",
		"txID": "1b6d6414-9b58-45d0-9723-1f31712add71",
		"createDBs": [
			"db1",
			"db2"
		]
	},
  "signature": "aGVsbG8="
}'
```

Once the above transaction gets validated and committed, we can check the existance of `db1` and `db2`.

### Checking the existance of db1
```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: admin" \
     -H "Signature: abcd" \
     -X GET http://127.0.0.1:6001/db/db1 | jq .
```
**Output:**
```json
{
  "payload": {
    "header": {
      "nodeID": "bdb-node-1"
    },
    "exist": true
  }
}
```

### Checking the existance of db2

```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: admin" \
     -H "Signature: abcd" \
     -X GET http://127.0.0.1:6001/db/db2 | jq .
```
**Output:**
```json
{
  "payload": {
    "header": {
      "nodeID": "bdb-node-1"
    },
    "exist": true
  }
}
```

## Deletion of a Database

We can delete an existing database by issuing a database administration transaction. The following curl command can be used to delete two existing databases named `db1` and `db2`: 

```json
 curl \
   -H "Content-Type: application/json" \
   -X POST http://127.0.0.1:6001/db \
   --data '{
    "payload": {
		"userID": "admin",
		"txID": "1b6d6414-9b58-45d0-9723-1f31712add72",
		"deleteDBs": [
			"db1",
			"db2"
		]
	},
  "signature": "aGVsbG8="
}'
```

Once the above transaction gets validated and committed, we can check the existance of `db1` and `db2`.

### Checking the existance of db1
```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: admin" \
     -H "Signature: abcd" \
     -X GET http://127.0.0.1:6001/db/db1 | jq .
```
**Output:**
```json
{
  "payload": {
    "header": {
      "nodeID": "bdb-node-1"
    },
  }
}
```

### Checking the existance of db2

```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: admin" \
     -H "Signature: abcd" \
     -X GET http://127.0.0.1:6001/db/db2 | jq .
```
**Output:**
```json
{
  "payload": {
    "header": {
      "nodeID": "bdb-node-1"
    },
  }
}
```

The default values are omitted and hence, the `exist = false` is not printed. 

## Creation and Deletion of Databases in a Single Transaction

Within a single transaction, we can create and delete as many numnber of database we want by includinb both the `createDBs` and `deleteDBs` list. 

```json
 curl \
   -H "Content-Type: application/json" \
   -X POST http://127.0.0.1:6001/db \
   --data '{
    "payload": {
		"userID": "admin",
		"txID": "1b6d6414-9b58-45d0-9723-1f31712add74",
		"createDBs": [
			"db3",
      "db4"
		],
    "deleteDBs": [
      "db1",
      "db2"
    ]
	},
  "signature": "aGVsbG8="
}'
```

The above transaction will be valid if `db3` and `db4` do not exist and `db1` and `db2` exist in the cluster.

## Invalid Database Administration Transaction

We cover the incorrect usage of administration transaction that can lead to invalidation of the submitted database administration transaction.

### Database to be created already exist

Let's create a new database `db3`
```json
 curl \
   -H "Content-Type: application/json" \
   -X POST http://127.0.0.1:6001/db \
   --data '{
    "payload": {
		"userID": "admin",
		"txID": "1b6d6414-9b58-45d0-9723-1f31712add73",
		"createDBs": [
			"db3"
		]
	},
  "signature": "aGVsbG8="
}'
```

Let's try to create `db3` again
```json
 curl \
   -H "Content-Type: application/json" \
   -X POST http://127.0.0.1:6001/db \
   --data '{
    "payload": {
		"userID": "admin",
		"txID": "1b6d6414-9b58-45d0-9723-1f31712add74",
		"createDBs": [
			"db3"
		]
	},
  "signature": "aGVsbG8="
}'
```
The above transaction would be invalidated with the following reason: `the database [db3] already exists in the cluster and hence, it cannot be created`

### Database to be deleted does not exist

Let's try to delete `db4` which does not exist in the cluster
```json
 curl \
   -H "Content-Type: application/json" \
   -X POST http://127.0.0.1:6001/db \
   --data '{
    "payload": {
		"userID": "admin",
		"txID": "1b6d6414-9b58-45d0-9723-1f31712add75",
		"deleteDBs": [
			"db4"
		]
	},
  "signature": "aGVsbG8="
}'
```
The above transaction would be invalidated with the following reason: `the database [db4] does not exist in the cluster and hence, it cannot be deleted`

### Database to be deleted is a system database

Let's try to delete a system database `_config_` 
```json
 curl \
   -H "Content-Type: application/json" \
   -X POST http://127.0.0.1:6001/db \
   --data '{
    "payload": {
		"userID": "admin",
		"txID": "1b6d6414-9b58-45d0-9723-1f31712add76",
		"deleteDBs": [
			"_config"
		]
	},
  "signature": "aGVsbG8="
}'
```
The above transaction would be invalidated with the following reason: `the database [_config] is a system database which cannot be deleted`