---
id: simple-data-query
title: Query Data Using Keys
---

To query data using the key, we need to use a database context. The `Get()` method in the `DataTxContext`
retrieves a given key from a given database.

```go
type DataTxContext interface {
    .
    ...
	// Get existing key value
	Get(dbName, key string) ([]byte, *types.Metadata, error)
    ...
    .
}
```

:::info prerequisite

For the example shown here to work, we need to have

- Two databases named `db1` and `db2` in the Orion server. If you have not created these two databases,
refer to [creates databases using SDK](../../transactions/gosdk/dbtx#1-creation-of-databases) to create `db1` and `db2`.
- Two users named `alice` and `bob`. If you have not created these users already, refer to [create users using SDK](../../transactions/gosdk/usertx#1-addition-of-users).
- The key `key2` in database `db2`. If you have not stored `key2` in `db2` already, refer to [create states](../../transactions/gosdk/datatx#1-creation-of-new-states-in-database)

Finally, [Create a connection](../../pre-requisite/gosdk#creating-a-connection-to-the-orion-cluster) and
[Open a database session](../../pre-requisite/gosdk#opening-a-database-session).
:::

## Source Code

```go
package main

import (
	"fmt"
)

func main() {
	db, err := createConnection()
    // if err is not nil, print and return

	session, err := openSession(db, "bob")
    // if err is not nil, print and return

	tx, err := session.DataTx()
    // if err is not nil, print and return

	v, m, err := tx.Get("db2", "key1")
    // if err is not nil, print and return

	if v == nil {
		fmt.Println("the database db2 does not have key1")
		return
	}

	fmt.Println("value         :", string(v))
	fmt.Println("version       :", m.Version)
	fmt.Println("access control:", m.AccessControl)

	err = tx.Abort()
    // if err is not nil, print and return
}
```

## Source Code Commentary

For the sake of simplicity, not all `errors` are handled in this code. Furthermore, the implementation of `createConnection()` and `openSession()`
can be found [here](../../pre-requisite/gosdk).

Calling `session.DataTx()` starts a new data transaction and returns the data transaction context. Note that we use this context only for query purposes and not for performing any operations on the data.

To fetch the key `key1` in database `db2`, we call `tx.Get("db2", "key1")`. It returns

  1. value
  2. metadata
  3. error

The value is `[]byte`. The metadata holds the version and access control as shown below:

```go
type Metadata struct {
	Version              *Version
	AccessControl        *AccessControl
}
```
The `Version` denotes the version of the key fetched. It is denoted by a block number and a transaction number that last modified the key.

```go
type Version struct {
	BlockNum             uint64
	TxNum                uint64
}
```

The ACL holds

 1. `ReadUsers`: a list of users who can only read the key
 2. `ReadWriteUsers`: a list of users who can read and write the key.
 3. `SignPolicyForWrite`: denotes whether signature of all users in the ReadWriteUsers is needed to perform writes to the key.

```go
type AccessControl struct {
	ReadUsers            map[string]bool
	ReadWriteUsers       map[string]bool
	SignPolicyForWrite   AccessControlWritePolicy
}

type AccessControlWritePolicy int32

const (
	AccessControl_ANY AccessControlWritePolicy = 0
	AccessControl_ALL AccessControlWritePolicy = 1
)
```

Finally, we can abort the transaction by calling `tx.Abort()`.
