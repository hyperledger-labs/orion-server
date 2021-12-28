---
id: db
title: Check the Existence of a Database
---

To check the existence of a database, we need to use a database context. The method `Exists()`
in the `DBsTxContext` checks whether a given database already exists in the Orion cluster.

```go
type DBsTxContext interface {
    ...
    ...
	// Exists checks whenever database is already created
    // highlight-next-line
	Exists(dbName string) (bool, error)
}
```


## Source Code
The following code checks the existence of databases `db1` and `db2`.

```go
package main

import (
	"fmt"
)

func main() {
	db, err := createConnection()
    // if err is not nil, print and return

	session, err := openSession(db, "admin")
    // if err is not nil, print and return

	dbtx, err := session.DBsTx()
    // if err is not nil, print and return

	exist, err := dbtx.Exists("db1")
    // if err is not nil, print and return
	fmt.Println("Does database exist?", exist)

	exist, err = dbtx.Exists("db2")
    // if err is not nil, print and return
	fmt.Println("Does database exist?", exist)

	err = dbtx.Abort()
    // if err is not nil, print and return
}
```

## Source Code Commentary
For the sake of simplicity, not all errors are handled in this code. Furthermore, the implementation of `createConnection()` and `openSession()` can be found [here](../../pre-requisite/gosdk).

Calling `session.DBsTx()` starts a new database administration transaction and returns the database administration transaction context.
Note that, in this example, we use this context only for the query purpose and not to perform any database administrative operation.

Calling `dbtx.Exists("db1")` and `dbtx.Exists("db2")` check whether the given databases exist or not. It returns a boolean flag where `true` denotes that the
database exists, while `false` denotes that the database does not exist.

As we are executing a read-only query, it is not necessary to commit the transaction and hence, we can abort it by calling `dbtx.Abort()`. If we need to record
all the read-only transactions history into the centralized ledger, then it is advisable to do `dbtx.Commit()` rather than `dbtx.Abort()`.
