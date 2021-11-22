---
id: db
title: Check the Existence of a Database
---

```go
type DBsTxContext interface {
    ...
    ...
	// Exists checks whenever database is already created
    // highlight-next-line
	Exists(dbName string) (bool, error)
}
```


### Source Code
```go
package main

import (
	"fmt"
)

func main() {
	db, err := createConnection()
    // if err is not nil, print and return

	session, err := openSession(db)
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

### Source Code Commentry
For simplicity, not all errors are handled in this code. Further, the implementation of `createConnection()` and `openSession()` can be found [here](../../pre-requisite/gosdk).

The `session.DBsTx()` starts a new database administration transaction and returns the database administration transaction context. We can then perform all
administrative activities using the transaction context.

The `dbtx.Exists("db1")` and `dbtx.Exists("db2")` check whether the given databases exist or not. It returns a boolean flag where `true` denotes that the
database exist while `false` denotes that the database does not exist.

As we are executing a read-only query, it is not necessary to commit the transaction and hence, we are aborting it by calling `dbtx.Abort()`. If we need to record
all read-only transactions history into the centralized ledger, then it is advisable to do `dbtx.Commit()` rather than `dbtx.Abort()`.


