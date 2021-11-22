---
id: simple-data-query
title: Query Data Using Keys
---


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

	fmt.Println("access control:", m.AccessControl)
	fmt.Println("version       :", m.Version)
	fmt.Println("value         :", string(v))

	err = tx.Abort()
    // if err is not nil, print and return
}
```

