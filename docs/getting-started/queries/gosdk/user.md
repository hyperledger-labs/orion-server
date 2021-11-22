---
id: user
title: Query an User Information
---

### Source Code

```go 
package main

import (
	"fmt"
	"strconv"
)

func main() {
	bcdb, err := createConnection()
    // if err is not nil, print and return

	session, err := openSession(bcdb)
    // if err is not nil, print and return

	tx, err := session.UsersTx()
    // if err is not nil, print and return

	alice, err := tx.GetUser("alice")
    // if err is not nil, print and return

}
```

### (3.2) Source Code Commentry
For simplicity, not all errors are handled in this code. Further, the implementation of `createConnection()` and `openSession()` can be found
[here](../../pre-requisite/gosdk).

