---
id: cluster-config
title: Query the Cluster Configuration
---

The cluster configuration includes the configuration of
   1. node,
   2. admin, and
   3. consensus configuration (used for replication).

When the bdb server bootup for the first time, it reads nodes, admins, and consensus configuration present in the configuration file `config.yml` and creates a genesis block.

> As a pre-requisite, we need to first [create a connection](../../pre-requisite/gosdk#creating-a-connection-to-the-orion-cluster) and
[open a database session](../../pre-requisite/gosdk#opening-a-database-session).

Once a database session is created, we can call `session.ConfigTx()` to get the method `GetClusterConfig()` that fetches the cluster configuration.

```go
type ConfigTxContext interface {
	// GetClusterConfig returns the current cluster config.
	// A ConfigTxContext only gets the current config once, subsequent calls return a cached value.
	// The value returned is a deep clone of the cached value and can be manipulated.
    // highlight-next-line
	GetClusterConfig() (*types.ClusterConfig, error)
}
```

## Source Code
```go
package main

import (
	"encoding/json"
	"fmt"
)

func main() {
	db, err := createConnection()
    // if err is not nil, print and return

	session, err := openSession(db, "admin")
    // if err is not nil, print and return

	tx, err := session.ConfigTx()
    // if err is not nil, print and return

	v, err := tx.GetClusterConfig()
    // if err is not nil, print and return

	config, err := json.Marshal(v)
    // if err is not nil, print and return

	fmt.Println(string(config))

	err = tx.Abort()
    // if err is not nil, print and return
}
```
### Source Code Commentary
