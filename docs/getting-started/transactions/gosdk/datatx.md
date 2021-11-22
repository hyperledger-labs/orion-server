---
id: datatx
title: Data Transaction
---
# Data Transaction

We can create, update and delete states maintained by the Orion cluster using the data transaction.

Using a data transaction, we can do the following:

  1. [Creation of new states](#2-creation-of-new-states)
  2. [Updation of an existing states](#3-updation-of-an-existing-state)
  3. [Deletion of an existing states](#4-deletion-of-an-existing-state)
  3. [Creation, Updation, Deletion of states within a single transaction](#5-creation-updation-deletion-of-states-within-a-single-transaction)
  4. [Operations on multiple databases](#6-operations-on-multiple-databases-in-a-single-transaction)
  5. [Multi-signatures transaction](#7-multi-signatures-transaction)

> As a pre-requisite, we need to first [create a connection](./../../pre-requisite/gosdk#creating-a-connection-to-the-orion-cluster) and [open a database session](./../../pre-requisite/gosdk#opening-a-database-session).

> In addition to this example, you can download and use the data transaction example from the go-sdk examples folder: [orion-sdk-go/examples/api/simple_tx/simple_tx.go](https://github.com/hyperledger-labs/orion-sdk-go/blob/main/examples/api/simple_tx/simple_tx.go) 

Once a [database session](./../../pre-requisite/gosdk#opening-a-database-session) is created, a call to `session.DataTx()` will create new data transaction context and thus will start a new data
transaction. The data transaction context provides following methods to calls:

```go 
type DataTxContext interface {
	// Put new value to key
	Put(dbName string, key string, value []byte, acl *types.AccessControl) error
	// Get existing key value
	Get(dbName, key string) ([]byte, *types.Metadata, error)
	// Delete value for key
	Delete(dbName, key string) error
	// AssertRead insert a key-version to the transaction assert map
	AssertRead(dbName string, key string, version *types.Version) error
	// AddMustSignUser adds userID to the multi-sign data transaction's
	// MustSignUserIDs set. All users in the MustSignUserIDs set must co-sign
	// the transaction for it to be valid. Note that, in addition, when a
	// transaction modifies keys which have multiple users in the write ACL,
	// or when a transaction modifies keys where each key has different user
	// in the write ACL, the signature of additional users may be required.
	// AddMustSignUser can be used to add users whose signatures is required,
	// on top of those mandates by the ACLs of the keys in the write-set of
	// the transaction. The userID of the initiating client is always in
	// the MustSignUserIDs set."
	AddMustSignUser(userID string)
	// SignConstructedTxEnvelopeAndCloseTx returns a signed transaction envelope and
	// also closes the transaction context. When a transaction requires
	// signatures from multiple users, an initiating user prepares the
	// transaction and calls SignConstructedTxEnvelopeAndCloseTx in order to
	// sign it and construct the envelope. The envelope must then be
	// circulated among all the users that need to co-sign it."
	SignConstructedTxEnvelopeAndCloseTx() (proto.Message, error)
	// Commit submits transaction to the server, can be sync or async.
	// Sync option returns tx id and tx receipt and
	// in case of error, commitTimeout error is one of possible errors to return.
	// Async returns tx id, always nil as tx receipt or error
	Commit(sync bool) (string, *types.TxReceipt, error)
	// Abort cancel submission and abandon all changes
	// within given transaction context
	Abort() error
	// CommittedTxEnvelope returns transaction envelope, can be called only after Commit(), otherwise will return nil
	CommittedTxEnvelope() (proto.Message, error)
}
```

## (1) Prerequisite

For all examples shown here to work, we need to have two databases named `db1` and `db2` in the orion-server. If you have not created these to databases,
refer to [creates databases using SDK](./dbtx#creation-of-databases) to create `db1` and `db2`. Then, we need to have two users named `alice` and `bob`. 
If you have not created these users already, refer to [create users using SDK](./usertx##2-addition-of-users)

## (2) Creation of new states in database

### (2.1) Create a state with key `key1`

Let's store a new state `key1` with the value `{"name":"abc","age":31,"graduated":true}`.

```go
package main

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/hyperledger-labs/orion-server/pkg/types"
)

type person struct {
	Name      string `json:"name"`
	Age       int64  `json:"age"`
	Graduated bool   `json:"graduated"`
}

func main() {
	db, err := createConnection()
    // if err is not nil, print and return

	session, err := openSession(db, "alice")
    // if err is not nil, print and return

	tx, err := session.DataTx()
    // if err is not nil, print and return

	p := &person{
		Name:      "abc",
		Age:       31,
		Graduated: true,
	}

	jVal, err := json.Marshal(p)
    // if err is not nil, print and return

	acl := &types.AccessControl{
		ReadUsers: map[string]bool{
			"alice": true,
			"bob":   true,
		},
		ReadWriteUsers: map[string]bool{
			"alice": true,
		},
	}
	err = tx.Put("db2", "key1", jVal, acl)
    // if err is not nil, print and return

	txID, receipt, err := tx.Commit(true)
   // if err is not nil, print and return

	fmt.Println("transaction with txID " + txID + " got committed in the block " + strconv.Itoa(int(receipt.GetHeader().GetBaseHeader().GetNumber())))
}
```


### (2.2) Checking the existence of `key1`

Let's query the node to see whether `key1` exists. The query can be submitted by either `alice` or `bob` as both have
the read permission to this key. No one else can read `key1` including the admin user of the node. In this example,
we use `bob` to query the key.

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

## (3) Update of an existing state

### (3.1) Update the key `key1`

Let's update the value of `key1`. In order to do that, we need to execute the following three steps:

1. Fetch `key1` from the server.
2. Construct the updated value
    - update the `age` from `31`. The new value would be `{"name":"abc","age":32,"graduated":true}`
3. Commit the data transaction

```go
package main

import (
	"encoding/json"
	"fmt"
	"strconv"
)

type person struct {
	Name      string `json:"name"`
	Age       int64  `json:"age"`
	Graduated bool   `json:"graduated"`
}

func main() {
	db, err := createConnection()
    // if err is not nil, print and return

	session, err := openSession(db, "alice")
    // if err is not nil, print and return

	tx, err := session.DataTx()
    // if err is not nil, print and return

	v, m, err := tx.Get("db2", "key1")
    // if err is not nil, print and return

	p := &person{}
	err = json.Unmarshal(v, p)
    // if err is not nil, print and return

	p.Age = 32

	jVal, err := json.Marshal(p)
    // if err is not nil, print and return

	err = tx.Put("db2", "key1", jVal, m.AccessControl)
    // if err is not nil, print and return

	txID, receipt, err := tx.Commit(true)
    // if err is not nil, print and return

	fmt.Println("transaction with txID " + txID + " got committed in the block " + strconv.Itoa(int(receipt.GetHeader().GetBaseHeader().GetNumber())))
}
```

### (3.2) Checking the existence of the updated key `key1`

Let's query the node to see whether `key1` has been updated. In this example, we use `alice` to query the key.

```go
package main

import (
	"encoding/json"
	"fmt"
)

type person struct {
	Name      string `json:"name"`
	Age       int64  `json:"age"`
	Graduated bool   `json:"graduated"`
}

func main() {
	db, err := createConnection()
    // if err is not nil, print and return

	session, err := openSession(db, "alice")
    // if err is not nil, print and return

	tx, err := session.DataTx()
    // if err is not nil, print and return

	v, _, err := tx.Get("db2", "key1")
    // if err is not nil, print and return

	p := &person{}
	err = json.Unmarshal(v, p)
    // if err is not nil, print and return

	if p.Age != 32 {
		fmt.Println("update was not successful")
		return
	}

	fmt.Println("age:", p.Age)

	err = tx.Abort()
    // if err is not nil, print and return
}
```

## (4) Deletion of an existing state

### (4.1) Delete the key `key1`

Let's delete the key `key1`. 

```go
package main

import (
	"fmt"
	"strconv"
)

func main() {
	db, err := createConnection()
    // if err is not nil, print and return

	session, err := openSession(db, "alice")
    // if err is not nil, print and return

	tx, err := session.DataTx()
    // if err is not nil, print and return

	err = tx.Delete("db2", "key1")
    // if err is not nil, print and return

	txID, receipt, err := tx.Commit(true)
    // if err is not nil, print and return

	fmt.Println("transaction with txID " + txID + " got committed in the block " + strconv.Itoa(int(receipt.GetHeader().GetBaseHeader().GetNumber())))
}
```

### (4.2) Checking the non-existence of the deleted key `key1`

```go
package main

import (
	"fmt"
)

func main() {
	db, err := createConnection()
    // if err is not nil, print and return

	session, err := openSession(db, "alice")
    // if err is not nil, print and return

	tx, err := session.DataTx()
    // if err is not nil, print and return

	v, m, err := tx.Get("db2", "key1")
    // if err is not nil, print and return

	if v != nil || m != nil {
		fmt.Println("delete was not successful")
		return
	}

	err = tx.Abort()
	if err != nil {
		fmt.Println(err.Error())
	}
}
```

## (5) Creating, Updating, Deleting states within a single transaction

Let's create `key1` and `key2` so that in the next transaction we can do all three operations.

### (5.1) Create `key1` and `key2`

```go
package main

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/hyperledger-labs/orion-server/pkg/types"
)

type person struct {
	Name      string `json:"name"`
	Age       int64  `json:"age"`
	Graduated bool   `json:"graduated"`
}

func main() {
	db, err := createConnection()
    // if err is not nil, print and return

	session, err := openSession(db, "alice")
    // if err is not nil, print and return

	tx, err := session.DataTx()
    // if err is not nil, print and return

	p1 := &person{
		Name:      "abc",
		Age:       31,
		Graduated: true,
	}

	jVal, err := json.Marshal(p1)
    // if err is not nil, print and return

	acl := &types.AccessControl{
		ReadUsers: map[string]bool{
			"alice": true,
			"bob":   true,
		},
		ReadWriteUsers: map[string]bool{
			"alice": true,
		},
	}
	err = tx.Put("db2", "key1", jVal, acl)
    // if err is not nil, print and return

	p2 := &person{
		Name:      "def",
		Age:       20,
		Graduated: false,
	}

	jVal, err = json.Marshal(p2)
    // if err is not nil, print and return

	err = tx.Put("db2", "key2", jVal, acl)
    // if err is not nil, print and return

	txID, receipt, err := tx.Commit(true)
    // if err is not nil, print and return

	fmt.Println("transaction with txID " + txID + " got committed in the block " + strconv.Itoa(int(receipt.GetHeader().GetBaseHeader().GetNumber())))
}
```

### (5.2) Create `key3`, update `key2`, and delete `key1`

Now, we have required data in the server, we can execute creation, updation, and deletion within a single transaction.

```go
package main

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

type person struct {
	Name      string `json:"name"`
	Age       int64  `json:"age"`
	Graduated bool   `json:"graduated"`
}

func main() {
	db, err := createConnection()
    // if err is not nil, print and return

	session, err := openSession(db, "alice")
    // if err is not nil, print and return

	tx, err := session.DataTx()
    // if err is not nil, print and return

	err = createKey3(tx)
    // if err is not nil, print and return

	err = updateKey2(tx)
    // if err is not nil, print and return

	err = deleteKey1(tx)
    // if err is not nil, print and return

	txID, receipt, err := tx.Commit(true)
    // if err is not nil, print and return

	fmt.Println("transaction with txID " + txID + " got committed in the block " + strconv.Itoa(int(receipt.GetHeader().GetBaseHeader().GetNumber())))
}

func createKey3(tx bcdb.DataTxContext) error {
	p1 := &person{
		Name:      "ghi",
		Age:       24,
		Graduated: true,
	}

	jVal, err := json.Marshal(p1)
	if err != nil {
		return err
	}

	acl := &types.AccessControl{
		ReadWriteUsers: map[string]bool{
			"alice": true,
		},
	}

	return tx.Put("db2", "key3", jVal, acl)
}

func updateKey2(tx bcdb.DataTxContext) error {
	v, m, err := tx.Get("db2", "key2")
	if err != nil {
		return err
	}

	p := &person{}
	err = json.Unmarshal(v, p)
	if err != nil {
		return err
	}
	p.Age = 24
	p.Graduated = true

	jVal, err := json.Marshal(p)
	if err != nil {
		return err
	}

	return tx.Put("db2", "key2", jVal, m.AccessControl)
}

func deleteKey1(tx bcdb.DataTxContext) error {
	return tx.Delete("db2", "key1")
}
```

### (5.3) Check the non-existence of `key1` and existence of updated `key2` and newly created `key3`

```go
package main

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
)

type person struct {
	Name      string `json:"name"`
	Age       int64  `json:"age"`
	Graduated bool   `json:"graduated"`
}

func main() {
	db, err := createConnection()
    // if err is not nil, print and return

	session, err := openSession(db, "alice")
    // if err is not nil, print and return

	tx, err := session.DataTx()
    // if err is not nil, print and return

	err = checkNonExistenceOfKey1(tx)
    // if err is not nil, print and return

	err = checkUpdateKey2(tx)
    // if err is not nil, print and return

	err = checkExistenceOfKey3(tx)
    // if err is not nil, print and return

	err = tx.Abort()
    // if err is not nil, print and return

func checkNonExistenceOfKey1(tx bcdb.DataTxContext) error {
	v, m, err := tx.Get("db2", "key1")
	if err != nil {
		return err
	}

	if v != nil || m != nil {
		return errors.New("key1 was not deleted")
	}

	return nil
}

func checkUpdateKey2(tx bcdb.DataTxContext) error {
	v, _, err := tx.Get("db2", "key2")
	if err != nil {
		return err
	}

	p := &person{}
	err = json.Unmarshal(v, p)
	if err != nil {
		return err
	}

	if p.Age != 24 || p.Graduated == false {
		return errors.New("key2 was not updated")
	}

	return nil
}

func checkExistenceOfKey3(tx bcdb.DataTxContext) error {
	v, m, err := tx.Get("db2", "key3")
	if err != nil {
		return err
	}

	if v == nil || m == nil {
		return errors.New("key2 was not created")
	}

	return nil
}
```

## (6) Operations on Multiple Databases in a Single Transaction

A data transaction can access or modify more than one user database in a transaction. In the below example,
we perform operations on two databases `db1` and `db2` within a single transaction.

### (6.1) Update `alice`'s privileges

As both `alice` and `bob` have only read permission on the database `db1`, first, we update the privilege of `alice`
to have read-write permission on `db1` as shown below:

```go
package main

import (
	"fmt"
	"strconv"

	"github.com/hyperledger-labs/orion-server/pkg/types"
)

func main() {
	bcdb, err := createConnection()
    // if err is not nil, print and return

	session, err := openSession(bcdb, "admin")
    // if err is not nil, print and return

	tx, err := session.UsersTx()
    // if err is not nil, print and return

	alice, err := tx.GetUser("alice")
    // if err is not nil, print and return

	alice.Privilege.DbPermission = map[string]types.Privilege_Access{
		"db1": 1,
		"db2": 1,
	}
	err = tx.PutUser(alice, nil)
    // if err is not nil, print and return

	txID, receipt, err := tx.Commit(true)
	// if err is not nil, print and return
	
	fmt.Println("transaction with txID " + txID + " got committed in the block " + strconv.Itoa(int(receipt.GetHeader().GetBaseHeader().GetNumber())))
}
```

In order to construct the above transaction payload, first, we need to fetch the `alice` user to capture the certificate and current
privileges. For brevity, the fetch operation is not shown here. Next, we need to construct the above payload. In the `user_reads` field,
we have the set the version which would be used during multi-version concurrency control. The `user_writes` field holds the updated
privileges where the read-only permission on `db1` denoted by `"db1":0` has been changed to `"db1":1`.

Now that `alice` has read-write permission on both `db1` and `db2`, we can perform the multi-database operation.

### (6.2) Create `key4` in `db1` and `key4` in `db2`

Let's create
  1. the key `key4` with value `{"name":"abc","age":31,"graduated":true}` in `db1`
  2. the key `key4` with value `{"name":"def","age":20,"graduated":false}` in `db2`

```go
package main

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

type person struct {
	Name      string `json:"name"`
	Age       int64  `json:"age"`
	Graduated bool   `json:"graduated"`
}

func main() {
	db, err := createConnection()
    // if err is not nil, print and return

	session, err := openSession(db, "alice")
    // if err is not nil, print and return

	tx, err := session.DataTx()
    // if err is not nil, print and return

	err = createKey4InDB1(tx)
    // if err is not nil, print and return

	err = createKey4InDB2(tx)
    // if err is not nil, print and return

	txID, receipt, err := tx.Commit(true)
    // if err is not nil, print and return

	fmt.Println("transaction with txID " + txID + " got committed in the block " + strconv.Itoa(int(receipt.GetHeader().GetBaseHeader().GetNumber())))
}

func createKey4InDB1(tx bcdb.DataTxContext) error {
	p1 := &person{
		Name:      "abc",
		Age:       31,
		Graduated: true,
	}

	jVal, err := json.Marshal(p1)
	if err != nil {
		return err
	}

	acl := &types.AccessControl{
		ReadUsers: map[string]bool{
			"alice": true,
			"bob":   true,
		},
		ReadWriteUsers: map[string]bool{
			"alice": true,
		},
	}

	return tx.Put("db1", "key4", jVal, acl)
}

func createKey4InDB2(tx bcdb.DataTxContext) error {
	p2 := &person{
		Name:      "def",
		Age:       20,
		Graduated: false,
	}

	jVal, err := json.Marshal(p2)
	if err != nil {
		return err
	}

	return tx.Put("db2", "key4", jVal, nil)
}
```

### (6.3) Check the existence of `key4` in `db1` and `key4` in `db2`

Let's fetch `key4` from `db1` and `key4` from `db2` to ensure that these keys have been created successfully.

```go
package main

import (
	"fmt"
)

func main() {
	db, err := createConnection()
    // if err is not nil, print and return

	session, err := openSession(db, "alice")
    // if err is not nil, print and return

	tx, err := session.DataTx()
    // if err is not nil, print and return

	v, m, err := tx.Get("db1", "key4")
    // if err is not nil, print and return

	if v == nil || m == nil {
		fmt.Println("key1 was not stored into db1")
	}

	v, m, err = tx.Get("db2", "key4")
    // if err is not nil, print and return

	if v == nil || m == nil {
		fmt.Println("key1 was not stored into db2")
	}

	err = tx.Abort()
    // if err is not nil, print and return
}
```
## (7) Multi-Signatures Transaction
