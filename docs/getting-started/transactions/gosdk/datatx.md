---
id: datatx
title: Data Transaction
---
# Data Transaction

We can create, update, and delete states maintained by the Orion cluster using data transactions.

Using a data transaction, we can do the following:

  1. [Create new states](#1-create-new-states)
  2. [Update existing states](#2-update-an-existing-state)
  3. [Delete existing states](#3-delete-an-existing-state)
  4. [Create, update, and delete states within a single transaction](#4-create-update-delete-states-within-a-single-transaction)
  5. [Operations on multiple databases](#5-operations-on-multiple-databases-in-a-single-transaction)
  6. [Multi-signatures transaction](#6-multi-signatures-transaction)

:::info prerequisite
For all examples shown here to work, we need to have

 1. Two databases named `db1` and `db2` in the Orion server. If you have not created these two databases,
refer to [creates databases using SDK](./dbtx#creation-of-databases) create `db1` and `db2`.
 2. Two users named `alice` and `bob`. If you have not created these users already, refer to [create users using SDK](./usertx##2-addition-of-users).

Finally, [Create a connection](../../pre-requisite/gosdk#creating-a-connection-to-the-orion-cluster) and
[Open a database session](../../pre-requisite/gosdk#opening-a-database-session).
:::

Once a [database session](./../../pre-requisite/gosdk#opening-a-database-session) is created, a call to `session.DataTx()` will create a new data transaction context and thus will start a new data
transaction. The data transaction context provides the following methods to calls:

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

## 1) Create new states in database

### 1.1) Source Code

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
			"bob":   true,
		},
		ReadWriteUsers: map[string]bool{
			"alice": true,
		},
		SignPolicyForWrite: types.AccessControl_ANY,
	}
	err = tx.Put("db2", "key1", jVal, acl)
    // if err is not nil, print and return

	txID, receipt, err := tx.Commit(true)
    // if err is not nil, print and return

	fmt.Println("transaction with txID " + txID + " got committed in the block " + strconv.Itoa(int(receipt.GetHeader().GetBaseHeader().GetNumber())))
}
```

### 1.2) Source Code Commentary
For the sake of simplicity, not all errors are handled in this code. Furthermore, the implementation of `createConnection()` and `openSession()` can be found [here](../../pre-requisite/gosdk).

The `session.DataTx()` starts a new data transaction and returns the data transaction context. We can then perform all
data manipulation activities using this transaction context.

Let's create a key `key1` with the value `{"name":"abc","age":31,"graduated":true}`. In the above code, we use the struct with JSON tags and then marshal the struct to create this JSON value.

Once the value is created, we define the access control for this key `key1`. The structure of ACL is shown below:
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
The ACL holds
 1. `ReadUsers`: a list of users who can only read the key
 2. `ReadWriteUsers`: a list of users who can read and write the key.
 3. `SignPolicyForWrite`: denotes whether signature of all users in the `ReadWriteUsers` is needed to perform writes to the key.

The following are various ACL configurations and associated effects:
 1. If the ACL is `nil` for a key, any users of Orion can access that key.
 2. If the ACL has only `ReadUsers` for a key, then the key would become read-only forever by users present in the `ReadUsers` list.
 3. If the ACL has `ReadWriteUsers` for a key, users in this list can read and write that key. As `SignPolicyForWrite` is not set, it uses the default policy which is `AccessControl_ANY`. This means that any user in the `ReadWriteUsers` can write to the key.
 4. If the ACL has only `SignPolicyForWrite` but `ReadUsers` and `ReadWriteUsers` are empty for a key, then no user can read or write to that key forever.
 5. If the ACL has `ReadWriteUsers` and `AccessControl_ALL` for the `SignPolicyForWrite`, then the key can be read by any users in the `ReadWriteUsers` but only when all the users sign the transaction, can the key be updated or deleted.
 6. If the ACL has `ReadUsers` and `SignPolicyForWrite` for a key, then the key would become read-only forever by users present in the `ReadUsers` list.

In our code, we have provided the read access on `key1` to `alice` and `bob` but only `alice` can write to the key. As there is a
single user in the `ReadWriteUsers`, we have set the `SignPolicyForWrite` to `AccessControl_ANY`. Even if we set
`SignPolicyForWrite` to `AccessControl_ALL`, it works, as only `alice` has the write permission on `key1`.

We then use `tx.Put()` to store the key `key1` with the value `jVal` and an access control list `acl` in the database `db2`.

Finally, the transaction is committed by calling `dbtx.Commit(true)`. The argument true denotes that this is a synchronous submission. As a result, the `Commit()`
returns the transaction receipt if this transaction gets committed before the `TxTimeout` configured in the `openSession()`.

The structure of txReceipt can be seen [here]. The user can store this txReceipt, as it is a commitment used to verify the proof generated by the server.

Refer to [Query Data](../../queries/gosdk/simple-data-query) in queries to check whether `key1` has been created.

## 2) Update an existing state

### 2.1) Source Code

Let's update the value of `key1`. To do that, we need to execute the following three steps:

1. Fetch `key1` from the server.
2. Construct the updated value.
    - update the `age` from `31`. The new value would be `{"name":"abc","age":32,"graduated":true}`
3. Commit the data transaction.

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

### 2.2) Soure Code Commentary
For the sake of simplicity, not all errors are handled in this code. Further, the implementation of `createConnection()` and `openSession()`
can be found [here](../../pre-requisite/gosdk).

The `session.DataTx()` starts a new data transaction and returns the data transaction context. We can then perform all data manipulation activities using this transaction context.

As we need to update the value of `key1`, first we need to fetch `key1` from the Orion server by calling `tx.Get("db2", "key1")`.
The fetched value is `[]byte` and it holds the JSON marshalled value. We need to unmarshal it to the `person` object. From
the `person` object, it is easy to change the `Age` to `32`.

Once the fetched value is updated locally, we need to marshal it again by calling `json.Marshal()` and store the updated marshalled value by calling `tx.Put("db2", "key1", jVal, m.AccessControl)`. Note that we are passing the same access control as we are not making any changes to it.

Finally, the transaction is committed by calling `dbtx.Commit(true)`. The argument "true" denotes that this is a synchronous submission. As a result, the `Commit()`
returns the transaction receipt if this transaction gets committed before the `TxTimeout` configured in the `openSession()`.

The structure of txReceipt can be seen [here]. The user can store this txReceipt as it is a commitment used to verify the proof generated by the server.

Refer to [Query Data](../../queries/gosdk/simple-data-query) in queries to check whether `key1` has been updated.

## 3) Delete an existing state

### 3.1) Source Code

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

### 3.2) Source Code Commentary
For the sake of simplicity, not all errors are handled in this code. Furthermore, the implementation of `createConnection()` and `openSession()`
can be found [here](../../pre-requisite/gosdk).

The `session.DataTx()` starts a new data transaction and returns the data transaction context. We can then perform all data manipulation activities using this transaction context.

To delete the key `key1`, we need to call `tx.Delete("db2", "key1")`. Note that this is a blind delete. If you want to avoid the blind delete, we need to first fetch `key1` by calling `tx.Get("db2", "key1")` but ignoring the returned value and version. Next, we need to call `tx.Delete("db2", "key1")` to delete the key `key1`.

Finally, the transaction is committed by calling `tx.Commit(true)`. The argument "true" denotes that this is a synchronous submission. As a result, the `Commit()`
returns the transaction receipt if this transaction gets committed before the `TxTimeout` configured in the `openSession()`.

The structure of txReceipt can be seen [here]. The user can store this txReceipt as it is a commitment used to verify the proof generated by the server.

## 4) Create, update, and delete states within a single transaction

<details>
<summary> Let's create `key1` and `key2` so that in the next transaction we can do all three operations</summary>.

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

</details>


### 4.1) Source Code

Now that we have the required data in the server, we can create, update, and delete within a single transaction.

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

### 4.2) Source Code Commentary
For the sake of simplicity, not all errors are handled in this code. Further, the implementation of `createConnection()` and `openSession()`
can be found [here](../../pre-requisite/gosdk).

The `session.DataTx()` starts a new data transaction and returns the data transaction context. We can then perform all data manipulation activities using this transaction context.

It can be clearly seen from the source that we use the same transaction context to create a new key `key3`,
update an existing key `key2`, and delete an existing key `key1`.

The value for `key3` is created by marshaling the `person` object. Then, `tx.Put("db2", "key3", jVal, acl)` stores
`key3` with the value `{"name":"ghi","age":24,"graduated":true}`. Only `alice` has read and write permission on `key3` as defined in the `ReadWriteUsers` access control list.

For updating `key2`, first, we fetch `key2` by calling `tx.Get("db2", "key2")`. Next, the fetched value is unmarshalled
into a person object such that fields such as `age` and `graduated` can be updated. Then, the updated value is
stored by calling `tx.Put("db2", "key2", jVal, m.AccessControl)`.

Finally, the transaction is committed by calling `tx.Commit(true)`. The argument "true" denotes that this is a synchronous submission. As a result, the `Commit()`
returns the transaction receipt if this transaction gets committed before the `TxTimeout` configured in the `openSession()`.

The structure of txReceipt can be seen [here]. The user can store this txReceipt as it is a commitment used to verify the proof generated by the server.

Refer to [Query Data](../../queries/gosdk/simple-data-query) in queries to check whether `key3` has been created,
`key2` has been updated, and `key1` has been deleted.

## 5) Operations on Multiple Databases in a Single Transaction

A data transaction can access or modify more than one user database in a transaction. In the below example,
we perform operations on two databases, `db1` and `db2`, within a single transaction.

<details>
<summary> Update `alice`'s privileges </summary>
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
</details>

### 5.1) Source Code

Let's create
  1. Key `key4` with value `{"name":"abc","age":31,"graduated":true}` in `db1`
  2. Key `key4` with value `{"name":"def","age":20,"graduated":false}` in `db2`

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

### 5.2) Source Code Commentary
For the sake of simplicity, not all errors are handled in this code. Further, the implementation of `createConnection()` and `openSession()`
can be found [here](../../pre-requisite/gosdk).

The `session.DataTx()` starts a new data transaction and returns the data transaction context. We can then perform all data manipulation activities using this transaction context.

In database `db1`, we create a key `key4` with the value `{"name":"abc","age":31,"graduated":true}` using the person object and
json marshal. Then, the value is stored by calling `tx.Put("db1", "key4", jVal, acl)`. Note that the first parameter is `db1`.
As per the ACL, `bob` has only read permission, while `alice` has both read and write permissions.

In database `db2`, we create a key `key4` with the value `{"name":"def","age":20,"graduated":false}` using the person object and
json marshal. Then, the value is stored by calling `tx.Put("db2", "key4", jVal, nil)`. Note that the first parameter is `db1`.
Further, there is no ACL on `key4`. As a result, both `alice` and `bob` can read or write to `key4`. In fact, any new user of Orion can read or write to `key4` when the ACL is empty.

:::info Additional Examples
In addition to this example, you can download and use the data transaction examples from the go-sdk examples folder: [orion-sdk-go/examples/api/simple_tx/simple_tx.go](https://github.com/hyperledger-labs/orion-sdk-go/blob/main/examples/api/simple_tx/simple_tx.go)
:::
