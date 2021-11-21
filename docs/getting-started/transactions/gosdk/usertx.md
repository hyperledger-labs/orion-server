---
id: usertx
title: User Administration Transaction
---
# User Administration Transaction

We can create, update and delete users of the Orion cluster using the user administration transaction.

Next, we will see an example for
  1. [Addition of Users](#2-addition-of-users)
  2. [Updation of Users](#3-updation-of-users)
  3. [Deletion of Users](#4-deletion-of-a-user)

Note that all user administration transactions must be submitted by the admin.

> As a pre-requisite, we need to first [create a connection](./../../pre-requisite/gosdk#creating-a-connection-to-the-orion-cluster) and [open a database session](./../../pre-requisite/gosdk#opening-a-database-session).

> In addition to this example, you can download and use user administration transaction example from gosdk examples folder: [orion-sdk-go/examples/api/user_tx/user_tx.go](https://github.com/hyperledger-labs/orion-sdk-go/blob/main/examples/api/user_tx/user_tx.go)

Once a [database session](./../../pre-requisite/gosdk#opening-a-database-session) is created, we can call `session.UsersTx()` to start the users
administration transaction context. On this transaction context, we have the support for following method calls:

```go 
type UsersTxContext interface {
	// PutUser introduce new user into database
	PutUser(user *types.User, acl *types.AccessControl) error
	// GetUser obtain user's record from database
	GetUser(userID string) (*types.User, error)
	// RemoveUser delete existing user from the database
	RemoveUser(userID string) error
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
refer to [creates databases using SDK](./dbtx#creation-of-databases) to create `db1` and `db2`.

## (2) Addition of Users

When the cluster is started for the first time, it will contain only the admin user specified in the `config.yml`. This admin user can add any other user to the cluster.

### (2.1) Source Code
The following code adds two new users `alice` and `bob` to the orion cluster.
```go 
package main

import (
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"strconv"

	"github.com/hyperledger-labs/orion-server/pkg/types"
)

func main() {
	bcdb, err := createConnection()
    // if err is not nil, print and return

	session, err := openSession(bcdb)
    // if err is not nil, print and return

	tx, err := session.UsersTx()
    // if err is not nil, print and return

	alice, err := createAliceUser()
    // if err is not nil, print and return

	bob, err := createBobUser()
    // if err is not nil, print and return

	err = tx.PutUser(alice, nil)
    // if err is not nil, print and return


	bobACL := &types.AccessControl{
		ReadUsers: map[string]bool{
			"admin": true,
		},
	}

	err = tx.PutUser(bob, bobACL)
    // if err is not nil, print and return

	txID, receipt, err := tx.Commit(true)
	if err != nil {
		fmt.Println("transaction did not get committed", err.Error())
		return
	}
	fmt.Println("transaction with txID " + txID + " got committed in the block " + strconv.Itoa(int(receipt.GetHeader().GetBaseHeader().GetNumber())))
}

func createAliceUser() (*types.User, error) {
	alicePemUserCert, err := ioutil.ReadFile("./crypto/alice/alice.pem")
	if err != nil {
		return nil, err
	}
	aliceCertBlock, _ := pem.Decode(alicePemUserCert)

	alice := &types.User{
		Id:          "alice",
		Certificate: aliceCertBlock.Bytes,
		Privilege: &types.Privilege{
			DbPermission: map[string]types.Privilege_Access{
				"db1": types.Privilege_Read,
				"db2": types.Privilege_ReadWrite,
			},
		},
	}

	return alice, nil
}

func createBobUser() (*types.User, error) {
	//reading and decoding bob's certificate
	bobPemUserCert, err := ioutil.ReadFile("./crypto/bob/bob.pem")
	if err != nil {
		return nil, err
	}
	bobCertBlock, _ := pem.Decode(bobPemUserCert)

	bob := &types.User{
		Id:          "bob",
		Certificate: bobCertBlock.Bytes,
		Privilege: &types.Privilege{
			DbPermission: map[string]types.Privilege_Access{
				"db1": types.Privilege_Read,
				"db2": types.Privilege_Read,
			},
		},
	}

	return bob, nil
}
```

### (2.2) Source Code Commentry

For simplicity, not all errors are handled in this code. Further, the implementation of `createConnection()` and `openSession()` can be found [here](../../pre-requisite/gosdk).

The `session.UsersTx()` starts a new user administration transaction and returns the user administration transaction context. We can then perform all
user administrative activities using this transaction context.

The first setup is to create the user `alice` and `bob`. The structure of an user is shown below:
```go
type User struct {
	Id                   string
	Certificate          []byte
	Privilege            *Privilege
}

type Privilege struct {
	DbPermission map[string]Privilege_Access
    // admin has privileges to submit a user administration transaction,
	// cluster configuration transaction, and database administration
	// transaction. Further, admin has permission to read-write states
	// from any database provided that the state has no ACL defined. If
	// a state has a read and write ACL, the admin can read or write to
	// the state only if the admin is listed in the read or write ACL list.
	Admin                bool
}
```

The `Id` field holds an unique identifier or username. The `Certificate` field holds the pem decoded certificate of the user. The `Privilege` field holds 
the access privileges of this user such as read and write permissions to various databases.

In the above code, we have created the user `alice` with read permission to the database `db1` and read-write permission to the database `db2`. Then,
we have created the user `bob` with only read permission to both `db1` and `db2`.

Once the users are created, `tx.PutUser("alice", nil)` stores the user `alice` to the database while `tx.PutUser("bob", bobACL)` stores the user `bob`
to the database with read-write ACL on the user `bob`. 

For the Access Control List (ACL), we have passed `nil` for `alice`. This means no access control on the user `alice`. Hence, any user can read and update `alice`. However, for
`bob`, we have set the ACL.

The structure of Access Control List is shown below:
```go 
type AccessControl struct {
	ReadUsers            map[string]bool
	ReadWriteUsers       map[string]bool
}
```

The `ReadUsers` can be used to define which users have read access on this user. In the above code, only the `admin` user has the read access on the user `bob`.
As no users in `ReadWriteUsers`, no one can update the user `bob`. In other words, the user `bob` can be read by `admin` and never be updated by anyone.

Finally, the transaction is committed by calling `dbtx.Commit(true)`. The argument true denotes that the synchronous submission. As a result, the `Commit()`
would return the transaction receipt if this transaction gets committed before the `TxTimeout` configured in the `openSession()`.

The structure of txReceipt can be seen [here]. The user can store this txReceipt as it is a committment used to verify the proof generated by the server.

## (3) Updation of Users 

Let's update the user `alice` to have no privileges. This can be done by
  1. fetching the `alice` record from the database
  2. setting the `Privilege` field to `nil`
  3. committing the updated `alice` record to the database

### (3.1) Source Code

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

	alice.Privilege = nil
	err = tx.PutUser(alice, nil)
    // if err is not nil, print and return

	fmt.Println("Committing transaction")
	txID, receipt, err := tx.Commit(true)
	if err != nil {
		fmt.Println("trnsaction did not get committed", err.Error())
		return
	}
	fmt.Println("transaction with txID " + txID + " got committed in the block " + strconv.Itoa(int(receipt.GetHeader().GetBaseHeader().GetNumber())))
}
```

### (3.2) Source Code Commentry
For simplicity, not all errors are handled in this code. Further, the implementation of `createConnection()` and `openSession()` can be found
[here](../../pre-requisite/gosdk).

The `session.UsersTx()` starts a new user administration transaction and returns the user administration transaction context. We can then perform
all user administrative activities using this transaction context.

In order to update the `Privilege` detail of the user `alice`, first, we fetch the `alice` record from the database. This is done by calling
`tx.GetUser("alice")`. Second, to remove all privileges assigned to the user `alice`, we set `Privilege` to `nil` in the fetched record. Third, we
store the updated `alice` to the database by calling `tx.PutUser(alice, nil)`.

Finally, the transaction is committed by calling `dbtx.Commit(true)`. The argument true denotes that the synchronous submission. As a result,
the `Commit()` would return the transaction receipt if this transaction gets committed before the `TxTimeout` configured in the `openSession()`.

## (4) Deletion of a User

Let's delete the user `bob`. To do that, we need to execute the following steps:

  1. Fetch the user `alice`. This would record the read version in the transaction.
  2. Delete the user `alice`
  3. Commit the transaction

Note that if we do not fetch the user `alice` and instead delete the user directly, it would result in a blind delete.

### (4.1) Source Code
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

	_, err = tx.GetUser("alice")
    // if err is not nil, print and return

	err = tx.RemoveUser("alice")
    // if err is not nil, print and return

	txID, receipt, err := tx.Commit(true)
	if err != nil {
		fmt.Println("transaction did not get committed", err.Error())
		return
	}
	fmt.Println("transaction with txID " + txID + " got committed in the block " + strconv.Itoa(int(receipt.GetHeader().GetBaseHeader().GetNumber())))
}
```

### (4.2) Source Code Commentry

For simplicity, not all errors are handled in this code. Further, the implementation of `createConnection()` and `openSession()` can be found
[here](../../pre-requisite/gosdk).

The `session.UsersTx()` starts a new user administration transaction and returns the user administration transaction context. We can then perform
all user administrative activities using this transaction context.

To delete the user `alice`, first, we fetch the user by calling `tx.GetUser("alice")`. This is just to record the version so that multi-version concurrency
control can be executed. Second, the user is deleted by calling `tx.RemoveUser("alice")`. Finally, the transaction is committed by calling `dbtx.Commit(true)`.
The argument true denotes that the synchronous submission. As a result, the `Commit()` would return the transaction receipt if this transaction gets
committed before the `TxTimeout` configured in the `openSession()`.

