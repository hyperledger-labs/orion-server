---
id: gosdk
title: Creating a Connection and Opening a Session with SDK
---

<!--
 Copyright IBM Corp. All Rights Reserved.

 SPDX-License-Identifier: CC-BY-4.0
 -->

When we use the SDK to perform queries and transactions, the following two steps must be executed first:

 1. Creating a connection to the Orion cluster
 2. Opening a database session with the Orion cluster

Let's look at these two steps.

> We have an example of creating a connection and opening a session at [orion-sdk-go/examples/api/connection-and-session].

## Creating a Connection to the Orion Cluster

### Source Code
The following function creates a connection to our [single node Orion cluster](./../launching-one-node/binary) deployed using the sample configuration.
```go
func createConnection() (bcdb.BCDB, error) {
	logger, err := logger.New(
		&logger.Config{
			Level:         "debug",
			OutputPath:    []string{"stdout"},
			ErrOutputPath: []string{"stderr"},
			Encoding:      "console",
			Name:          "bcdb-client",
		},
	)
	if err != nil {
		return nil, err
	}

	conConf := &config.ConnectionConfig{
		ReplicaSet: []*config.Replica{
			{
				ID:       "bdb-node-1",
				Endpoint: "http://127.0.0.1:6001",
			},
		},
		RootCAs: []string{
			"./crypto/CA/CA.pem",
		},
		Logger: logger,
	}

	db, err := bcdb.Create(conConf)
	if err != nil {
		return nil, err
	}

	return db, nil
}
```

### Source Code Commentry
The `bcdb.Create()` method in the `bcdb` package at the SDK prepares a connection context to the Orion cluster
and loads the certificate of certificate authorities.

The signature of the `Create()` function is shown below:
```go
func Create(config *config.ConnectionConfig) (BCDB, error)
```
The parameter `config.ConnectionConfig` holds 
 1. the `ID` and `IP address` of each Orion node in the cluster
 2. certificate of root CAs, and
 3. a logger to log messages

The structure of the `config.ConnectionConfig` is shown below:
```go
// ConnectionConfig required configuration in order to
// open session with BCDB instance, replica set informations
// servers root CAs
type ConnectionConfig struct {
	// List of replicas URIs client can connect to
	ReplicaSet []*Replica
	// Keeps path to the server's root CA
	RootCAs []string
	// Logger instance, if nil an internal logger is created
	Logger *logger.SugarLogger
}

// Replica
type Replica struct {
	// ID replica's ID
	ID string
	// Endpoint the URI of the replica to connect to
	Endpoint string
}
```

In our [simple deployment](./../launching-one-node/binary), we have only one node in the cluster. Hence, we have one `Replica` with the
`ID` as `bdb-node-1` and `Endpoint` as `http://127.0.0.1:6001`. Further, we have only one root certificate authority and hence, the
`RootCAs` holds the path to a single CA's certificate only.

The `Create()` would return the `BCDB` implementation that allows the user to create database sessions with the Orion cluster.
```go
type BCDB interface {
	// Session instantiates session to the database
	Session(config *config.SessionConfig) (DBSession, error)
}
```

## Opening a Database Session

### Source Code

Now, that we can created a connection and received the `BCDB` implementation, we can open a database session by calling the `Session()` method.
The following function opens a database session for a given connection.
```go
func openSession(db bcdb.BCDB, userID string) (bcdb.DBSession, error) {
	sessionConf := &config.SessionConfig{
		UserConfig: &config.UserConfig{
			UserID:         userID,
			CertPath:       "./crypto/" + userID + "/" + userID + ".pem",
			PrivateKeyPath: "./crypto/" + userID + "/" + userID + ".key",
		},
		TxTimeout:    20 * time.Second,
		QueryTimeout: 10 * time.Second,
	}

	session, err := db.Session(sessionConf)
	if err != nil {
		return nil, err
	}

	return session, nil
}
```

### Source Code Commentry

The signature of `Session()` method is shown below:
```go
Session(config *config.SessionConfig) (DBSession, error)
```

The `Session()` takes `config.SessionConfig` as a parameter which holds the user credentials, transaction timeout, and query timeout.
The structure of the `config.SessionConfig` is shown below:

```go
// SessionConfig keeps per database session
// configuration information
type SessionConfig struct {
	UserConfig *UserConfig
	// The transaction timeout given to the server in case of tx sync commit - `tx.Commit(true)`.
	// SDK will wait for `TxTimeout` + some communication margin
	// or for timeout error from server, whatever come first.
	TxTimeout time.Duration
	// The query timeout - SDK will wait for query result maximum `QueryTimeout` time.
	QueryTimeout time.Duration
}


// UserConfig user related information
// maintains wallet with public and private keys
type UserConfig struct {
	// UserID the identity of the user
	UserID string
	// CertPath path to the user's certificate
	CertPath string
	// PrivateKeyPath path to the user's private key
	PrivateKeyPath string
}
```

As the `admin` user is submitting the transactions, we have set the `UserConfig` to hold the userID of `admin`, certificate, and private key  of
the `admin` user. The transaction timeout is set to 20 seconds. This means that the SDK would wait for 20 seconds to receive the
transaction's status and receipt synchronously. Once timeout happens, the SDK needs to pool for the transaction status asychronously.

The `Session()` would return the `DBSession` implementation that allows the user to execute various database transactions and queries.
The `DBSession` implementation supports the following methods:
```go
// DBSession captures user's session
type DBSession interface {
    // DBsTx starts a Database Administration Transaction
	DBsTx() (DBsTxContext, error)
    // UserTx starts a User Administration Transaction
	UsersTx() (UsersTxContext, error)
    // DataTx starts a Data Transaction
	DataTx(options ...TxContextOption) (DataTxContext, error)
    // LoadDataTx loads a pre-compileted data transaction
	LoadDataTx(*types.DataTxEnvelope) (LoadedDataTxContext, error)
    // ConfigTx starts a Cluster Configuration Transaction
	ConfigTx() (ConfigTxContext, error)
    // Provenance returns a provenance querier that supports various provenance queries
	Provenance() (Provenance, error)
    // Ledger returns a ledger querier that supports various ledger queries
	Ledger() (Ledger, error)
    // JSONQuery returns a JSON querier that supports complex queries on value fields using JSON syntax
	JSONQuery() (JSONQuery, error)
}
```

Once the user gets the `DBSession`, any types of transaction can be started
```go
    tx, err := session.DBsTx()
```
