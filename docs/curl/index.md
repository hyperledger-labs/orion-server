# CURL Commands

The CURL command can be used to query and submit transactions to the BDB server. However, the signature on the message must be generated separately using our utility [TODO].

For building the executable, refer to [TODO].

Before issuing the query and transaction commands, the BDB server needs to be started. A sample command to start the BDB server is provided below:

```sh
./bdb start --configpath $HOME/projects/github.ibm.com/blockchaindb/server/config/
```

For more details on other options available with *bdb* command and configuration details, refer to [TODO]

## Queries

Let's look at some of the queries that we execute as soon as the BDB server is started. To pretty print the JSON output, we use `jq`. The installation steps for `jq` can be found [here](https://stedolan.github.io/jq/).

For each query, we need to set the `UserID` and `Signature` header. As the signing utility needs to be built, for now, user can provide any random string as a signature. Currently, we do not verify the signature. 

We support four types of queries:
1. Querying the cluster configuration
2. Querying a user information/credentials/privileges
3. Checking the existance of a database
4. Querying a data

An example for each of the above query type is provided in [queries](query.md).

## Transactions

We support four types of transactions:
1. Cluser configuration transaction
    - for adding/deleting/updating a node and admin configuration. For an example CURL command, refer to [config transaction](configtx.md).
2. Database administration transaction
    - for creating/deleting a database. For an example CURL command, refer to [db transaction](dbtx.md).
3. User administration transaction
    - for adding/deleting/updating a user credentials/privileges. For an example CURL command, refer to [user transaction](usertx.md).
4. Data transaction. 
    - for adding/deleting/updating a data/state. For an example CURL command, refer to [data transaction](datatx.md).
