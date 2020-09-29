
## Querying the Cluster Configuration

The cluster configuration includes node and admin configuration. When the BDB server bootup for the first time, it reads nodes and admins configuration present in the `config.yml` and creates a genesis block. The user can query the current cluster configuration by issing a GET request on `/config` endpoint.

```sh
curl \
   -H "Content-Type: application/json" \
   -H "UserID: admin" \
   -H "Signature: abcd" \
   -X GET http://127.0.0.1:6001/config | jq .
   ```
A sample output of above command is shown below. The actual content might change depending on the configuration specified in `config.yml`. For now, all users in the cluster can query the cluster configuration to identity the set of nodes, IP address of each node along with the listening port number, certificate of the node, etc...

```json
{
  "payload": {
    "header": {
      "nodeID": "bdb-node-1"
    },
    "config": {
      "nodes": [
        {
          "ID": "bdb-node-1",
          "address": "127.0.0.1",
          "port": 6001,
          "certificate": "MIIByjCCAXCgAwIBAgIJANpNUdoo3I6IMAoGCCqGSM49BAMCMC0xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJOSjERMA8GA1UECgwIQ0EsIEluYy4wHhcNMjAwNzI5MDQ0MDU3WhcNMzAwNzI3MDQ0MDU3WjBDMQswCQYDVQQGEwJVUzELMAkGA1UECAwCTkoxEzARBgNVBAoMClRlc3QsIEluYy4xEjAQBgNVBAMMCWxvY2FsaG9zdDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABL5Z1vzlL0qs/YDjxs8UQG7vo7Aj1yGLoQUsbm7GLrRZw1y7RggVRNGQzBRPrZsz4edcxY9rasBKWX65QUzXs9CjYzBhMEcGA1UdIwRAMD6hMaQvMC0xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJOSjERMA8GA1UECgwIQ0EsIEluYy6CCQC1ToDGgQn0CzAJBgNVHRMEAjAAMAsGA1UdDwQEAwIE8DAKBggqhkjOPQQDAgNIADBFAiEA2MexoXmPKBRoVRWnxXbVQ2faLMkBl1oXGhJzjat81M4CIAxmI2DN2P5XXepvlgJzsKYOexAcOOZtQs8RnPdM5j1N"
        }
      ],
      "admins": [
        {
          "ID": "admin",
          "certificate": "MIIByTCCAXCgAwIBAgIJANpNUdoo3I6JMAoGCCqGSM49BAMCMC0xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJOSjERMA8GA1UECgwIQ0EsIEluYy4wHhcNMjAwNzI5MDQ0MDU3WhcNMzAwNzI3MDQ0MDU3WjBDMQswCQYDVQQGEwJVUzELMAkGA1UECAwCTkoxEzARBgNVBAoMClRlc3QsIEluYy4xEjAQBgNVBAMMCWxvY2FsaG9zdDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABKcC7RwceItyu0j6FPOpw1lcdSwYME6ut0khaYGGHR2GM/yQ0EDy2Iuz3shiYJlbbmXRFruP13BlIzkebM8YX82jYzBhMEcGA1UdIwRAMD6hMaQvMC0xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJOSjERMA8GA1UECgwIQ0EsIEluYy6CCQC1ToDGgQn0CzAJBgNVHRMEAjAAMAsGA1UdDwQEAwIE8DAKBggqhkjOPQQDAgNHADBEAiBEorw60V70bRWh4KWq0II5zCPkfeTjLOV9GZYTBlEvTgIgXeiJr/r/+oyHJbtDm9GUS9M8o1Se7B6vAP9if+/mVC0="
        }
      ],
      "rootCA_certificate": "MIIBSjCB8AIJALVOgMaBCfQLMAoGCCqGSM49BAMCMC0xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJOSjERMA8GA1UECgwIQ0EsIEluYy4wHhcNMjAwNzI5MDQ0MDU3WhcNMzAwNzI3MDQ0MDU3WjAtMQswCQYDVQQGEwJVUzELMAkGA1UECAwCTkoxETAPBgNVBAoMCENBLCBJbmMuMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEOQgOfb8X1GU8vdxSDW7KjCSRhjshDwKHgf6T3m0YhebGRQS1ey0eAXEVopWmxJJHBu1ETfT26eBjtxJ+5kmRojAKBggqhkjOPQQDAgNJADBGAiEAx/8k4agfLJywKgktChASTZOXTddc9S9xF24ZuQ7xF28CIQC8TG/lhVRWUaTUTUYGpdSp6tJVLf86xBW2d+h4GtcEhQ=="
    },
    "metadata": {
      "version": {
        "block_num": 1
      }
    }
  }
}
```

## Querying the User Information

One user can query information about another user or self. If the access control is defined for the user entry, it would be enforced during the query. A user information can be retrieved by issuing a GET request on `/user/{userid}` endpoint where `{userid}` should be replaced with the ID of the user whom information needs to be fetched. 

In our `config.yml`, we have an admin entry with ID `admin`. Hence, we do a self query.
```sh
curl \
   -H "Content-Type: application/json" \
   -H "UserID: admin" \
   -H "Signature: abcd" \
   -X GET http://127.0.0.1:6001/user/admin | jq
```
A sample output of the above command is shown below. 
```json
{
  "payload": {
    "header": {
      "nodeID": "bdb-node-1"
    },
    "user": {
      "ID": "admin",
      "certificate": "MIIByTCCAXCgAwIBAgIJANpNUdoo3I6JMAoGCCqGSM49BAMCMC0xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJOSjERMA8GA1UECgwIQ0EsIEluYy4wHhcNMjAwNzI5MDQ0MDU3WhcNMzAwNzI3MDQ0MDU3WjBDMQswCQYDVQQGEwJVUzELMAkGA1UECAwCTkoxEzARBgNVBAoMClRlc3QsIEluYy4xEjAQBgNVBAMMCWxvY2FsaG9zdDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABKcC7RwceItyu0j6FPOpw1lcdSwYME6ut0khaYGGHR2GM/yQ0EDy2Iuz3shiYJlbbmXRFruP13BlIzkebM8YX82jYzBhMEcGA1UdIwRAMD6hMaQvMC0xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJOSjERMA8GA1UECgwIQ0EsIEluYy6CCQC1ToDGgQn0CzAJBgNVHRMEAjAAMAsGA1UdDwQEAwIE8DAKBggqhkjOPQQDAgNHADBEAiBEorw60V70bRWh4KWq0II5zCPkfeTjLOV9GZYTBlEvTgIgXeiJr/r/+oyHJbtDm9GUS9M8o1Se7B6vAP9if+/mVC0=",
      "privilege": {
        "DB_administration": true,
        "cluster_administration": true,
        "user_administration": true
      }
    },
    "metadata": {
      "version": {
        "block_num": 1
      }
    }
  }
}
```
## Checking the Database Existance

To check whether a database exist/created, the user can issue a GET request on `/db/{dbname}` endpoint where `{dbname}` should be replaced with the `dbname` for which the user needs to perform this check.

When the BDB server bootups, it creates a default database called `bdb` in the cluster. We can check its existance using the following command:

```sh
curl \
   -H "Content-Type: application/json" \
   -H "UserID: admin" \
   -H "Signature: abcd" \
   -X GET http://127.0.0.1:6001/db/bdb | jq .
```
The above command results in the following output:
```json
{
  "payload": {
    "header": {
      "nodeID": "bdb-node-1"
    },
    "exist": true
  }
}
```

## Error Due to Missing HTTP Request Headers

Every query must set UserID and Signature in the HTTP request header. If any of these two is missing, the error would be returned from the BDB server. For a correct usage, refer to [queries](query.md).

### Missing UserID

```sh
curl \
   -H "Content-Type: application/json" \
   -H "Signature: abcd" \
   -X GET http://127.0.0.1:6001/config | jq .
```

```json
{
  "error": "UserID is not set in the http request header"
}
```

### Missing Signature
```sh
curl \
   -H "Content-Type: application/json" \
   -H "UserID: admin" \
   -X GET http://127.0.0.1:6001/config | jq .
```
```json
{
  "error": "Signature is not set in the http request header"
}
```

## Error Due to Non-Existing User

The userID set in the header must exist in the cluster. Otherwise, the query would be rejected with an error. 
```sh
curl \
   -H "Content-Type: application/json" \
   -H "UserID: user1" \
   -H "Signature: abcd" \
   -X GET http://127.0.0.1:6001/config | jq .
```
```json
{
  "error": "/config query is rejected as the submitting user [user1] does not exist in the cluster"
}
```

## Error Due to Access Control Permission