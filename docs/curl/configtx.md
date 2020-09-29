# Cluster Configuration Transaction

To add/remove/update a database node or admin, we needs to submit a configuration transaction. As we do not have raft consensus yet, the configuration changes are no-op for now. 

When the database node boots up for the first time, it would create a genesis block with node and admin configuration provided in the `config.yml`. We can submit the following GET request to fetch the current config

```sh
curl \
   -H "Content-Type: application/json" \
   -H "UserID: admin" \
   -H "Signature: abcd" \
   -X GET http://127.0.0.1:6001/config | jq .
```
The above command would return the following committed cluster configuration. As can be seen, we have one existing node with ID `bdb-node-1`. The version of this config can be seen in the metadata field. Let's add a new node with ID `bdb-node-2`

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

Though configuration changes are no-op for now, let's at the following example:

1. Addition of a new node
2. Updating listening port of an existing node
3. Removal of an existing node


## Addition of a Node

To add a new node to the cluster, we need to execute the following steps:

1. Fetch the currently committed configuration by issuing a `GET /config`.
2. Append a new node configuration to the existing list.
3. Submit a config transaction by issuing a `POST /config {txPayload}` where txPayload is the updated json configuration constructed in step 2. Note that the signature on the payload is must. 

We need to issue a POST request on the `/config` to store the new configuration. 
> Note that the `read_old_config_version` is reflecting the newly read configuration version. 
```json
 curl \
   -H "Content-Type: application/json" \
   -X POST http://127.0.0.1:6001/config \
   --data '{
  "payload": {
    "userID": "admin",
    "txID": "1b6d6414-9b58-45d0-9723-1f31712add79",
    "read_old_config_version": {
      "block_num": 1,
      "tx_num": 0
    },
    "new_config": {
      "nodes": [
        {
          "ID": "bdb-node-1",
          "address": "127.0.0.1",
          "port": 6001,
          "certificate": "MIIByjCCAXCgAwIBAgIJANpNUdoo3I6IMAoGCCqGSM49BAMCMC0xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJOSjERMA8GA1UECgwIQ0EsIEluYy4wHhcNMjAwNzI5MDQ0MDU3WhcNMzAwNzI3MDQ0MDU3WjBDMQswCQYDVQQGEwJVUzELMAkGA1UECAwCTkoxEzARBgNVBAoMClRlc3QsIEluYy4xEjAQBgNVBAMMCWxvY2FsaG9zdDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABL5Z1vzlL0qs/YDjxs8UQG7vo7Aj1yGLoQUsbm7GLrRZw1y7RggVRNGQzBRPrZsz4edcxY9rasBKWX65QUzXs9CjYzBhMEcGA1UdIwRAMD6hMaQvMC0xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJOSjERMA8GA1UECgwIQ0EsIEluYy6CCQC1ToDGgQn0CzAJBgNVHRMEAjAAMAsGA1UdDwQEAwIE8DAKBggqhkjOPQQDAgNIADBFAiEA2MexoXmPKBRoVRWnxXbVQ2faLMkBl1oXGhJzjat81M4CIAxmI2DN2P5XXepvlgJzsKYOexAcOOZtQs8RnPdM5j1N"
        },
        {
          "ID": "bdb-node-2",
          "address": "127.0.0.2",
          "port": 6002,
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
    }
  },
  "signature": "aGVsbG8="
}'
  ```

  The above transaction would get validated and committed. We can query the updated cluster configuration using the following command:
  ```sh
curl \
   -H "Content-Type: application/json" \
   -H "UserID: admin" \
   -H "Signature: abcd" \
   -X GET http://127.0.0.1:6001/config | jq .
```

## Updating a Node Configuration

We can update the IP address, port, and certificate of an existing node using a configuration transaction. For example, we can update the listening port of `bdb-node-2` by executing the following steps. 

1. Fetch the currently committed configuration by issuing a `GET /config`.
2. For `bdb-node-2`, change the listening port from `6002` to `7002`. 
3. Submit a config transaction by issuing a `POST /config {txPayload}` where txPayload is the updated json configuration constructed in step 2. Note that the signature on the payload is must. 

We need to issue a POST request on the `/config` to store the new configuration. 

> Note that the `read_old_config_version` is reflecting the newly read configuration version. 

```json
curl \
   -H "Content-Type: application/json" \
   -X POST http://127.0.0.1:6001/config \
   --data '{
  "payload": {
    "userID": "admin",
    "txID": "1b6d6414-9b58-45d0-9723-1f31712add79",
    "read_old_config_version": {
      "block_num": 2,
      "tx_num": 0
    },
    "new_config": {
      "nodes": [
        {
          "ID": "bdb-node-1",
          "address": "127.0.0.1",
          "port": 6001,
          "certificate": "MIIByjCCAXCgAwIBAgIJANpNUdoo3I6IMAoGCCqGSM49BAMCMC0xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJOSjERMA8GA1UECgwIQ0EsIEluYy4wHhcNMjAwNzI5MDQ0MDU3WhcNMzAwNzI3MDQ0MDU3WjBDMQswCQYDVQQGEwJVUzELMAkGA1UECAwCTkoxEzARBgNVBAoMClRlc3QsIEluYy4xEjAQBgNVBAMMCWxvY2FsaG9zdDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABL5Z1vzlL0qs/YDjxs8UQG7vo7Aj1yGLoQUsbm7GLrRZw1y7RggVRNGQzBRPrZsz4edcxY9rasBKWX65QUzXs9CjYzBhMEcGA1UdIwRAMD6hMaQvMC0xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJOSjERMA8GA1UECgwIQ0EsIEluYy6CCQC1ToDGgQn0CzAJBgNVHRMEAjAAMAsGA1UdDwQEAwIE8DAKBggqhkjOPQQDAgNIADBFAiEA2MexoXmPKBRoVRWnxXbVQ2faLMkBl1oXGhJzjat81M4CIAxmI2DN2P5XXepvlgJzsKYOexAcOOZtQs8RnPdM5j1N"
        },
        {
          "ID": "bdb-node-2",
          "address": "127.0.0.2",
          "port": 7002,
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
    }
  },
  "signature": "aGVsbG8="
}'
```

## Deleting a Node

Similar to addition and updation of nodes, we can do the deletion of a node too. For example, to delete a node `bdb-node-2`, we can execute the following steps:

1. Fetch the currently committed configuration by issuing a `GET /config`.
2. Remove `bdb-node-2` entry.
3. Submit a config transaction by issuing a `POST /config {txPayload}` where txPayload is the updated json configuration constructed in step 2. Note that the signature on the payload is must. 

We need to issue a POST request on the `/config` to store the new configuration. 

> Note that the `read_old_config_version` is reflecting the newly read configuration version. 

```json
curl \
   -H "Content-Type: application/json" \
   -X POST http://127.0.0.1:6001/config \
   --data '{
  "payload": {
    "userID": "admin",
    "txID": "1b6d6414-9b58-45d0-9723-1f31712add79",
    "read_old_config_version": {
      "block_num": 3,
      "tx_num": 0
    },
    "new_config": {
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
    }
  },
  "signature": "aGVsbG8="
}'
```

## Add/Update/Delete Admins

During the initial bootup of the `bdb` server, the admin configuration provided in the `config.yml` is used. Similar to node configuration transaction, we can submit configuration transaction to add/update/delete admin configurations too. 