# User Administration Transaction
We can create/update/delete users of the `bdb` database cluster using the user administration transaction. By issuing a `POST /user {txPayload}`, we can perform the user administration.

## Addition of a User

When the cluster is started for the first time, it will contain only the admin user specified in the `config.yml`. This admin user can add any other user to the cluster. In the below example, the admin user is adding a new user called `user1` with certain privileges. Along with that, there is also an access control rule per user which would decide who can read/write information associated with the `user1`. If no access control is specified, anyone can read the information but only those users who have user admin privilege can update the information. Let's add a new user `user1` to the `bdb` cluster. 

> Within a single transaction, we can add more than a single user. For simplicity, the example adds a single user only.

```json
 curl \
   -H "Content-Type: application/json" \
   -X POST http://127.0.0.1:6001/user \
   --data '{
	"payload": {
		"userID": "admin",
		"txID": "1b6d6414-9b58-45d0-9723-1f31712add01",
		"user_writes": [
			{
				"user": {
					"ID": "user1",
					"certificate": "MIIByTCCAXCgAwIBAgIJANpNUdoo3I6JMAoGCCqGSM49BAMCMC0xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJOSjERMA8GA1UECgwIQ0EsIEluYy4wHhcNMjAwNzI5MDQ0MDU3WhcNMzAwNzI3MDQ0MDU3WjBDMQswCQYDVQQGEwJVUzELMAkGA1UECAwCTkoxEzARBgNVBAoMClRlc3QsIEluYy4xEjAQBgNVBAMMCWxvY2FsaG9zdDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABKcC7RwceItyu0j6FPOpw1lcdSwYME6ut0khaYGGHR2GM/yQ0EDy2Iuz3shiYJlbbmXRFruP13BlIzkebM8YX82jYzBhMEcGA1UdIwRAMD6hMaQvMC0xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJOSjERMA8GA1UECgwIQ0EsIEluYy6CCQC1ToDGgQn0CzAJBgNVHRMEAjAAMAsGA1UdDwQEAwIE8DAKBggqhkjOPQQDAgNHADBEAiBEorw60V70bRWh4KWq0II5zCPkfeTjLOV9GZYTBlEvTgIgXeiJr/r/+oyHJbtDm9GUS9M8o1Se7B6vAP9if+/mVC0=",
					"privilege": {
						"DB_permission": {
                            "db1": 0,
                            "db2": 1
						},
						"DB_administration": true,
						"cluster_administration": true,
						"user_administration": true
					}
				},
				"ACL": {
					"read_users": {
						"admin": true
					},
					"read_write_users": {
						"admin": true
					}
				}
			}
		]
	},
    "signature": "aGVsbG8="
}'
```
Once the transaction get committed, we can query the user information as follows:

```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: admin" \
     -H "Signature: abcd" \
     -X GET http://127.0.0.1:6001/user/user1 | jq .
```
**Output**
```json
{
  "payload": {
    "header": {
      "nodeID": "bdb-node-1"
    },
    "user": {
      "ID": "user1",
      "certificate": "MIIByTCCAXCgAwIBAgIJANpNUdoo3I6JMAoGCCqGSM49BAMCMC0xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJOSjERMA8GA1UECgwIQ0EsIEluYy4wHhcNMjAwNzI5MDQ0MDU3WhcNMzAwNzI3MDQ0MDU3WjBDMQswCQYDVQQGEwJVUzELMAkGA1UECAwCTkoxEzARBgNVBAoMClRlc3QsIEluYy4xEjAQBgNVBAMMCWxvY2FsaG9zdDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABKcC7RwceItyu0j6FPOpw1lcdSwYME6ut0khaYGGHR2GM/yQ0EDy2Iuz3shiYJlbbmXRFruP13BlIzkebM8YX82jYzBhMEcGA1UdIwRAMD6hMaQvMC0xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJOSjERMA8GA1UECgwIQ0EsIEluYy6CCQC1ToDGgQn0CzAJBgNVHRMEAjAAMAsGA1UdDwQEAwIE8DAKBggqhkjOPQQDAgNHADBEAiBEorw60V70bRWh4KWq0II5zCPkfeTjLOV9GZYTBlEvTgIgXeiJr/r/+oyHJbtDm9GUS9M8o1Se7B6vAP9if+/mVC0=",
      "privilege": {
        "DB_permission": {
          "db1": 0,
          "db2": 1
        },
        "DB_administration": true,
        "cluster_administration": true,
        "user_administration": true
      }
    },
    "metadata": {
      "version": {
        "block_num": 18
      },
      "access_control": {
        "read_users": {
          "admin": true
        },
        "read_write_users": {
          "admin": true
        }
      }
    }
  }
}
```
## Updation of a User

Let's remove all privileges given to `user1`. In order to do that, we can execute the following steps:

1. Fetch the current committed information of the `user1`.
2. Remove the privilege section and construct the transaction payload.
3. Submit the transaction payload by issuing a `POST /user {txPayload}`

> Note that the `user_reads` contains the version of the read information. If this does not match the committed version, the transaction would be invalidated. 

> Within a single transaction, we can update more than a single user. For simplicity, the example updates a single user only.

```json
 curl \
   -H "Content-Type: application/json" \
   -X POST http://127.0.0.1:6001/user \
   --data '{
	"payload": {
		"userID": "admin",
		"txID": "1b6d6414-9b58-45d0-9723-1f31712add02",
        "user_reads": [
			{
				"userID": "user1",
				"version": {
					"block_num": 18,
					"tx_num": 0
				}
			}
		],
		"user_writes": [
			{
				"user": {
					"ID": "user1",
					"certificate": "MIIByTCCAXCgAwIBAgIJANpNUdoo3I6JMAoGCCqGSM49BAMCMC0xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJOSjERMA8GA1UECgwIQ0EsIEluYy4wHhcNMjAwNzI5MDQ0MDU3WhcNMzAwNzI3MDQ0MDU3WjBDMQswCQYDVQQGEwJVUzELMAkGA1UECAwCTkoxEzARBgNVBAoMClRlc3QsIEluYy4xEjAQBgNVBAMMCWxvY2FsaG9zdDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABKcC7RwceItyu0j6FPOpw1lcdSwYME6ut0khaYGGHR2GM/yQ0EDy2Iuz3shiYJlbbmXRFruP13BlIzkebM8YX82jYzBhMEcGA1UdIwRAMD6hMaQvMC0xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJOSjERMA8GA1UECgwIQ0EsIEluYy6CCQC1ToDGgQn0CzAJBgNVHRMEAjAAMAsGA1UdDwQEAwIE8DAKBggqhkjOPQQDAgNHADBEAiBEorw60V70bRWh4KWq0II5zCPkfeTjLOV9GZYTBlEvTgIgXeiJr/r/+oyHJbtDm9GUS9M8o1Se7B6vAP9if+/mVC0="
				},
				"ACL": {
					"read_users": {
						"admin": true
					},
					"read_write_users": {
						"admin": true
					}
				}
			}
		]
	},
    "signature": "aGVsbG8="
}'
```
Once the transaction get committed, we can query the user information as follows to check whether all privileges given to the `user1` have been removed:

```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: admin" \
     -H "Signature: abcd" \
     -X GET http://127.0.0.1:6001/user/user1 | jq .
```
**Output**
```json
{
  "payload": {
    "header": {
      "nodeID": "bdb-node-1"
    },
    "user": {
      "ID": "user1",
      "certificate": "MIIByTCCAXCgAwIBAgIJANpNUdoo3I6JMAoGCCqGSM49BAMCMC0xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJOSjERMA8GA1UECgwIQ0EsIEluYy4wHhcNMjAwNzI5MDQ0MDU3WhcNMzAwNzI3MDQ0MDU3WjBDMQswCQYDVQQGEwJVUzELMAkGA1UECAwCTkoxEzARBgNVBAoMClRlc3QsIEluYy4xEjAQBgNVBAMMCWxvY2FsaG9zdDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABKcC7RwceItyu0j6FPOpw1lcdSwYME6ut0khaYGGHR2GM/yQ0EDy2Iuz3shiYJlbbmXRFruP13BlIzkebM8YX82jYzBhMEcGA1UdIwRAMD6hMaQvMC0xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJOSjERMA8GA1UECgwIQ0EsIEluYy6CCQC1ToDGgQn0CzAJBgNVHRMEAjAAMAsGA1UdDwQEAwIE8DAKBggqhkjOPQQDAgNHADBEAiBEorw60V70bRWh4KWq0II5zCPkfeTjLOV9GZYTBlEvTgIgXeiJr/r/+oyHJbtDm9GUS9M8o1Se7B6vAP9if+/mVC0="
    },
    "metadata": {
      "version": {
        "block_num": 19
      },
      "access_control": {
        "read_users": {
          "admin": true
        },
        "read_write_users": {
          "admin": true
        }
      }
    }
  }
}
```
## Deletion of a User

Let's delete `user1` from the cluster. In order to do that, we can execute the following steps:

1. Fetch the current committed information of the `user1` to get the committed version.
2. Add the committed version to the `user_reads`.
3. Add the `user1` to the `user_deletes`
3. Submit the transaction payload by issuing a `POST /user {txPayload}`

> Within a single transaction, we can delete more than a single user. For simplicity, the example delete a single user only.

```json
 curl \
   -H "Content-Type: application/json" \
   -X POST http://127.0.0.1:6001/user \
   --data '{
	"payload": {
		"userID": "admin",
		"txID": "1b6d6414-9b58-45d0-9723-1f31712add03",
        "user_reads": [
			{
				"userID": "user1",
				"version": {
					"block_num": 19,
					"tx_num": 0
				}
			}
		],
		"user_deletes": [
			{
				"userID": "user1"
			}
		]
	},
    "signature": "aGVsbG8="
}'
```

## Addition/Updation/Deletion of a User

Within a single transaction, we can add a new user, update or delete an existing user. 

## Invalid User Administration Transaction

TODO (subsequent PR)