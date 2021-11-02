---
id: db
title: Check the Existance of a Database
---
## Checking the Database Existance

To check whether a database exist/created, the user can issue a GET request on `/db/{dbname}` endpoint where `{dbname}` should be replaced with
the `dbname` for which the user needs to perform this check.

For this query, the submitting user needs to sign `{"user_id":"<userid","db_name":"<dbname>"}` where `userid` denotes the submitting user and the
`<dbname>` denotes the name of the database for which the user performs the existance check. 

When the BDB server bootups, it creates a default database called `bdb` in the cluster. Hence, we can check its existance. For this case, the
submitting user `admin` needs to sign `{"user_id":"admin","db_name":"bdb"}` as shown below:

```sh
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key -data='{"user_id":"admin","db_name":"bdb"}'
```
The above command would produce a digital signature and prints it as base64 encoded string as shown below:
```sh
MEUCIBzH0qIz88jKdHsJvmQsNNuK3Cf0G+7LDWSiwv6yjba0AiEAgb/hBFZrr3w64M0Q6LmZjQ0i/sjYr27K1DJSlXHWfRU=
```

Once the signature is computed, we can issue a `GET` request using the following `cURL` command
by setting the above signature string in the `Signature` header.
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
  "response": {
    "header": {
      "node_id": "bdb-node-1"
. 

},
    "exist": true
  },
  "signature": "MEQCIAhSD5eQ+lBCaN7C/fILXcHADekGi+1RteDLmBbgHS4sAiAU+h/uwp/CrKmRdgHeAN7wOArRj5BdPC4Qp8Mzw4uIaQ=="
}
```

