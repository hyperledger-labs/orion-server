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
  "error": "signature verification failed"
}
```

## Error Due to Access Control Permission

TODO

## Error Due to In-Correct Signature

TODO
