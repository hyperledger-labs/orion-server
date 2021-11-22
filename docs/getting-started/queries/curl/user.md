---
id: user
title: Query an User Information
---
## Querying the User Information

One user can query information about another user or self. If the access control is defined for the user entry, it would be enforced during the query.
A user information can be retrieved by issuing a `GET` request on `/user/{userid}` endpoint where `{userid}` should be replaced with the ID of the user
whom information needs to be fetched.

Here, the submitting user needs to sign the `{"user_id":"<submitting_user_id>","target_user_id":"<target_user_id"}` where `<submitting_user_id>` denotes the
ID of the user who is submitting the query and `<target_user_id>` denotes the ID of the user whom information needs to be fetched.

In our `config.yml`, we have an admin entry with ID `admin`. Hence, we do a self query.
In our example, the submitting user `admin` wants to fetch its own information and hence, the following data needs to be signer
`{"user_id":"admin","target_user_id":"admin"}` as follows:

```shell
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key -data='{"user_id":"admin","target_user_id":"admin"}'
```
The above command would produce a digital signature and prints it as base64 encoded string as shown below:
```shell
MEQCIB6NmOvgUvYwRLBAFz/OovrGRfhnwbxMwFb4+Dniano6AiBbObFcC6Cv/wcBLXzDf3pG0zybHTSA5ksMGCusfAridw==
```
Once the signature is computed, we can issue a `GET` request using the following `cURL` command
by setting the above signature string in the `Signature` header.
```shell
curl \
   -H "Content-Type: application/json" \
   -H "UserID: admin" \
   -H "Signature: MEQCIB6NmOvgUvYwRLBAFz/OovrGRfhnwbxMwFb4+Dniano6AiBbObFcC6Cv/wcBLXzDf3pG0zybHTSA5ksMGCusfAridw==" \
   -X GET http://127.0.0.1:6001/user/admin | jq .
```
A sample output of the above command is shown below.
```webmanifest
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "user": {
      "id": "admin",
      "certificate": "MIIBsTCCAVigAwIBAgIRANDg6u+E+sdowdPBGXyoD9swCgYIKoZIzj0EAwIwHjEcMBoGA1UEAxMTQ2FyIHJlZ2lzdHJ5IFJvb3RDQTAeFw0yMTA2MTYxMTEzMjdaFw0yMjA2MTYxMTE4MjdaMCQxIjAgBgNVBAMTGUNhciByZWdpc3RyeSBDbGllbnQgYWRtaW4wWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAT9GYTj7ka7se8bEqclDdulRJnpay+EU/b4QUMiLl/9cB1iBgKegVXztZCcQs3S+XR1Y2b/1xSqMbPwHOV5kuUDo3EwbzAOBgNVHQ8BAf8EBAMCBaAwHQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAU7nVzp7gto++BPlj5KAF1IA62TNEwDwYDVR0RBAgwBocEfwAAATAKBggqhkjOPQQDAgNHADBEAiB09qz5bAyguEEI4HMUPIBqRTF3RBMjfTKrpIjBh1ai9gIgWLL0SNsD/5a4xjp+fol42npUY13mIpByYg56IW0TsBI=",
      "privilege": {
        "admin": true
      }
    },
    "metadata": {
      "version": {
        "block_num": 1
      }
    }
  },
  "signature": "MEUCIQDN7DAo/2drpnQYB9lonKcqNQRjRooARhw1D+aSZgOhyAIgbAbofC016zyBcccu6BRfwSrYDbQnekbSu9lJVR1FCOw="
}
```
