---
id: crypto-materials
title: Creating Crypto Material
---

<!--
 Copyright IBM Corp. All Rights Reserved.

 SPDX-License-Identifier: CC-BY-4.0
 -->

### Crypto materials configuration

Hyperledger Orion uses public key cryptography to achieve authentication and transaction non-repudiation. This is true for both the client and the server. Therefore, each client signs the queries and transactions it submits to the server, and each server signs the responses it sends to the client.

Thus, each Orion client needs a certificate/key pair to sign each query and transaction it submits to the server, and the server has to have its own certificate/key pair to sign each response to the client.

In order to validate client signatures, the Orion server has to keep the certificate of every client. This is achieved by the process of user on-boarding [see here](../transactions/curl/usertx). Conversely, in order to validate the server's signature on responses, each client has to have access to the server certificate. 
This is achieved by reading the ClusterConfig.Nodes [see here](../queries/curl/node-config). Both server and client need access to a CA certificate chain to validate client/server (resp.) certificates.

So, to start a new Orion server, each server has to have the following:
1. Root CA certificate (we support multiple CAs, including multilevel CAs).
   * `caConfig.rootCACertsPath` 
2. Its own certificate, sign by a CA from above and a corresponding private key.
   * `server.identity.certificatePath` and `server.identity.keyPath`
3. Default administrative user certificate, usually denoted as `admin` certificate.
   * `admin.certificatePath`

To work with Orion server, each client needs:
1. Root CA certificate.
```go 
&config.ConnectionConfig{     
   RootCAs: []string{
      "./crypto/CA/CA.pem",
   },
}
```
2. Its own certificate, sign by the root CA from above and the corresponding private key.
```go
&config.SessionConfig{
	UserConfig: &config.UserConfig{
		UserID:         userID,
		CertPath:       "./crypto/" + userID + "/" + userID + ".pem",
		PrivateKeyPath: "./crypto/" + userID + "/" + userID + ".key",
	},
}
```
3. A server certificate that can be accessed using the node config query [here](../queries/curl/node-config).

> For now, we ignore TLS configuration and the location of TLS certificates, for both servers and clients.

### Crypto materials generation
To generate new crypto materials, we use `scripts/cryptoGen.sh`. The first argument is the absolute path to the folder used to store the generated files.

Running it with only the first argument will generate a minimal set of crypto materials, including three sets of certificates or in total four certificates.

1. `admin` and `user` - for db users
2. `server` - for server node
3. CA - for Certificate Authority 

To create a minimal set of cryptographic materials, you can run:
`./scripts/cryptoGen.sh deployment`

This run will generate four folders under `deployment/crypto`:
1. CA
2. server
3. admin
4. user

If you need more certificates (for extra client or extra server nodes), they can be specified as extra arguments: 

`./scripts/cryptoGen.sh deployment <extra users>`

For example:

`./scripts/cryptoGen.sh deployment alice bob`


 