This folder contains the crypto materials, including root CA private key, self-signed root CA certificate, and private keys and certificates per user.

### creating private keys and certificates and using them in configuration
Run from `orion-server` root folder:

Run `./scripts/cryptoGen.sh deployment [args]`,
replace `[args]` with any optional user.


Example:

Create keys and certs for CA, admin, server, user, alice anb bob by: `./scripts/cryptoGen.sh deployment alice bob`

The generated crypto materials are stored inside deployment.