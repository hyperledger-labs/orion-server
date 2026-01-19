#!/usr/bin/env node

/**
 * Orion Transaction Signer
 * 
 * Signs transaction payloads using ECDSA (prime256v1/P-256) with SHA-256 hash,
 * matching the behavior of the Go signer in cmd/signer/signer.go
 * 
 * Usage:
 *   node signTx.js -privatekey=<path> -data='<json>'
 * 
 * Example:
 *   node signTx.js -privatekey=deployment/crypto/admin/admin.key \
 *     -data='{"user_id":"admin","tx_id":"1b6d6414-9b58-45d0-9723-1f31712add71","create_dbs":["db1","db2"]}'
 */

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

/**
 * Parse command line arguments
 */
function parseArgs() {
  const args = {
    privatekey: null,
    data: null
  };

  for (let i = 2; i < process.argv.length; i++) {
    const arg = process.argv[i];
    
    if (arg.startsWith('-privatekey=')) {
      args.privatekey = arg.substring('-privatekey='.length);
    } else if (arg.startsWith('-data=')) {
      args.data = arg.substring('-data='.length);
    } else if (arg === '-h' || arg === '--help') {
      printHelp();
      process.exit(0);
    }
  }

  return args;
}

/**
 * Print help message
 */
function printHelp() {
  console.log(`Orion Transaction Signer

All the following two flags must be set. An example command is shown below:

  signTx.js -data='{"userID":"admin"}' -privatekey=admin.key

Flags:
  -privatekey string
        path to the private key to be used for adding a digital signature
  -data string
        json data to be signed. Surround that data with single quotes.
        An example json data is '{"userID":"admin"}'
  -h, --help
        show this help message

Example:
  node signTx.js -privatekey=deployment/crypto/admin/admin.key \\
    -data='{"user_id":"admin","tx_id":"unique-id","create_dbs":["db1","db2"]}'
`);
}

/**
 * Load PEM private key from file
 * Supports both PKCS#8 and SEC1 EC formats (same as Go implementation)
 */
function loadPrivateKey(keyPath) {
  try {
    const keyData = fs.readFileSync(keyPath, 'utf8');
    
    // Node.js crypto.createPrivateKey handles both PKCS#8 and SEC1 formats automatically
    const privateKey = crypto.createPrivateKey({
      key: keyData,
      format: 'pem'
    });

    return privateKey;
  } catch (error) {
    console.error(`Error loading private key from ${keyPath}:`, error.message);
    process.exit(1);
  }
}

/**
 * Sign data using ECDSA with SHA-256
 * This matches the Go implementation in pkg/crypto/signer.go:
 * 1. Compute SHA-256 hash of the data
 * 2. Sign the hash using ECDSA private key
 * 3. Return base64-encoded signature
 */
function signData(privateKey, data) {
  try {
    // Create a Sign object with SHA-256
    const sign = crypto.createSign('SHA256');
    
    // Update with the data to be signed
    sign.update(data);
    sign.end();
    
    // Sign and return as base64
    // The signature format is DER-encoded ASN.1 (same as Go's default ECDSA signature format)
    const signature = sign.sign(privateKey);
    
    return signature.toString('base64');
  } catch (error) {
    console.error('Error signing data:', error.message);
    process.exit(1);
  }
}

/**
 * Validate JSON data
 */
function validateJSON(data) {
  try {
    JSON.parse(data);
    return true;
  } catch (error) {
    console.error('Error: Invalid JSON data:', error.message);
    return false;
  }
}

/**
 * Main function
 */
function main() {
  const args = parseArgs();

  // Validate arguments
  if (!args.privatekey || !args.data) {
    console.error('Error: Both -privatekey and -data flags are required\n');
    printHelp();
    process.exit(1);
  }

  // Resolve relative paths
  const keyPath = path.resolve(args.privatekey);

  // Check if key file exists
  if (!fs.existsSync(keyPath)) {
    console.error(`Error: Private key file not found: ${keyPath}`);
    process.exit(1);
  }

  // Validate JSON
  if (!validateJSON(args.data)) {
    process.exit(1);
  }

  // Load private key
  const privateKey = loadPrivateKey(keyPath);

  // Sign the data
  const signature = signData(privateKey, args.data);

  // Output signature (mimics Go signer which just prints to stdout)
  process.stdout.write(signature);
}

// Run main function
if (require.main === module) {
  main();
}

// Export for testing
module.exports = { loadPrivateKey, signData, validateJSON };
