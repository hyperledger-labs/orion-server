# Running a 3-Node Orion Raft Cluster

This guide explains how to set up and run a 3-node Orion blockchain database cluster using Docker Compose and Raft consensus.

## Architecture Overview

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  orion-server1  │────▶│  orion-server2  │────▶│  orion-server3  │
│   Leader/Node   │◀────│  Follower/Node  │◀────│  Follower/Node  │
│                 │     │                 │     │                 │
│  Port 6001      │     │  Port 6002      │     │  Port 6003      │
│  Raft 7051      │     │  Raft 7052      │     │  Raft 7053      │
└─────────────────┘     └─────────────────┘     └─────────────────┘
         │                       │                       │
         └───────────────────────┴───────────────────────┘
                        Raft Consensus
                        (Ports 7050)
```

## Prerequisites

- Docker and Docker Compose
- Access to orion-server repository

## Step 1: Generate Crypto Materials 🔐

Generate certificates for CA, admin, and all 3 server nodes:

```bash
cd /path/to/orion-server

# Generate crypto materials
./scripts/cryptoGen3Nodes.sh deployment

# Verify creation
ls -la deployment/crypto/
# Should see: CA/, admin/, server1/, server2/, server3/
```

This creates:
- **CA/**: Root certificate authority
- **admin/**: Administrator user certificates
- **server1/**, **server2/**, **server3/**: Each node's certificates and keys

## Step 2: Create Ledger Directories 📁

Each node needs its own ledger directory:

```bash
mkdir -p ledger/node1
mkdir -p ledger/node2
mkdir -p ledger/node3
```

## Step 3: Start the Cluster 🚀

```bash
cd operations

# Start all 3 nodes
docker-compose -f docker-compose-3nodes.yml up -d

# Check status
docker-compose -f docker-compose-3nodes.yml ps

# View logs
docker-compose -f docker-compose-3nodes.yml logs -f
```

## Step 4: Verify Cluster Health ✅

### Check Individual Nodes

```bash
# Check node 1
curl http://localhost:6001/config/cluster | jq .

# Check node 2  
curl http://localhost:6002/config/cluster | jq .

# Check node 3
curl http://localhost:6003/config/cluster | jq .
```

All should return the same cluster configuration showing 3 nodes.

### Check Raft Leader

The logs will show which node is the Raft leader:

```bash
docker-compose -f docker-compose-3nodes.yml logs | grep -i "leader"
```

## Configuration Files Explained

### Shared Configuration (`3node-shared-config-bootstrap.yml`)

Defines the **cluster topology** - same for all nodes:

```yaml
nodes:
  - nodeId: orion-server1
    host: orion-server1
    port: 6001
  - nodeId: orion-server2
    host: orion-server2
    port: 6001
  - nodeId: orion-server3
    host: orion-server3
    port: 6001

consensus:
  algorithm: raft
  members:
    - nodeId: orion-server1
      raftId: 1
      peerHost: orion-server1
      peerPort: 7050
    - nodeId: orion-server2
      raftId: 2
      peerHost: orion-server2
      peerPort: 7050
    - nodeId: orion-server3
      raftId: 3
      peerHost: orion-server3
      peerPort: 7050
```

**Key Points:**
- `raftId` must be > 0 for consensus members
- `peerHost` uses Docker service names for networking
- All nodes see the same cluster topology

### Local Configurations (`config-server1.yml`, `config-server2.yml`, `config-server3.yml`)

Each node has its **own identity** and **storage paths**:

```yaml
server:
  identity:
    id: orion-server1  # Different for each node
    certificatePath: /etc/orion-server/crypto/server1/server1.pem
    keyPath: /etc/orion-server/crypto/server1/server1.key
  
bootstrap:
  method: genesis
  file: /etc/orion-server/config/3node-shared-config-bootstrap.yml  # Same file
```

## Port Mapping

| Node | Client API (Host) | Client API (Container) | Raft (Host) | Raft (Container) |
|------|-------------------|------------------------|-------------|------------------|
| Node 1 | 6001 | 6001 | 7051 | 7050 |
| Node 2 | 6002 | 6001 | 7052 | 7050 |
| Node 3 | 6003 | 6001 | 7053 | 7050 |

## Testing the Cluster

### Create Databases

Connect to any node (transactions are replicated to all nodes):

```bash
# Create databases via node 1
node ../scripts/submitTx.js \
  --url http://localhost:6001 \
  --create-dbs db1,db2
```

### Verify Replication

Check that databases exist on all nodes:

```bash
# Query node 1
curl -H "UserID: admin" -H "Signature: ..." http://localhost:6001/db/db1

# Query node 2
curl -H "UserID: admin" -H "Signature: ..." http://localhost:6002/db/db1

# Query node 3
curl -H "UserID: admin" -H "Signature: ..." http://localhost:6003/db/db1
```

All should return the same data!

## Raft Consensus Behavior

### Leader Election

- One node is elected as **leader**
- Leader handles all writes
- Followers replicate from leader
- If leader fails, new election occurs

### Write Process

1. Client sends transaction to any node
2. If follower, forwards to leader
3. Leader proposes to Raft
4. Majority (2 of 3) must accept
5. Leader commits and notifies followers
6. All nodes apply to state

### Fault Tolerance

- **1 node failure**: Cluster continues (2/3 quorum)
- **2 node failures**: Cluster stops (no quorum)

## Monitoring

### Check Cluster Status

```bash
curl http://localhost:6001/config/cluster | jq '.response.nodes'
```

### View Node Logs

```bash
# All nodes
docker-compose -f docker-compose-3nodes.yml logs -f

# Specific node
docker-compose -f docker-compose-3nodes.yml logs -f orion-server1
```

### Check Raft State

Look for these log messages:
- `became leader` - Node elected as leader
- `became follower` - Node following leader
- `heartbeat` - Leader sending heartbeats

## Stopping the Cluster

```bash
# Stop all nodes
docker-compose -f docker-compose-3nodes.yml down

# Stop and remove volumes
docker-compose -f docker-compose-3nodes.yml down -v
```

## Troubleshooting

### Nodes Can't Connect

**Symptom**: Logs show connection refused errors

**Solution**: Ensure all nodes are on the same Docker network:
```bash
docker network inspect operations_orion-network
```

### Genesis Block Mismatch

**Symptom**: "genesis block mismatch" error

**Solution**: 
1. Stop all nodes
2. Clear ledger directories:
   ```bash
   rm -rf ledger/node*
   mkdir -p ledger/node{1,2,3}
   ```
3. Restart cluster

### No Quorum

**Symptom**: Cluster won't commit transactions

**Solution**: Ensure at least 2 nodes are running:
```bash
docker-compose -f docker-compose-3nodes.yml ps
```

### Certificate Errors

**Symptom**: "certificate verification failed"

**Solution**: Regenerate crypto materials:
```bash
./scripts/cryptoGen3Nodes.sh deployment
```

## Adding More Nodes

To expand to 5 nodes:

1. Update `3node-shared-config-bootstrap.yml` to add nodes 4 and 5
2. Create `config-server4.yml` and `config-server5.yml`
3. Generate certificates for server4 and server5
4. Add services to `docker-compose-3nodes.yml`
5. **Important**: Use configuration transactions to add nodes to a running cluster (don't recreate genesis block)

## Important Notes

⚠️ **Genesis Block**: All nodes must start with the same shared configuration for the genesis block

⚠️ **Raft IDs**: Must be unique integers > 0 for each member

⚠️ **Hostnames**: Use Docker service names (orion-server1, orion-server2, etc.) for inter-node communication

⚠️ **Bootstrap Method**: All nodes use `method: genesis` when starting fresh. For adding nodes to existing cluster, use `method: join`

## References

- [Orion Documentation](https://hyperledger-labs.github.io/orion-server/)
- [Raft Consensus Algorithm](https://raft.github.io/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
