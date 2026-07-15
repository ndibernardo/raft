# raft-rs

A Raft consensus implementation in Rust, designed as the foundation for a distributed key-value store.

## Overview

This library implements the Raft consensus algorithm as described in "In Search of an Understandable Consensus Algorithm" by Ongaro and Ousterhout.
The Raft layer is generic over command type. The key-value store is implemented as a state machine that applies committed log entries.

## Running a cluster

Build the release binary:

```bash
cargo build --release
```

Start three nodes in separate terminals. Each node needs a unique `--id`, its own `--addr`, and one `--peer` flag per other cluster member. Pass `--client-addr` to enable the HTTP API on that node.

**Node 1**
```bash
./target/release/raft \
  --id 1 \
  --addr 127.0.0.1:9001 \
  --peer 2=127.0.0.1:9002 \
  --peer 3=127.0.0.1:9003 \
  --data-dir ./data/node1 \
  --client-addr 127.0.0.1:8001
```

**Node 2**
```bash
./target/release/raft \
  --id 2 \
  --addr 127.0.0.1:9002 \
  --peer 1=127.0.0.1:9001 \
  --peer 3=127.0.0.1:9003 \
  --data-dir ./data/node2 \
  --client-addr 127.0.0.1:8002
```

**Node 3**
```bash
./target/release/raft \
  --id 3 \
  --addr 127.0.0.1:9003 \
  --peer 1=127.0.0.1:9001 \
  --peer 2=127.0.0.1:9002 \
  --data-dir ./data/node3 \
  --client-addr 127.0.0.1:8003
```

Each node prints its listen addresses to stderr on startup. The cluster elects a leader automatically once a majority of nodes are reachable. Persistent state (term, vote, log) is stored in the `--data-dir` directory and survives restarts.

## Client API

Send commands to any node's client API. Writes and membership changes are accepted only by the leader — non-leaders return `503 not the leader`. Only one membership change may be in flight at a time; concurrent requests return `409`.

```bash
# Store a value
curl -X PUT http://127.0.0.1:8001/kv/db-primary -d "10.0.0.1:5432"   # ok

# Read it back
curl http://127.0.0.1:8001/kv/db-primary                             # 10.0.0.1:5432

# Failover: point to the new primary
curl -X PUT http://127.0.0.1:8001/kv/db-primary -d "10.0.0.2:5432"   # ok
curl http://127.0.0.1:8001/kv/db-primary                             # 10.0.0.2:5432

# Remove the key
curl -X DELETE http://127.0.0.1:8001/kv/db-primary                   # ok
curl http://127.0.0.1:8001/kv/db-primary                             # 404

# Writes to a non-leader are rejected
curl -X PUT http://127.0.0.1:8002/kv/db-primary -d "10.0.0.1:5432"   # 503 not the leader

# Add a node; blocks until committed by a quorum
curl -X POST http://127.0.0.1:8001/cluster/members \
  -H "Content-Type: application/json" \
  -d '{"id": 4, "addr": "127.0.0.1:9004"}'                           # ok

# Remove a node
curl -X DELETE http://127.0.0.1:8001/cluster/members/4               # ok
```

## Testing

```bash
cargo test
```

Unit tests in `src/` cover node protocol logic, storage, state machine, and transport. Property tests in `tests/common/cluster.rs` run randomised operation sequences verified against election-safety and state-machine-safety invariants. Integration tests in `tests/` exercise end-to-end scenarios (election, replication, commit propagation, re-election) through the public API only.

## To be implemented

The core protocol — leader election, log replication, the safety rules, persistence, and single-server membership changes — is complete. Still missing from the paper and dissertation:

- Log compaction and snapshotting (paper section 7, dissertation chapter 5)
- Read-only queries without log writes (dissertation section 6.4)
- Client sessions and request de-duplication (dissertation section 6.3)
- Pre-vote (dissertation section 9.6)
- Leader self-removal step-down (dissertation section 4.2.2)
- Joint consensus (paper section 6)
- Accelerated log backtracking (paper section 5.3)

## References

- [Raft Paper](https://raft.github.io/raft.pdf)
- [Raft PhD Dissertation](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
- [TLA+ Specification](https://github.com/ongardie/raft.tla)
