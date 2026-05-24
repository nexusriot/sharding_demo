# Distributed Sharding & Replication (with Health Checks + Quorums + Merkle Anti-Entropy)
**Disclaimer:** This is a PoC demo — just for fun ;)

- Client → Router → Replicas flow  
- Modulo sharding w/ R replicas  
- Consistent hashing w/ vnodes & R replicas  
- Health-based routing  
- End-to-end PUT/GET flows  
- Quorums (W/R)  
- Merkle Anti-Entropy

## Implementations

Two parallel ports of the same model live in this repo. Pick whichever you
prefer — the algorithms and on-screen output are intentionally equivalent.

| | Python | Go |
|---|---|---|
| Demo (non-interactive) | [`demo.py`](demo.py) | [`go/cmd/demo`](go/cmd/demo/main.go) |
| Live TUI dashboard     | [`tui.py`](tui.py) (Rich) | [`go/cmd/tui`](go/cmd/tui/main.go) (Bubbletea) |
| Dependencies           | `rich` only                | `bubbletea`, `lipgloss` |
| Run                    | `python3 demo.py` / `python3 tui.py` | `cd go && make demo` / `make tui` |
| Tests                  | —                          | `cd go && make test` (17 unit tests + benchmarks) |
| Install                | —                          | `cd go && make deb && sudo dpkg -i dist/*.deb` |

The TUI shows the consistent-hash ring, per-node health and key counts, a
live primary-distribution bar chart, and a step-by-step trace of which
nodes are visited when a key is routed. See [`go/README.md`](go/README.md)
for a guided walkthrough of the panels and hotkeys.

## High-Level Architecture

    +---------+        +----------------------+        +----------------------------+
    | Client  | -----> |  Router (strategy)   | -----> |  Replica set for key `K`   |
    +---------+        |  - modulo OR CH ring |        |  R distinct healthy nodes  |
                       |  - health-aware      |        +----------------------------+
                       |  - pick_replicas(K,R)|        e.g. ["n3","n0","n2"] (R=3)
                       +----------------------+

## Modulo Sharding w/ Replication (R=3)

### Key Placement

    hash(K) -> H
    index0 = H % N

    Nodes (N=6):       [ n0  n1  n2  n3  n4  n5 ]
    Health:              ✓   ✗   ✓   ✓   ✓   ✓

Walk forward to collect **R healthy distinct nodes**:

    Start idx = H % 6 = 1 -> n1
     step0: n1 ✗ (down) -> skip
     step1: n2 ✓       -> pick #1 (primary)
     step2: n3 ✓       -> pick #2
     step3: n4 ✓       -> pick #3

    Replica set = [n2, n3, n4]

### PUT Flow

    Client.put(K,V)
      ↓
    Router.pick_replicas(K,3)
      ↓
    [n2, n3, n4]
      ↓  ↓  ↓
     write to each healthy replica

Warn if less than R:

    [WARN] only 2/3 healthy replicas available

### GET Flow

    Client.get(K)
      ↓
    pick_replicas → [n2, n3, n4]
      ↓
    try n2.get(K) → if None try next → return first hit

## Consistent Hashing (CH) Ring w/ Virtual Nodes & R Replication

### Ring View

                       (hash space)
            ┌──────────────────────────────────────────┐
            │   • n0#17      • n1#02                   │
            │           \                              │
            │            \       • n3#55               │
            │   • n2#08   \                             │
            │              \     • n0#63               │
            │               \                           │
            │                \     • n2#71             │
            │                 \                         │
            │                  \     • n1#88           │
            │                   \                       │
            └─────────────• K=hash("K")────────────────┘

Walk clockwise from K:

    n3 (✓) → pick #1
    n0 (✓) → pick #2
    n2 (✓) → pick #3

    Replica set = [n3, n0, n2]

If `n0` is down:

    n3 (✓) → pick
    n0 (✗) → skip
    n2 (✓) → pick
    n1 (✓) → pick

    Replica set = [n3, n2, n1]

> **vnodes** = smoother distribution + easier scaling.

## Health Checks

       [Probe/Circuit-breaker]
                  ↓
     Router.set_health("n2", False)
                  ↓
     Routing immediately skips n2

Effects:

- `put` → write only to healthy replicas (warn if <R)  
- `get` → try healthy nodes until value found  

## End-to-End Sequences

### PUT (R=3, CH)

    Client.put(K,V)
       ↓
    hash(K) → ring position
       ↓
    pick_replicas → [n3, n0, n2]
       ↓
    write → n3, n0, n2

### GET (R=3, modulo)

    Client.get(K)
      ↓
    pick_replicas → [n2, n3, n4]
      ↓
    n2.get(K) → returns first hit

## Mental Model

                ┌──────────────┐
                │   Hashing    │  -> find key position
                └──────┬───────┘
                       │
            ┌──────────▼──────────┐
            │   Node Selection    │  -> modulo or CH ring
            └──────────┬──────────┘
                       │
            ┌──────────▼──────────┐
            │   Replication (R)   │  -> choose R healthy nodes
            └──────────┬──────────┘
                       │
            ┌──────────▼──────────┐
            │ Health Awareness    │  -> skip unhealthy nodes
            └─────────────────────┘

# Quorums (W/R)

Quorums extend replication with stronger consistency guarantees.

### Definitions

- **N** = replication factor  
- **W** = write quorum (minimum acknowledgements)  
- **R** = read quorum (minimum successful reads)

Guarantee:

    If R + W > N → system prevents stale reads.

### Versioned Writes

Values are stored as:

    (value, version)

Replicas overwrite only when:

    incoming_version >= current_version

### Quorum Write (W)

    PUT(K,V)
       ↓
    pick_replicas(N)
       ↓
    send write(version)
       ↓
    if ACKs >= W → success
    else → failure

### Quorum Read (R) + Read Repair

    GET(K)
       ↓
    pick_replicas(N)
       ↓
    collect responses
       ↓
    if <R responses → failure
    else pick newest version
       ↓
    read repair: update stale replicas

### Summary

| Feature | Basic Replication | With Quorums |
|--------|--------------------|--------------|
| Write | best-effort | must meet W |
| Read | first-hit | must meet R |
| Consistency | eventual | strong if R+W>N |
| Repair | none | automatic read repair |

# Merkle Anti-Entropy

Even with replication + quorums, replicas may drift over time due to failures, partitions or missed updates.

**Merkle Trees** provide efficient background reconciliation:

- Each replica builds a hash tree over `(key, value, version)`  
- Roots are compared  
- If roots match → replicas identical  
- If not → compare subtrees or leaves (simplified in this PoC)  
- Only keys that actually diverge are exchanged  
- The version with the highest timestamp wins  

This allows **full replica convergence** with very low bandwidth.

### Merkle Repair Flow

1. Each replica builds a Merkle tree using the same key ordering  
2. Compare root hashes  
3. If different → identify differing leaves  
4. Sync only those keys  
5. Mutate stale replicas to the newest version  
6. After sync, all nodes converge


## Next Improvements

- Quorums (W/R) ✔️  
- Merkle anti-entropy ✔️  
- Read repair  
- Gossip for health  
- Tunable consistency per request  
- Background repair scheduler  
- Partition healing strategies  