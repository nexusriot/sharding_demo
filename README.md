# Distributed Sharding & Replication (with Health Checks)



-   Client → Router → Replicas flow
-   Modulo sharding w/ R replicas
-   Consistent hashing w/ vnodes & R replicas
-   Health-based routing
-   End-to-end PUT/GET flows

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

-   `put` → write only to healthy replicas (warn if \<R)
-   `get` → try healthy nodes until value found

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

## Next Improvements

-   Quorums (W/R)
-   Read repair
-   Merkle anti-entropy
-   Gossip for health
