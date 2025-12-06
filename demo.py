#!/usr/bin/env python3
from __future__ import annotations
import hashlib
import bisect
import random
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Iterable, Optional, Set


# ------------------------------ Storage node ------------------------------

@dataclass
class Node:
    name: str
    store: Dict[str, str] = field(default_factory=dict)
    healthy: bool = True  # health flag

    def put(self, k: str, v: str) -> None:
        self.store[k] = v

    def get(self, k: str) -> Optional[str]:
        return self.store.get(k)

    def delete(self, k: str) -> None:
        self.store.pop(k, None)


# ------------------------------ Modulo sharding w/ replication ------------------------------

class ModuloRouter:
    """Shard by hash(key) % N, with replication and health awareness."""
    def __init__(self, nodes: List[Node]) -> None:
        self.nodes = nodes

    @staticmethod
    def _h(key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def _alive_nodes(self) -> List[Node]:
        return [n for n in self.nodes if n.healthy]

    def pick(self, key: str) -> Node:
        alive = self._alive_nodes()
        if not alive:
            raise RuntimeError("No healthy nodes available")
        h = self._h(key)
        # try offsets until we land on a healthy node
        for off in range(len(self.nodes)):
            n = self.nodes[(h + off) % len(self.nodes)]
            if n.healthy:
                return n
        return alive[0]  # fallback (shouldn't happen)

    def pick_replicas(self, key: str, r: int) -> List[Node]:
        if r <= 0:
            return []
        if not any(n.healthy for n in self.nodes):
            return []
        h = self._h(key)
        chosen: List[Node] = []
        seen: Set[str] = set()
        i = 0
        while len(chosen) < r and i < len(self.nodes) * 2:  # bounded
            n = self.nodes[(h + i) % len(self.nodes)]
            if n.healthy and n.name not in seen:
                seen.add(n.name)
                chosen.append(n)
            i += 1
        return chosen

    def add_node(self, node: Node) -> None:
        self.nodes.append(node)

    def remove_node(self, name: str) -> None:
        self.nodes = [n for n in self.nodes if n.name != name]

    def set_health(self, name: str, healthy: bool) -> None:
        for n in self.nodes:
            if n.name == name:
                n.healthy = healthy


# ------------------------------ Consistent hashing (vnodes) w/ replication ------------------------------

class ConsistentHashRouter:
    """
    Consistent hashing with virtual nodes, replication (R), and health awareness.
    Each physical node has `vnodes` points on the ring; we pick replicas by walking clockwise
    and taking the first R *distinct healthy* physical nodes.
    """
    def __init__(self, nodes: Iterable[Node], vnodes: int = 128) -> None:
        self.vnodes = vnodes
        self.ring: List[Tuple[int, Node]] = []
        self.nodes_by_name: Dict[str, Node] = {}
        for n in nodes:
            self.nodes_by_name[n.name] = n
            self._add_phys(n)
        self.ring.sort(key=lambda x: x[0])

    @staticmethod
    def _h(s: str) -> int:
        return int(hashlib.sha1(s.encode()).hexdigest(), 16)

    def _add_phys(self, node: Node) -> None:
        for v in range(self.vnodes):
            h = self._h(f"{node.name}#{v}")
            self.ring.append((h, node))

    def add_node(self, node: Node) -> None:
        self.nodes_by_name[node.name] = node
        for v in range(self.vnodes):
            h = self._h(f"{node.name}#{v}")
            bisect.insort(self.ring, (h, node))

    def remove_node(self, name: str) -> None:
        self.nodes_by_name.pop(name, None)
        self.ring = [(h, n) for (h, n) in self.ring if n.name != name]

    def set_health(self, name: str, healthy: bool) -> None:
        if name in self.nodes_by_name:
            self.nodes_by_name[name].healthy = healthy

    def pick(self, key: str) -> Node:
        # first healthy replica
        reps = self.pick_replicas(key, 1)
        if not reps:
            raise RuntimeError("No healthy nodes available")
        return reps[0]

    def pick_replicas(self, key: str, r: int) -> List[Node]:
        if r <= 0 or not self.ring:
            return []
        h = self._h(key)
        i = bisect.bisect_left(self.ring, (h, None))  # type: ignore[arg-type]
        chosen: List[Node] = []
        seen: Set[str] = set()
        steps = 0
        # Walk clockwise around the ring until we have R distinct healthy nodes
        while len(chosen) < r and steps < len(self.ring) + self.vnodes:
            if i >= len(self.ring):
                i = 0
            node = self.ring[i][1]
            if node.healthy and node.name not in seen:
                chosen.append(node)
                seen.add(node.name)
            i += 1
            steps += 1
        return chosen


# ------------------------------ Client facade with replication & health ------------------------------

class KVClient:
    def __init__(self, router, replication_factor: int = 3) -> None:
        self.router = router
        self.R = replication_factor

    # Health control passthroughs (optional)
    def set_health(self, name: str, healthy: bool) -> None:
        # Works for either router type
        if hasattr(self.router, "set_health"):
            self.router.set_health(name, healthy)

    def put(self, k: str, v: str) -> None:
        replicas: List[Node]
        if hasattr(self.router, "pick_replicas"):
            replicas = self.router.pick_replicas(k, self.R)
        else:
            replicas = [self.router.pick(k)]
        if len(replicas) < self.R:
            print(f"[WARN] only {len(replicas)}/{self.R} healthy replicas available for key={k}")
        for n in replicas:
            n.put(k, v)

    def get(self, k: str) -> Optional[str]:
        replicas: List[Node]
        if hasattr(self.router, "pick_replicas"):
            replicas = self.router.pick_replicas(k, self.R)
        else:
            replicas = [self.router.pick(k)]
        for n in replicas:
            val = n.get(k)
            if val is not None:
                return val
        return None

    def delete(self, k: str) -> None:
        replicas: List[Node]
        if hasattr(self.router, "pick_replicas"):
            replicas = self.router.pick_replicas(k, self.R)
        else:
            replicas = [self.router.pick(k)]
        for n in replicas:
            n.delete(k)


# ------------------------------ Demo & utilities ------------------------------

def distribution(keys: List[str], replicas_func, label: str) -> Dict[str, int]:
    buckets = defaultdict(int)
    for k in keys:
        # count only the primary (first replica) for distribution
        reps = replicas_func(k, 1)
        if reps:
            buckets[reps[0].name] += 1
    print(f"\n{label} (primary) distribution:")
    for name, cnt in sorted(buckets.items()):
        print(f"  {name:<8} {cnt:6d}")
    return dict(buckets)

def replica_layout(keys: List[str], replicas_func, r: int, limit: int = 5) -> None:
    print(f"\nReplica layout for first {limit} keys (R={r}):")
    for k in keys[:limit]:
        names = [n.name for n in replicas_func(k, r)]
        print(f"  {k:<20} -> {names}")

def demo():
    print("Modulo vs Consistent Hash no quorum")

    # Nodes
    nodes_mod = [Node(f"n{i}") for i in range(4)]
    nodes_ch = [Node(f"n{i}") for i in range(4)]

    # Keyset
    random.seed(42) # the answer to life the universe and everything
    keys = [f"k{i}-{random.getrandbits(32)}" for i in range(5_000)]

    # ---------- Modulo + replication ----------
    mod = ModuloRouter(nodes_mod)
    client_mod = KVClient(mod, replication_factor=3)

    for k in keys[:2000]:
        client_mod.put(k, f"V:{k}")

    distribution(keys, mod.pick_replicas, "Modulo (R=3)")
    replica_layout(keys, mod.pick_replicas, r=3, limit=3)

    # Simulate a failure
    client_mod.set_health("n2", False)
    print("\n[Modulo] Marked n2 unhealthy")
    distribution(keys, mod.pick_replicas, "Modulo after n2 down (R=3)")
    sample = keys[0]
    print(f"GET {sample} -> {client_mod.get(sample)}")

    # ---------- Consistent Hash + replication ----------
    ch = ConsistentHashRouter(nodes_ch, vnodes=128)
    client_ch = KVClient(ch, replication_factor=3)
    for k in keys:
        client_ch.put(k, f"V:{k}")

    distribution(keys, ch.pick_replicas, "ConsistentHash (R=3)")
    replica_layout(keys, ch.pick_replicas, r=3, limit=3)

    # Simulate a failure
    client_ch.set_health("n1", False)
    print("\n[CH] Marked n1 unhealthy")
    distribution(keys, ch.pick_replicas, "ConsistentHash after n1 down (R=3)")

    # Show read still works with node down
    s = keys[1]
    print(f"GET {s} -> {client_ch.get(s)}")

    # Bring node back
    client_ch.set_health("n1", True)
    print("\n[CH] Marked n1 healthy again")
    print(f"GET {s} -> {client_ch.get(s)}")


VersionedVal = Tuple[str, int]  # (value, version)

@dataclass
class QNode:
    """Versioned node for quorum and Merkle simulations."""
    name: str
    store: Dict[str, VersionedVal] = field(default_factory=dict)
    healthy: bool = True

    def put(self, k: str, v: str, version: int) -> None:
        cur = self.store.get(k)
        if cur is None or version >= cur[1]:
            self.store[k] = (v, version)

    def get(self, k: str) -> Optional[VersionedVal]:
        return self.store.get(k)

    def delete(self, k: str, version: int) -> None:
        cur = self.store.get(k)
        if cur is None or version >= cur[1]:
            self.store.pop(k, None)


class QuorumKVClient:
    """
    N = replication_factor
    R = read quorum
    W = write quorum
    """

    def __init__(self, router, replication_factor: int = 3,
                 read_quorum: int = 2, write_quorum: int = 2) -> None:
        self.router = router
        self.N = replication_factor
        self.R = read_quorum
        self.W = write_quorum
        self._version = 0

    def _next_version(self) -> int:
        self._version += 1
        return self._version

    def set_health(self, name: str, healthy: bool) -> None:
        if hasattr(self.router, "set_health"):
            self.router.set_health(name, healthy)

    def put(self, k: str, v: str) -> bool:
        replicas: List[QNode] = self.router.pick_replicas(k, self.N)  # type: ignore[assignment]
        if not replicas:
            print(f"[PUT] no replicas for key={k}")
            return False

        version = self._next_version()
        acks = 0

        for n in replicas:
            if not n.healthy:
                continue
            n.put(k, v, version)
            acks += 1

        if acks < self.W:
            print(f"[PUT] FAILED quorum for key={k}, acks={acks}, W={self.W}")
            return False

        if acks < self.N:
            print(f"[PUT] key={k} wrote to only {acks}/{self.N} replicas")

        return True

    def get(self, k: str) -> Optional[str]:
        replicas: List[QNode] = self.router.pick_replicas(k, self.N)  # type: ignore[assignment]
        if not replicas:
            print(f"[GET] no replicas for key={k}")
            return None

        responses: List[Tuple[str, int, QNode]] = []
        for n in replicas:
            if not n.healthy:
                continue
            res = n.get(k)
            if res is not None:
                val, ver = res
                responses.append((val, ver, n))

        if len(responses) < self.R:
            print(f"[GET] FAILED quorum for key={k}, responses={len(responses)}, R={self.R}")
            return None

        newest_val, newest_ver, _ = max(responses, key=lambda x: x[1])

        # read repair to stale replicas
        for n in replicas:
            if not n.healthy:
                continue
            cur = n.get(k)
            if cur is None or cur[1] < newest_ver:
                n.put(k, newest_val, newest_ver)

        return newest_val

    def delete(self, k: str) -> bool:
        replicas: List[QNode] = self.router.pick_replicas(k, self.N)  # type: ignore[assignment]
        if not replicas:
            print(f"[DEL] no replicas for key={k}")
            return False

        version = self._next_version()
        acks = 0
        for n in replicas:
            if not n.healthy:
                continue
            n.delete(k, version)
            acks += 1

        if acks < self.W:
            print(f"[DEL] FAILED quorum for key={k}, acks={acks}, W={self.W}")
            return False

        return True


def _hash_data(s: str) -> int:
    return int(hashlib.sha1(s.encode()).hexdigest(), 16)


class MerkleTree:
    """
    Simple Merkle tree over a fixed list of hashes (leaves).

    For this simulation:
    - All replicas build leaves in the same key order.
    - If two trees have equal root -> state identical for that keyset.
    - If roots differ -> we fall back to leaf comparison to find differing keys.
      (Not optimal, because I'm stupid to make optimal)
    """

    def __init__(self, leaves: List[int]) -> None:
        self.levels: List[List[int]] = []
        if not leaves:
            self.levels = [[0]]
            return
        self.levels.append(leaves)
        level = leaves
        while len(level) > 1:
            next_level: List[int] = []
            for i in range(0, len(level), 2):
                left = level[i]
                right = level[i + 1] if i + 1 < len(level) else left
                combined = _hash_data(f"{left}:{right}")
                next_level.append(combined)
            self.levels.append(next_level)
            level = next_level

    @classmethod
    def for_node(cls, node: QNode, keys: List[str]) -> "MerkleTree":
        leaves: List[int] = []
        for k in keys:
            rec = node.get(k)
            if rec is None:
                payload = f"{k}:<MISSING>"
            else:
                val, ver = rec
                payload = f"{k}:{val}:{ver}"
            leaves.append(_hash_data(payload))
        return cls(leaves)

    def root(self) -> int:
        return self.levels[-1][0] if self.levels else 0

    def diff_leaf_indices(self, other: "MerkleTree") -> List[int]:
        # If roots equal, no differences.
        if self.root() == other.root():
            return []
        # Otherwise, brute-force leaf comparison.
        a_leaves = self.levels[0]
        b_leaves = other.levels[0]
        diffs: List[int] = []
        for i, (ha, hb) in enumerate(zip(a_leaves, b_leaves)):
            if ha != hb:
                diffs.append(i)
        # If one side has extra leaves, mark them as diffs too
        if len(a_leaves) != len(b_leaves):
            longer = max(len(a_leaves), len(b_leaves))
            for i in range(min(len(a_leaves), len(b_leaves)), longer):
                diffs.append(i)
        return diffs


def run_anti_entropy(nodes: List[QNode], keys: List[str]) -> None:
    """
    Pairwise Merkle anti-entropy between all nodes.
    """
    print("\nMerkle: comparing replicas")
    total_repairs = 0
    for i in range(len(nodes)):
        for j in range(i + 1, len(nodes)):
            a = nodes[i]
            b = nodes[j]

            tree_a = MerkleTree.for_node(a, keys)
            tree_b = MerkleTree.for_node(b, keys)

            if tree_a.root() == tree_b.root():
                continue

            diff_idx = tree_a.diff_leaf_indices(tree_b)
            if not diff_idx:
                continue

            print(f"[AntiEntropy] {a.name} <-> {b.name}: {len(diff_idx)} differing keys")

            for idx in diff_idx:
                if idx >= len(keys):
                    continue
                k = keys[idx]
                va = a.get(k)
                vb = b.get(k)
                if va is None and vb is None:
                    continue
                if va is None:
                    v, ver = vb  # type: ignore[misc]
                    a.put(k, v, ver)
                    total_repairs += 1
                elif vb is None:
                    v, ver = va
                    b.put(k, v, ver)
                    total_repairs += 1
                else:
                    va_val, va_ver = va
                    vb_val, vb_ver = vb
                    if va_ver > vb_ver:
                        b.put(k, va_val, va_ver)
                        total_repairs += 1
                    elif vb_ver > va_ver:
                        a.put(k, vb_val, vb_ver)
                        total_repairs += 1
                    # if == version, ignore
    print(f"[AntiEntropy] Total repaired entries: {total_repairs}")


def demo_quorums_and_merkle():
    print("\nQuorum: Consistent Hash + N/R/W + Merkle Anti-Entropy")

    # versioned nodes and router
    nodes_q = [QNode(f"q{i}") for i in range(4)]
    ch_q = ConsistentHashRouter(nodes_q, vnodes=64)

    client = QuorumKVClient(
        ch_q,
        replication_factor=3,  # N
        read_quorum=2,         # R
        write_quorum=2,        # W
    )

    keys = [f"k{i}" for i in range(10)]

    print("\n[Quorum] Writing initial values with quorums")
    for i, k in enumerate(keys):
        ok = client.put(k, f"V{i}")
        print(f"PUT {k} = V{i} -> {ok}")

    print("\n[Quorum] Reads before any divergence")
    for k in keys:
        print(f"GET {k} -> {client.get(k)}")

    # Simulate divergence: one node misses some keys
    victim = nodes_q[2]  # q2
    print("\n[Quorum] Simulating divergence: deleting some keys from q2 directly")
    for k in keys[:3]:
        victim.store.pop(k, None)

    # Also corrupt a version on another node
    corrupt = nodes_q[1]  # q1
    if "k5" in corrupt.store:
        v, ver = corrupt.store["k5"]
        corrupt.store["k5"] = (v + "_stale", max(0, ver - 1))
        print("[Quorum] Corrupted k5 on q1 with older version")

    print("\n[Quorum] Reads after divergence (before anti-entropy)")
    for k in keys:
        print(f"GET {k} -> {client.get(k)}")

    # Run Merkle anti-entropy over all nodes
    run_anti_entropy(nodes_q, keys)

    print("\n[Quorum] Reads after Merkle anti-entropy repair")
    for k in keys:
        print(f"GET {k} -> {client.get(k)}")


if __name__ == "__main__":
    demo()
    demo_quorums_and_merkle()
