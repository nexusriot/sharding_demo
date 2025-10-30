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
    healthy: bool = True  # health flag (acts as a super-simplified health check)

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
        replicas: List[Node] = []
        if hasattr(self.router, "pick_replicas"):
            replicas = self.router.pick_replicas(k, self.R)
        else:
            replicas = [self.router.pick(k)]
        if len(replicas) < self.R:
            print(f"[WARN] only {len(replicas)}/{self.R} healthy replicas available for key={k}")
        for n in replicas:
            n.put(k, v)

    def get(self, k: str) -> Optional[str]:
        replicas: List[Node] = []
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
        replicas: List[Node] = []
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
    # Nodes
    nodes_mod = [Node(f"n{i}") for i in range(4)]
    nodes_ch  = [Node(f"n{i}") for i in range(4)]

    # Keyset
    random.seed(42)
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

if __name__ == "__main__":
    demo()
