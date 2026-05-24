package main

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/nexusriot/sharding_demo/sharding"
)

func distribution[T any](keys []string, pick func(string, int) []T, name func(T) string, label string) {
	buckets := map[string]int{}
	for _, k := range keys {
		reps := pick(k, 1)
		if len(reps) > 0 {
			buckets[name(reps[0])]++
		}
	}
	fmt.Printf("\n%s (primary) distribution:\n", label)
	names := make([]string, 0, len(buckets))
	for n := range buckets {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Printf("  %-8s %6d\n", n, buckets[n])
	}
}

func replicaLayout[T any](keys []string, pick func(string, int) []T, name func(T) string, r, limit int) {
	fmt.Printf("\nReplica layout for first %d keys (R=%d):\n", limit, r)
	for _, k := range keys[:limit] {
		reps := pick(k, r)
		names := make([]string, len(reps))
		for i, x := range reps {
			names[i] = name(x)
		}
		fmt.Printf("  %-20s -> %v\n", k, names)
	}
}

func nodeName(n *sharding.Node) string { return n.Name }

func demo() {
	fmt.Println("Modulo vs Consistent Hash no quorum")

	nodesMod := []*sharding.Node{}
	nodesCH := []*sharding.Node{}
	for i := 0; i < 4; i++ {
		nodesMod = append(nodesMod, sharding.NewNode(fmt.Sprintf("n%d", i)))
		nodesCH = append(nodesCH, sharding.NewNode(fmt.Sprintf("n%d", i)))
	}

	rng := rand.New(rand.NewSource(42))
	keys := make([]string, 5000)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d-%d", i, rng.Uint32())
	}

	mod := sharding.NewModuloRouter(nodesMod)
	for _, k := range keys[:2000] {
		reps := mod.PickReplicas(k, 3)
		if len(reps) < 3 {
			fmt.Printf("[WARN] only %d/3 healthy replicas for key=%s\n", len(reps), k)
		}
		for _, n := range reps {
			n.Put(k, "V:"+k)
		}
	}
	distribution(keys, mod.PickReplicas, nodeName, "Modulo (R=3)")
	replicaLayout(keys, mod.PickReplicas, nodeName, 3, 3)

	mod.SetHealth("n2", false)
	fmt.Println("\n[Modulo] Marked n2 unhealthy")
	distribution(keys, mod.PickReplicas, nodeName, "Modulo after n2 down (R=3)")
	sample := keys[0]
	for _, n := range mod.PickReplicas(sample, 3) {
		if v, ok := n.Get(sample); ok {
			fmt.Printf("GET %s -> %s\n", sample, v)
			break
		}
	}

	ch := sharding.NewConsistentHashRouter(nodesCH, 128)
	for _, k := range keys {
		for _, n := range ch.PickReplicas(k, 3) {
			n.Put(k, "V:"+k)
		}
	}
	distribution(keys, ch.PickReplicas, nodeName, "ConsistentHash (R=3)")
	replicaLayout(keys, ch.PickReplicas, nodeName, 3, 3)

	ch.SetHealth("n1", false)
	fmt.Println("\n[CH] Marked n1 unhealthy")
	distribution(keys, ch.PickReplicas, nodeName, "ConsistentHash after n1 down (R=3)")

	s := keys[1]
	for _, n := range ch.PickReplicas(s, 3) {
		if v, ok := n.Get(s); ok {
			fmt.Printf("GET %s -> %s\n", s, v)
			break
		}
	}
	ch.SetHealth("n1", true)
	fmt.Println("\n[CH] Marked n1 healthy again")
	for _, n := range ch.PickReplicas(s, 3) {
		if v, ok := n.Get(s); ok {
			fmt.Printf("GET %s -> %s\n", s, v)
			break
		}
	}
}

func demoQuorumsAndMerkle() {
	fmt.Println("\nQuorum: Consistent Hash + N/R/W + Merkle Anti-Entropy")

	nodes := make([]*sharding.QNode, 4)
	for i := range nodes {
		nodes[i] = sharding.NewQNode(fmt.Sprintf("q%d", i))
	}
	router := sharding.NewQRingRouter(nodes, 64)
	client := sharding.NewQuorumClient(router, 3, 2, 2)

	keys := make([]string, 10)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}

	fmt.Println("\n[Quorum] Writing initial values with quorums")
	for i, k := range keys {
		ok := client.Put(k, fmt.Sprintf("V%d", i))
		fmt.Printf("PUT %s = V%d -> %v\n", k, i, ok)
	}

	fmt.Println("\n[Quorum] Reads before any divergence")
	for _, k := range keys {
		v, _ := client.Get(k)
		fmt.Printf("GET %s -> %s\n", k, v)
	}

	victim := nodes[2]
	fmt.Println("\n[Quorum] Simulating divergence: deleting some keys from q2 directly")
	for _, k := range keys[:3] {
		delete(victim.Store, k)
	}

	corrupt := nodes[1]
	if vv, ok := corrupt.Store["k5"]; ok {
		ver := vv.Version - 1
		if ver < 0 {
			ver = 0
		}
		corrupt.Store["k5"] = sharding.VersionedVal{Value: vv.Value + "_stale", Version: ver}
		fmt.Println("[Quorum] Corrupted k5 on q1 with older version")
	}

	fmt.Println("\n[Quorum] Reads after divergence (before anti-entropy)")
	for _, k := range keys {
		v, _ := client.Get(k)
		fmt.Printf("GET %s -> %s\n", k, v)
	}

	sharding.RunAntiEntropy(nodes, keys)

	fmt.Println("\n[Quorum] Reads after Merkle anti-entropy repair")
	for _, k := range keys {
		v, _ := client.Get(k)
		fmt.Printf("GET %s -> %s\n", k, v)
	}
}

func main() {
	demo()
	demoQuorumsAndMerkle()
}
