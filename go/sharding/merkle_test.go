package sharding

import (
	"fmt"
	"testing"
)

func TestMerkleEqualRoot(t *testing.T) {
	leaves := []string{"a", "b", "c", "d"}
	t1 := NewMerkleTree(leaves)
	t2 := NewMerkleTree(leaves)
	if t1.Root() != t2.Root() {
		t.Fatal("identical leaves should produce identical root")
	}
	if diffs := t1.DiffLeafIndices(t2); len(diffs) != 0 {
		t.Fatalf("equal trees should report no diffs, got %v", diffs)
	}
}

func TestMerkleDetectsDiff(t *testing.T) {
	a := NewMerkleTree([]string{"a", "b", "c", "d"})
	b := NewMerkleTree([]string{"a", "X", "c", "Y"})
	if a.Root() == b.Root() {
		t.Fatal("different leaves should produce different roots")
	}
	diffs := a.DiffLeafIndices(b)
	if len(diffs) != 2 || diffs[0] != 1 || diffs[1] != 3 {
		t.Fatalf("want diffs [1 3], got %v", diffs)
	}
}

func TestAntiEntropyConvergence(t *testing.T) {
	nodes := makeQNodes(4)
	keys := make([]string, 20)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}
	// Seed all nodes with the same data.
	for ver, k := range keys {
		v := fmt.Sprintf("v%d", ver)
		for _, n := range nodes {
			n.Put(k, v, ver+1)
		}
	}
	// Diverge: drop some keys from node 0, corrupt some on node 1.
	delete(nodes[0].Store, "k0")
	delete(nodes[0].Store, "k1")
	nodes[1].Store["k5"] = VersionedVal{Value: "stale", Version: 1}

	// Pre-condition: roots disagree.
	r0 := MerkleForQNode(nodes[0], keys).Root()
	r1 := MerkleForQNode(nodes[1], keys).Root()
	if r0 == r1 {
		t.Fatal("expected divergence before anti-entropy")
	}

	RunAntiEntropy(nodes, keys)

	// After convergence, all roots equal.
	root := MerkleForQNode(nodes[0], keys).Root()
	for i, n := range nodes {
		if got := MerkleForQNode(n, keys).Root(); got != root {
			t.Fatalf("node %d root diverges after anti-entropy", i)
		}
	}
	// And k5 should have the newer value (version 6) on all nodes.
	for _, n := range nodes {
		v, ok := n.Get("k5")
		if !ok || v.Value != "v5" || v.Version != 6 {
			t.Fatalf("k5 not repaired on %s: %+v ok=%v", n.Name, v, ok)
		}
	}
}
