package sharding

import (
	"fmt"
	"testing"
)

func TestCHRingSorted(t *testing.T) {
	r := NewConsistentHashRouter(makeNodes(4), 32)
	if len(r.Ring) != 4*32 {
		t.Fatalf("want %d ring entries, got %d", 4*32, len(r.Ring))
	}
	for i := 1; i < len(r.Ring); i++ {
		if r.Ring[i-1].Hash.Cmp(r.Ring[i].Hash) > 0 {
			t.Fatalf("ring not sorted at %d", i)
		}
	}
}

func TestCHRingReplicasCount(t *testing.T) {
	r := NewConsistentHashRouter(makeNodes(5), 64)
	reps := r.PickReplicas("hello", 3)
	if len(reps) != 3 {
		t.Fatalf("want 3, got %d", len(reps))
	}
	seen := map[string]bool{}
	for _, n := range reps {
		if seen[n.Name] {
			t.Fatalf("duplicate %s", n.Name)
		}
		seen[n.Name] = true
	}
}

func TestCHRingSkipsUnhealthy(t *testing.T) {
	r := NewConsistentHashRouter(makeNodes(4), 64)
	r.SetHealth("n0", false)
	reps := r.PickReplicas("a-key", 3)
	if len(reps) != 3 {
		t.Fatalf("want 3 (3 still healthy), got %d", len(reps))
	}
	for _, n := range reps {
		if n.Name == "n0" {
			t.Fatalf("picked down node n0")
		}
	}
}

// Adding a node should remap only a small fraction of keys — the defining
// property of consistent hashing.
func TestCHRingStability(t *testing.T) {
	nodes := makeNodes(4)
	r := NewConsistentHashRouter(nodes, 128)
	before := map[string]string{}
	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("k-%d", i)
		before[k] = r.PickReplicas(k, 1)[0].Name
	}

	// Add a 5th node. Recreate the ring with 5 nodes (mirrors how a real
	// system would handle membership change).
	nodes2 := append(makeNodes(4), NewNode("n4"))
	r2 := NewConsistentHashRouter(nodes2, 128)
	moved := 0
	for k, prev := range before {
		now := r2.PickReplicas(k, 1)[0].Name
		if now != prev {
			moved++
		}
	}
	frac := float64(moved) / float64(len(before))
	if frac > 0.40 {
		t.Fatalf("too many keys moved: %.2f (want < 0.40)", frac)
	}
	if frac < 0.05 {
		t.Fatalf("suspiciously few keys moved: %.2f", frac)
	}
}

func TestCHRingDistribution(t *testing.T) {
	r := NewConsistentHashRouter(makeNodes(4), 128)
	counts := map[string]int{}
	for i := 0; i < 10000; i++ {
		reps := r.PickReplicas(fmt.Sprintf("k-%d", i), 1)
		counts[reps[0].Name]++
	}
	for name, c := range counts {
		if c < 1500 || c > 3500 {
			t.Errorf("uneven distribution: %s=%d", name, c)
		}
	}
}
