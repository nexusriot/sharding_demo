package sharding

import (
	"fmt"
	"testing"
)

func makeNodes(n int) []*Node {
	nodes := make([]*Node, n)
	for i := range nodes {
		nodes[i] = NewNode(fmt.Sprintf("n%d", i))
	}
	return nodes
}

func TestModuloPickReplicasCount(t *testing.T) {
	r := NewModuloRouter(makeNodes(4))
	reps := r.PickReplicas("hello", 3)
	if len(reps) != 3 {
		t.Fatalf("want 3 replicas, got %d", len(reps))
	}
	seen := map[string]bool{}
	for _, n := range reps {
		if seen[n.Name] {
			t.Fatalf("duplicate replica %s", n.Name)
		}
		seen[n.Name] = true
	}
}

func TestModuloDeterministic(t *testing.T) {
	r := NewModuloRouter(makeNodes(5))
	a := r.PickReplicas("key-42", 3)
	b := r.PickReplicas("key-42", 3)
	if len(a) != len(b) {
		t.Fatalf("length mismatch")
	}
	for i := range a {
		if a[i].Name != b[i].Name {
			t.Fatalf("not deterministic at %d: %s vs %s", i, a[i].Name, b[i].Name)
		}
	}
}

func TestModuloSkipsUnhealthy(t *testing.T) {
	r := NewModuloRouter(makeNodes(4))
	r.SetHealth("n0", false)
	r.SetHealth("n1", false)
	reps := r.PickReplicas("anything", 3)
	if len(reps) != 2 {
		t.Fatalf("want 2 (only 2 healthy), got %d", len(reps))
	}
	for _, n := range reps {
		if n.Name == "n0" || n.Name == "n1" {
			t.Fatalf("picked unhealthy node %s", n.Name)
		}
	}
}

func TestModuloAllDown(t *testing.T) {
	r := NewModuloRouter(makeNodes(3))
	for _, n := range r.Nodes {
		n.Healthy = false
	}
	if reps := r.PickReplicas("x", 3); len(reps) != 0 {
		t.Fatalf("want 0 when all down, got %d", len(reps))
	}
}

func TestModuloDistribution(t *testing.T) {
	r := NewModuloRouter(makeNodes(4))
	counts := map[string]int{}
	for i := 0; i < 10000; i++ {
		reps := r.PickReplicas(fmt.Sprintf("k-%d", i), 1)
		counts[reps[0].Name]++
	}
	for name, c := range counts {
		if c < 2000 || c > 3000 {
			t.Errorf("uneven distribution: %s=%d (want ~2500)", name, c)
		}
	}
}
