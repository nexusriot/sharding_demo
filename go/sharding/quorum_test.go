package sharding

import (
	"fmt"
	"testing"
)

func makeQNodes(n int) []*QNode {
	nodes := make([]*QNode, n)
	for i := range nodes {
		nodes[i] = NewQNode(fmt.Sprintf("q%d", i))
	}
	return nodes
}

func TestQuorumPutGet(t *testing.T) {
	nodes := makeQNodes(4)
	router := NewQRingRouter(nodes, 64)
	c := NewQuorumClient(router, 3, 2, 2)

	if !c.Put("a", "1") {
		t.Fatal("put failed")
	}
	v, ok := c.Get("a")
	if !ok || v != "1" {
		t.Fatalf("get: want 1 ok=true, got %q ok=%v", v, ok)
	}
}

func TestQuorumWriteFailsBelowW(t *testing.T) {
	nodes := makeQNodes(4)
	router := NewQRingRouter(nodes, 64)
	c := NewQuorumClient(router, 3, 2, 3)

	// Mark all but one node down; PUT can only hit 1 of 3 replicas.
	for i := 1; i < 4; i++ {
		nodes[i].Healthy = false
	}
	// Find a key whose replicas include n0; or just iterate.
	// Easier: force partial replication by making it so any pick will have
	// <W healthy hits. With only 1 healthy node total this is guaranteed.
	if c.Put("k", "v") {
		t.Fatal("PUT should fail when fewer than W replicas are healthy")
	}
}

func TestQuorumReadRepair(t *testing.T) {
	nodes := makeQNodes(4)
	router := NewQRingRouter(nodes, 64)
	c := NewQuorumClient(router, 3, 2, 2)

	c.Put("a", "v1")
	c.Put("a", "v2")
	c.Put("a", "v3")

	// Corrupt: delete from one of the actual replicas.
	replicas := router.PickQReplicas("a", 3)
	if len(replicas) < 2 {
		t.Fatal("not enough replicas to set up the test")
	}
	delete(replicas[0].Store, "a")

	v, ok := c.Get("a")
	if !ok || v != "v3" {
		t.Fatalf("get after corruption: want v3 ok=true, got %q ok=%v", v, ok)
	}
	// Read-repair should have restored the value on replicas[0].
	if rec, ok := replicas[0].Get("a"); !ok || rec.Value != "v3" {
		t.Fatalf("read repair did not restore key on replicas[0]: %+v ok=%v", rec, ok)
	}
}

func TestQNodeVersionOrdering(t *testing.T) {
	n := NewQNode("q0")
	n.Put("k", "old", 5)
	n.Put("k", "older", 3) // should be rejected
	v, _ := n.Get("k")
	if v.Value != "old" || v.Version != 5 {
		t.Fatalf("older version should not overwrite: got %+v", v)
	}
	n.Put("k", "new", 7)
	v, _ = n.Get("k")
	if v.Value != "new" || v.Version != 7 {
		t.Fatalf("newer version should win: got %+v", v)
	}
}
