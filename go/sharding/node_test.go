package sharding

import "testing"

func TestNodePutGetDelete(t *testing.T) {
	n := NewNode("n0")
	if !n.Healthy {
		t.Fatal("new node should start healthy")
	}
	n.Put("a", "1")
	if v, ok := n.Get("a"); !ok || v != "1" {
		t.Fatalf("get a: want 1 got %q ok=%v", v, ok)
	}
	if _, ok := n.Get("missing"); ok {
		t.Fatal("missing key should return ok=false")
	}
	n.Delete("a")
	if _, ok := n.Get("a"); ok {
		t.Fatal("deleted key should be gone")
	}
}
