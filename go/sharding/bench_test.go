package sharding

import (
	"fmt"
	"testing"
)

func benchKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench-key-%d", i)
	}
	return keys
}

func BenchmarkModuloPickReplicas(b *testing.B) {
	r := NewModuloRouter(makeNodes(16))
	keys := benchKeys(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.PickReplicas(keys[i&1023], 3)
	}
}

func BenchmarkCHRingPickReplicas(b *testing.B) {
	for _, v := range []int{16, 64, 256} {
		b.Run(fmt.Sprintf("vnodes=%d", v), func(b *testing.B) {
			r := NewConsistentHashRouter(makeNodes(16), v)
			keys := benchKeys(1024)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = r.PickReplicas(keys[i&1023], 3)
			}
		})
	}
}

func BenchmarkCHRingBuild(b *testing.B) {
	for _, v := range []int{16, 64, 256} {
		b.Run(fmt.Sprintf("vnodes=%d", v), func(b *testing.B) {
			nodes := makeNodes(16)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = NewConsistentHashRouter(nodes, v)
			}
		})
	}
}

func BenchmarkMerkleBuild(b *testing.B) {
	for _, sz := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("keys=%d", sz), func(b *testing.B) {
			node := NewQNode("q0")
			keys := make([]string, sz)
			for i := 0; i < sz; i++ {
				keys[i] = fmt.Sprintf("k%d", i)
				node.Put(keys[i], fmt.Sprintf("v%d", i), i+1)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = MerkleForQNode(node, keys)
			}
		})
	}
}

func BenchmarkAntiEntropyNoDiff(b *testing.B) {
	// Best case: roots match, no leaf scan needed.
	nodes := makeQNodes(4)
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
		for _, n := range nodes {
			n.Put(keys[i], fmt.Sprintf("v%d", i), i+1)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Suppress the printlns inside RunAntiEntropy by short-circuiting:
		// we re-run the cheap pair-compare loop here directly.
		for x := 0; x < len(nodes); x++ {
			for y := x + 1; y < len(nodes); y++ {
				ta := MerkleForQNode(nodes[x], keys)
				tb := MerkleForQNode(nodes[y], keys)
				if ta.Root() == tb.Root() {
					continue
				}
				_ = ta.DiffLeafIndices(tb)
			}
		}
	}
}
