package sharding

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
)

// MerkleTree over a fixed-order leaf list. Brute-force diff if roots differ
// (matches the Python PoC).
type MerkleTree struct {
	Levels [][]string
}

func hashStr(s string) string {
	sum := sha1.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

func NewMerkleTree(leaves []string) *MerkleTree {
	t := &MerkleTree{}
	if len(leaves) == 0 {
		t.Levels = [][]string{{""}}
		return t
	}
	t.Levels = append(t.Levels, leaves)
	level := leaves
	for len(level) > 1 {
		next := make([]string, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			left := level[i]
			right := left
			if i+1 < len(level) {
				right = level[i+1]
			}
			next = append(next, hashStr(left+":"+right))
		}
		t.Levels = append(t.Levels, next)
		level = next
	}
	return t
}

func MerkleForQNode(n *QNode, keys []string) *MerkleTree {
	leaves := make([]string, len(keys))
	for i, k := range keys {
		if rec, ok := n.Get(k); ok {
			leaves[i] = hashStr(fmt.Sprintf("%s:%s:%d", k, rec.Value, rec.Version))
		} else {
			leaves[i] = hashStr(k + ":<MISSING>")
		}
	}
	return NewMerkleTree(leaves)
}

func (t *MerkleTree) Root() string {
	if len(t.Levels) == 0 {
		return ""
	}
	return t.Levels[len(t.Levels)-1][0]
}

func (t *MerkleTree) DiffLeafIndices(other *MerkleTree) []int {
	if t.Root() == other.Root() {
		return nil
	}
	a, b := t.Levels[0], other.Levels[0]
	min := len(a)
	if len(b) < min {
		min = len(b)
	}
	var diffs []int
	for i := 0; i < min; i++ {
		if a[i] != b[i] {
			diffs = append(diffs, i)
		}
	}
	for i := min; i < len(a) || i < len(b); i++ {
		diffs = append(diffs, i)
	}
	return diffs
}

// RunAntiEntropy is the pairwise Merkle reconciliation from the Python PoC.
func RunAntiEntropy(nodes []*QNode, keys []string) int {
	fmt.Println("\nMerkle: comparing replicas")
	repairs := 0
	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			a, b := nodes[i], nodes[j]
			ta := MerkleForQNode(a, keys)
			tb := MerkleForQNode(b, keys)
			if ta.Root() == tb.Root() {
				continue
			}
			diffs := ta.DiffLeafIndices(tb)
			if len(diffs) == 0 {
				continue
			}
			fmt.Printf("[AntiEntropy] %s <-> %s: %d differing keys\n", a.Name, b.Name, len(diffs))
			for _, idx := range diffs {
				if idx >= len(keys) {
					continue
				}
				k := keys[idx]
				va, oka := a.Get(k)
				vb, okb := b.Get(k)
				switch {
				case !oka && !okb:
					continue
				case !oka:
					a.Put(k, vb.Value, vb.Version)
					repairs++
				case !okb:
					b.Put(k, va.Value, va.Version)
					repairs++
				case va.Version > vb.Version:
					b.Put(k, va.Value, va.Version)
					repairs++
				case vb.Version > va.Version:
					a.Put(k, vb.Value, vb.Version)
					repairs++
				}
			}
		}
	}
	fmt.Printf("[AntiEntropy] Total repaired entries: %d\n", repairs)
	return repairs
}
