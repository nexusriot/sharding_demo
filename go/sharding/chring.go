package sharding

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"sort"
)

type ringEntry struct {
	Hash *big.Int
	Node *Node
}

// ConsistentHashRouter implements a vnode-based CH ring with health awareness.
type ConsistentHashRouter struct {
	Vnodes int
	Ring   []ringEntry // sorted by Hash ascending
	byName map[string]*Node
}

func NewConsistentHashRouter(nodes []*Node, vnodes int) *ConsistentHashRouter {
	r := &ConsistentHashRouter{Vnodes: vnodes, byName: map[string]*Node{}}
	for _, n := range nodes {
		r.byName[n.Name] = n
		r.addPhys(n)
	}
	r.sortRing()
	return r
}

func HashSHA1(s string) *big.Int {
	sum := sha1.Sum([]byte(s))
	return new(big.Int).SetBytes(sum[:])
}

func (r *ConsistentHashRouter) addPhys(n *Node) {
	for v := 0; v < r.Vnodes; v++ {
		r.Ring = append(r.Ring, ringEntry{Hash: HashSHA1(fmt.Sprintf("%s#%d", n.Name, v)), Node: n})
	}
}

func (r *ConsistentHashRouter) sortRing() {
	sort.Slice(r.Ring, func(i, j int) bool { return r.Ring[i].Hash.Cmp(r.Ring[j].Hash) < 0 })
}

func (r *ConsistentHashRouter) SetHealth(name string, healthy bool) {
	if n, ok := r.byName[name]; ok {
		n.Healthy = healthy
	}
}

// bisectLeft returns the first index whose hash >= h.
func (r *ConsistentHashRouter) bisectLeft(h *big.Int) int {
	return sort.Search(len(r.Ring), func(i int) bool { return r.Ring[i].Hash.Cmp(h) >= 0 })
}

func (r *ConsistentHashRouter) PickReplicas(key string, k int) []*Node {
	if k <= 0 || len(r.Ring) == 0 {
		return nil
	}
	h := HashSHA1(key)
	i := r.bisectLeft(h)
	chosen := make([]*Node, 0, k)
	seen := map[string]struct{}{}
	maxSteps := len(r.Ring) + r.Vnodes
	for steps := 0; len(chosen) < k && steps < maxSteps; steps++ {
		if i >= len(r.Ring) {
			i = 0
		}
		n := r.Ring[i].Node
		if _, dup := seen[n.Name]; !dup && n.Healthy {
			chosen = append(chosen, n)
			seen[n.Name] = struct{}{}
		}
		i++
	}
	return chosen
}
