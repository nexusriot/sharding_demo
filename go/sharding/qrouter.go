package sharding

import (
	"fmt"
	"math/big"
	"sort"
)

// QRingRouter is a CH ring of QNodes (versioned). Mirrors ConsistentHashRouter.
type QRingRouter struct {
	Vnodes int
	Ring   []qRingEntry
	byName map[string]*QNode
}

type qRingEntry struct {
	Hash *big.Int
	Node *QNode
}

func NewQRingRouter(nodes []*QNode, vnodes int) *QRingRouter {
	r := &QRingRouter{Vnodes: vnodes, byName: map[string]*QNode{}}
	for _, n := range nodes {
		r.byName[n.Name] = n
		for v := 0; v < vnodes; v++ {
			r.Ring = append(r.Ring, qRingEntry{Hash: HashSHA1(fmt.Sprintf("%s#%d", n.Name, v)), Node: n})
		}
	}
	sort.Slice(r.Ring, func(i, j int) bool { return r.Ring[i].Hash.Cmp(r.Ring[j].Hash) < 0 })
	return r
}

func (r *QRingRouter) SetHealth(name string, healthy bool) {
	if n, ok := r.byName[name]; ok {
		n.Healthy = healthy
	}
}

func (r *QRingRouter) PickQReplicas(key string, k int) []*QNode {
	if k <= 0 || len(r.Ring) == 0 {
		return nil
	}
	h := HashSHA1(key)
	i := sort.Search(len(r.Ring), func(j int) bool { return r.Ring[j].Hash.Cmp(h) >= 0 })
	chosen := make([]*QNode, 0, k)
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
