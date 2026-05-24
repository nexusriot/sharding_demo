package sharding

import (
	"crypto/md5"
	"math/big"
)

// ModuloRouter shards by hash(key) % N with health-aware replication.
type ModuloRouter struct {
	Nodes []*Node
}

func NewModuloRouter(nodes []*Node) *ModuloRouter { return &ModuloRouter{Nodes: nodes} }

func HashMD5(key string) *big.Int {
	sum := md5.Sum([]byte(key))
	return new(big.Int).SetBytes(sum[:])
}

func (r *ModuloRouter) hashIdx(key string) int {
	h := HashMD5(key)
	n := big.NewInt(int64(len(r.Nodes)))
	return int(new(big.Int).Mod(h, n).Int64())
}

func (r *ModuloRouter) SetHealth(name string, healthy bool) {
	for _, n := range r.Nodes {
		if n.Name == name {
			n.Healthy = healthy
		}
	}
}

func (r *ModuloRouter) PickReplicas(key string, k int) []*Node {
	if k <= 0 || len(r.Nodes) == 0 {
		return nil
	}
	start := r.hashIdx(key)
	chosen := make([]*Node, 0, k)
	seen := map[string]struct{}{}
	for i := 0; i < len(r.Nodes)*2 && len(chosen) < k; i++ {
		n := r.Nodes[(start+i)%len(r.Nodes)]
		if _, ok := seen[n.Name]; ok {
			continue
		}
		if !n.Healthy {
			continue
		}
		seen[n.Name] = struct{}{}
		chosen = append(chosen, n)
	}
	return chosen
}
