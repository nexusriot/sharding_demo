package sharding

import "fmt"

// VersionedVal is (value, version) stored on a QNode.
type VersionedVal struct {
	Value   string
	Version int
}

type QNode struct {
	Name    string
	Store   map[string]VersionedVal
	Healthy bool
}

func NewQNode(name string) *QNode {
	return &QNode{Name: name, Store: map[string]VersionedVal{}, Healthy: true}
}

func (n *QNode) Put(k, v string, version int) {
	cur, ok := n.Store[k]
	if !ok || version >= cur.Version {
		n.Store[k] = VersionedVal{Value: v, Version: version}
	}
}

func (n *QNode) Get(k string) (VersionedVal, bool) {
	v, ok := n.Store[k]
	return v, ok
}

func (n *QNode) Delete(k string, version int) {
	cur, ok := n.Store[k]
	if !ok || version >= cur.Version {
		delete(n.Store, k)
	}
}

// QRouter is the minimal interface a quorum client needs.
type QRouter interface {
	PickQReplicas(key string, k int) []*QNode
	SetHealth(name string, healthy bool)
}

// QuorumClient implements N/R/W writes with read-repair on top of QRouter.
type QuorumClient struct {
	Router  QRouter
	N, R, W int
	version int
}

func NewQuorumClient(r QRouter, n, rq, wq int) *QuorumClient {
	return &QuorumClient{Router: r, N: n, R: rq, W: wq}
}

func (c *QuorumClient) SetHealth(name string, healthy bool) { c.Router.SetHealth(name, healthy) }

func (c *QuorumClient) nextVersion() int { c.version++; return c.version }

func (c *QuorumClient) Put(k, v string) bool {
	replicas := c.Router.PickQReplicas(k, c.N)
	if len(replicas) == 0 {
		fmt.Printf("[PUT] no replicas for key=%s\n", k)
		return false
	}
	ver := c.nextVersion()
	acks := 0
	for _, n := range replicas {
		if !n.Healthy {
			continue
		}
		n.Put(k, v, ver)
		acks++
	}
	if acks < c.W {
		fmt.Printf("[PUT] FAILED quorum for key=%s acks=%d W=%d\n", k, acks, c.W)
		return false
	}
	if acks < c.N {
		fmt.Printf("[PUT] key=%s wrote to only %d/%d replicas\n", k, acks, c.N)
	}
	return true
}

func (c *QuorumClient) Get(k string) (string, bool) {
	replicas := c.Router.PickQReplicas(k, c.N)
	if len(replicas) == 0 {
		fmt.Printf("[GET] no replicas for key=%s\n", k)
		return "", false
	}
	type resp struct {
		val  string
		ver  int
		node *QNode
	}
	var responses []resp
	for _, n := range replicas {
		if !n.Healthy {
			continue
		}
		if vv, ok := n.Get(k); ok {
			responses = append(responses, resp{vv.Value, vv.Version, n})
		}
	}
	if len(responses) < c.R {
		fmt.Printf("[GET] FAILED quorum for key=%s responses=%d R=%d\n", k, len(responses), c.R)
		return "", false
	}
	best := responses[0]
	for _, r := range responses[1:] {
		if r.ver > best.ver {
			best = r
		}
	}
	for _, n := range replicas {
		if !n.Healthy {
			continue
		}
		cur, ok := n.Get(k)
		if !ok || cur.Version < best.ver {
			n.Put(k, best.val, best.ver)
		}
	}
	return best.val, true
}
