package sharding

// Node is a basic in-memory key/value replica with a health flag.
type Node struct {
	Name    string
	Store   map[string]string
	Healthy bool
}

func NewNode(name string) *Node {
	return &Node{Name: name, Store: map[string]string{}, Healthy: true}
}

func (n *Node) Put(k, v string)           { n.Store[k] = v }
func (n *Node) Get(k string) (string, bool) { v, ok := n.Store[k]; return v, ok }
func (n *Node) Delete(k string)           { delete(n.Store, k) }
