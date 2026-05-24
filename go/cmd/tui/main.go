package main

import (
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"sort"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/nexusriot/sharding_demo/sharding"
)

const (
	numNodes = 6
	vnodes   = 64
)

var palette = []lipgloss.Color{
	"9", "10", "11", "12", "13", "14", "1", "2", "3", "4",
}

func colorFor(name string) lipgloss.Color {
	idx := 0
	if len(name) > 1 {
		fmt.Sscanf(name[1:], "%d", &idx)
	}
	return palette[idx%len(palette)]
}

type walkStep struct {
	Name    string
	Healthy bool
	Picked  bool
}

func traceWalk(m *model, key string) (chosen []string, steps []walkStep) {
	r := m.replication
	seen := map[string]struct{}{}
	if m.mode == "mod" {
		nodes := m.modulo.Nodes
		h := sharding.HashMD5(key)
		start := int(new(big.Int).Mod(h, big.NewInt(int64(len(nodes)))).Int64())
		for i := 0; i < len(nodes)*2; i++ {
			if len(chosen) >= r {
				break
			}
			n := nodes[(start+i)%len(nodes)]
			pick := n.Healthy
			if _, dup := seen[n.Name]; dup {
				pick = false
			}
			steps = append(steps, walkStep{n.Name, n.Healthy, pick})
			if pick {
				chosen = append(chosen, n.Name)
				seen[n.Name] = struct{}{}
			}
		}
		return
	}
	ring := m.ch.Ring
	if len(ring) == 0 {
		return
	}
	h := sharding.HashSHA1(key)
	i := sort.Search(len(ring), func(j int) bool { return ring[j].Hash.Cmp(h) >= 0 })
	maxSteps := len(ring) + vnodes
	for st := 0; len(chosen) < r && st < maxSteps; st++ {
		if i >= len(ring) {
			i = 0
		}
		n := ring[i].Node
		pick := false
		if _, dup := seen[n.Name]; !dup && n.Healthy {
			pick = true
		}
		// collapse consecutive vnodes of the same physical node
		if len(steps) == 0 || steps[len(steps)-1].Name != n.Name {
			steps = append(steps, walkStep{n.Name, n.Healthy, pick})
		}
		if pick {
			chosen = append(chosen, n.Name)
			seen[n.Name] = struct{}{}
		}
		i++
	}
	return
}

type model struct {
	width, height int

	nodes  []*sharding.Node
	modulo *sharding.ModuloRouter
	ch     *sharding.ConsistentHashRouter

	mode        string // "ch" or "mod"
	replication int

	seededKeys  []string
	lastKey     string
	lastChosen  []string
	lastSteps   []walkStep
	message     string
	promptMode  bool
	promptBuf   string
}

func ringEntryHashRatio(h *big.Int) float64 {
	// 2^160 as float (approximation is fine for visualization)
	max := new(big.Float).SetInt(new(big.Int).Lsh(big.NewInt(1), 160))
	cur := new(big.Float).SetInt(h)
	f, _ := new(big.Float).Quo(cur, max).Float64()
	return f
}

func initialModel() model {
	ns := make([]*sharding.Node, numNodes)
	for i := range ns {
		ns[i] = sharding.NewNode(fmt.Sprintf("n%d", i))
	}
	return model{
		nodes:       ns,
		modulo:      sharding.NewModuloRouter(ns),
		ch:          sharding.NewConsistentHashRouter(ns, vnodes),
		mode:        "ch",
		replication: 3,
		message:     "ready",
	}
}

func (m model) Init() tea.Cmd { return nil }

func (m *model) retraceIfNeeded() {
	if m.lastKey != "" {
		m.lastChosen, m.lastSteps = traceWalk(m, m.lastKey)
	}
}

func (m *model) toggleNode(idx int) {
	if idx >= len(m.nodes) {
		return
	}
	n := m.nodes[idx]
	n.Healthy = !n.Healthy
	if n.Healthy {
		m.message = fmt.Sprintf("%s -> UP", n.Name)
	} else {
		m.message = fmt.Sprintf("%s -> DOWN", n.Name)
	}
}

func (m *model) seedKeys() {
	rng := rand.New(rand.NewSource(0xC0FFEE))
	m.seededKeys = make([]string, 500)
	for i := range m.seededKeys {
		m.seededKeys[i] = fmt.Sprintf("k%d-%d", i, rng.Uint32())
	}
	r := m.replication
	for _, k := range m.seededKeys {
		var reps []*sharding.Node
		if m.mode == "mod" {
			reps = m.modulo.PickReplicas(k, r)
		} else {
			reps = m.ch.PickReplicas(k, r)
		}
		for _, n := range reps {
			n.Put(k, "V:"+k)
		}
	}
	m.message = fmt.Sprintf("seeded %d keys (R=%d)", len(m.seededKeys), r)
}

func (m *model) wipe() {
	for _, n := range m.nodes {
		n.Store = map[string]string{}
	}
	m.seededKeys = nil
	m.message = "wiped stores"
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil
	case tea.KeyMsg:
		if m.promptMode {
			switch msg.Type {
			case tea.KeyEnter:
				m.promptMode = false
				if m.promptBuf != "" {
					m.lastKey = m.promptBuf
					m.lastChosen, m.lastSteps = traceWalk(&m, m.lastKey)
					m.message = fmt.Sprintf("traced %q", m.lastKey)
				} else {
					m.message = "cancelled"
				}
				m.promptBuf = ""
				return m, nil
			case tea.KeyEsc:
				m.promptMode = false
				m.promptBuf = ""
				m.message = "cancelled"
				return m, nil
			case tea.KeyBackspace:
				if len(m.promptBuf) > 0 {
					m.promptBuf = m.promptBuf[:len(m.promptBuf)-1]
				}
				return m, nil
			case tea.KeyCtrlC:
				return m, tea.Quit
			case tea.KeyRunes, tea.KeySpace:
				m.promptBuf += msg.String()
				return m, nil
			}
			return m, nil
		}
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "m":
			m.mode = "mod"
			m.message = "modulo router"
		case "c":
			m.mode = "ch"
			m.message = "consistent-hash router"
		case "+", "=":
			if m.replication < len(m.nodes) {
				m.replication++
				m.message = fmt.Sprintf("R=%d", m.replication)
			}
		case "-":
			if m.replication > 1 {
				m.replication--
				m.message = fmt.Sprintf("R=%d", m.replication)
			}
		case "k":
			m.promptMode = true
			m.promptBuf = ""
			m.message = "enter key, ESC to cancel"
		case "s":
			m.seedKeys()
		case "w":
			m.wipe()
		default:
			if len(msg.String()) == 1 && msg.String()[0] >= '0' && msg.String()[0] <= '9' {
				m.toggleNode(int(msg.String()[0] - '0'))
			}
		}
		m.retraceIfNeeded()
	}
	return m, nil
}

var (
	borderStyle  = lipgloss.NewStyle().Border(lipgloss.RoundedBorder())
	dimStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	headerStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("15")).Bold(true)
	upStyle      = lipgloss.NewStyle().Foreground(lipgloss.Color("10")).Bold(true)
	downStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("9")).Bold(true)
	titleColors  = map[string]lipgloss.Color{
		"ring": "14", "nodes": "13", "walk": "11", "dist": "10", "head": "15",
	}
)

func panel(title string, body string, color lipgloss.Color, width int) string {
	style := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(color).
		Padding(0, 1)
	if width > 0 {
		style = style.Width(width - 2)
	}
	titleStr := lipgloss.NewStyle().Foreground(color).Bold(true).Render(" " + title + " ")
	return titleStr + "\n" + style.Render(body)
}

func (m model) renderHeader() string {
	var b strings.Builder
	b.WriteString(dimStyle.Render("mode: "))
	b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("14")).Bold(true).Render(strings.ToUpper(m.mode)))
	b.WriteString(dimStyle.Render("  R="))
	b.WriteString(headerStyle.Render(fmt.Sprintf("%d", m.replication)))
	up := 0
	for _, n := range m.nodes {
		if n.Healthy {
			up++
		}
	}
	b.WriteString(dimStyle.Render("  nodes="))
	b.WriteString(headerStyle.Render(fmt.Sprintf("%d/%d", up, len(m.nodes))))
	b.WriteString("    ")
	if m.promptMode {
		b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("10")).Bold(true).Render("key> "))
		b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("15")).Background(lipgloss.Color("236")).Render(m.promptBuf))
		b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("10")).Blink(true).Render("▌"))
	} else {
		b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("11")).Italic(true).Render(m.message))
	}
	b.WriteString("\n")
	b.WriteString(dimStyle.Render("  [m]odulo  [c]h  [0-5]toggle  [+/-]R  [k]ey  [s]eed  [w]ipe  [q]uit"))
	return panel("status", b.String(), titleColors["head"], m.width-2)
}

func (m model) renderRing(width, height int) string {
	if m.mode != "ch" {
		return panel("ring", dimStyle.Render("Ring view only applies to ConsistentHash mode.\nPress 'c' to switch."), titleColors["ring"], width)
	}
	w := width - 6
	h := height - 4
	if w < 20 {
		w = 20
	}
	if h < 8 {
		h = 8
	}
	canvas := make([][]string, h)
	styles := make([][]lipgloss.Style, h)
	for i := range canvas {
		canvas[i] = make([]string, w)
		styles[i] = make([]lipgloss.Style, w)
		for j := range canvas[i] {
			canvas[i][j] = " "
		}
	}
	cx, cy := w/2, h/2
	rx, ry := float64(w/2-2), float64(h/2-1)
	for _, entry := range m.ch.Ring {
		ratio := ringEntryHashRatio(entry.Hash)
		theta := ratio*2*math.Pi - math.Pi/2
		x := cx + int(math.Round(rx*math.Cos(theta)))
		y := cy + int(math.Round(ry*math.Sin(theta)))
		if x >= 0 && x < w && y >= 0 && y < h {
			if entry.Node.Healthy {
				canvas[y][x] = "●"
			} else {
				canvas[y][x] = "x"
			}
			styles[y][x] = lipgloss.NewStyle().Foreground(colorFor(entry.Node.Name)).Bold(true)
		}
	}
	if m.lastKey != "" {
		h160 := sharding.HashSHA1(m.lastKey)
		ratio := ringEntryHashRatio(h160)
		theta := ratio*2*math.Pi - math.Pi/2
		x := cx + int(math.Round(rx*math.Cos(theta)))
		y := cy + int(math.Round(ry*math.Sin(theta)))
		if x >= 0 && x < w && y >= 0 && y < h {
			canvas[y][x] = "K"
			styles[y][x] = lipgloss.NewStyle().Foreground(lipgloss.Color("15")).Background(lipgloss.Color("9")).Bold(true)
		}
	}
	var lines []string
	for y := 0; y < h; y++ {
		var line strings.Builder
		for x := 0; x < w; x++ {
			s := canvas[y][x]
			if s == " " {
				line.WriteString(" ")
			} else {
				line.WriteString(styles[y][x].Render(s))
			}
		}
		lines = append(lines, line.String())
	}
	body := strings.Join(lines, "\n")
	title := fmt.Sprintf("ring (vnodes=%d, sha1)", vnodes)
	return panel(title, body, titleColors["ring"], width)
}

func (m model) renderNodes(width int) string {
	total := 0
	for _, n := range m.nodes {
		total += len(n.Store)
	}
	if total == 0 {
		total = 1
	}
	var rows []string
	rows = append(rows, dimStyle.Render(fmt.Sprintf("%-3s %-6s %-8s %8s %8s", "#", "node", "health", "keys", "share")))
	for i, n := range m.nodes {
		col := lipgloss.NewStyle().Foreground(colorFor(n.Name)).Bold(true)
		health := upStyle.Render("UP")
		if !n.Healthy {
			health = downStyle.Render("DOWN")
		}
		share := fmt.Sprintf("%5.1f%%", float64(len(n.Store))/float64(total)*100)
		rows = append(rows, fmt.Sprintf("%-3d %s %-15s %8d %8s",
			i, col.Render(fmt.Sprintf("%-6s", n.Name)), health, len(n.Store), share))
	}
	rows = append(rows, "", dimStyle.Render("press 0-5 to toggle health"))
	return panel("nodes", strings.Join(rows, "\n"), titleColors["nodes"], width)
}

func (m model) renderWalk(width int) string {
	if m.lastKey == "" {
		return panel("replica walk", dimStyle.Render("press 'k' and type a key to trace its replica walk"), titleColors["walk"], width)
	}
	var lines []string
	lines = append(lines, dimStyle.Render("key:    ")+lipgloss.NewStyle().Bold(true).Render(m.lastKey))
	var h *big.Int
	if m.mode == "ch" {
		h = sharding.HashSHA1(m.lastKey)
	} else {
		h = sharding.HashMD5(m.lastKey)
	}
	lines = append(lines, dimStyle.Render("hash:   ")+fmt.Sprintf("%x", h.Bytes())[:16])

	var chosen strings.Builder
	chosen.WriteString(dimStyle.Render("chosen: "))
	for i, name := range m.lastChosen {
		if i > 0 {
			chosen.WriteString(" → ")
		}
		chosen.WriteString(lipgloss.NewStyle().Foreground(colorFor(name)).Bold(true).Render(name))
	}
	lines = append(lines, chosen.String())

	var walk strings.Builder
	walk.WriteString(dimStyle.Render("walk:   "))
	for i, step := range m.lastSteps {
		if i > 0 {
			walk.WriteString(" ▸ ")
		}
		col := colorFor(step.Name)
		switch {
		case step.Picked:
			walk.WriteString(lipgloss.NewStyle().Foreground(col).Background(lipgloss.Color("236")).Bold(true).Render(step.Name))
		case !step.Healthy:
			walk.WriteString(lipgloss.NewStyle().Foreground(col).Strikethrough(true).Render(step.Name))
		default:
			walk.WriteString(lipgloss.NewStyle().Foreground(col).Faint(true).Render(step.Name))
		}
	}
	lines = append(lines, walk.String())
	return panel("replica walk", strings.Join(lines, "\n"), titleColors["walk"], width)
}

func (m model) renderDistribution(width int) string {
	if len(m.seededKeys) == 0 {
		return panel("primary distribution", dimStyle.Render("press 's' to seed keys and see primary distribution"), titleColors["dist"], width)
	}
	counts := map[string]int{}
	for _, k := range m.seededKeys {
		var reps []*sharding.Node
		if m.mode == "mod" {
			reps = m.modulo.PickReplicas(k, 1)
		} else {
			reps = m.ch.PickReplicas(k, 1)
		}
		if len(reps) > 0 {
			counts[reps[0].Name]++
		}
	}
	total := 0
	mx := 0
	for _, c := range counts {
		total += c
		if c > mx {
			mx = c
		}
	}
	if total == 0 {
		return panel("primary distribution", downStyle.Render("no healthy nodes"), titleColors["dist"], width)
	}
	var lines []string
	for _, n := range m.nodes {
		cnt := counts[n.Name]
		pct := float64(cnt) / float64(total) * 100
		barLen := 0
		if mx > 0 {
			barLen = cnt * 30 / mx
		}
		colStyle := lipgloss.NewStyle().Foreground(colorFor(n.Name))
		line := colStyle.Render(n.Name+" ") + colStyle.Render(strings.Repeat("█", barLen)) + dimStyle.Render(fmt.Sprintf(" %5d  %5.1f%%", cnt, pct))
		lines = append(lines, line)
	}
	title := fmt.Sprintf("primary distribution (%d keys, %s)", len(m.seededKeys), m.mode)
	return panel(title, strings.Join(lines, "\n"), titleColors["dist"], width)
}

func (m model) View() string {
	if m.width == 0 {
		return "initializing..."
	}
	header := m.renderHeader()

	rightW := m.width / 2
	leftW := m.width - rightW
	mainH := m.height - 12
	if mainH < 12 {
		mainH = 12
	}

	ring := m.renderRing(leftW, mainH)
	nodes := m.renderNodes(rightW)
	dist := m.renderDistribution(rightW)
	right := lipgloss.JoinVertical(lipgloss.Left, nodes, dist)
	main := lipgloss.JoinHorizontal(lipgloss.Top, ring, right)

	walk := m.renderWalk(m.width - 2)

	return lipgloss.JoinVertical(lipgloss.Left, header, main, walk)
}

func main() {
	p := tea.NewProgram(initialModel(), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Println("error:", err)
	}
}
