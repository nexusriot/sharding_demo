# sharding-tui

A live terminal dashboard for the sharding / replication PoC. Visualizes the
consistent-hash ring, node health, replica selection, and key distribution in
real time — so you can *see* what changes when a node dies, when you switch
routers, or when you bump the replication factor.

Go port of the Python TUI (`../tui.py`). Built with
[bubbletea](https://github.com/charmbracelet/bubbletea) + lipgloss.

## Install / run

```sh
# from go/
make build           # → bin/sharding-demo, bin/sharding-tui
make tui             # build + run

# or a system install via .deb
make deb             # → dist/sharding-demo_<version>_<arch>.deb
sudo dpkg -i dist/sharding-demo_*.deb
sharding-tui
```

You can also `go run ./cmd/tui` if you don't want artifacts.

## Layout

```
┌─ status ─────────────────────────────────────────────────────────────┐
│ mode: CH   R=3   nodes=5/6    ready                                  │
│ [m]odulo [c]h [0-5]toggle [+/-]R [k]ey [s]eed [w]ipe [q]uit          │
└──────────────────────────────────────────────────────────────────────┘
┌─ ring ────────────────────────┐ ┌─ nodes ─────────────────────────┐
│         ●  ●                  │ │ # node  health   keys   share   │
│      ●        ●               │ │ 0 n0    UP        812   33.4%   │
│   ●              ●            │ │ 1 n1    DOWN        0    0.0%   │
│  ●       K        ●           │ │ ...                             │
│   ●              ●            │ └─────────────────────────────────┘
│      ●        ●               │ ┌─ primary distribution ──────────┐
│         ●  ●                  │ │ n0 ████████████ 165  33.0%      │
└───────────────────────────────┘ └─────────────────────────────────┘
┌─ replica walk ───────────────────────────────────────────────────────┐
│ key:    user:42                                                      │
│ hash:   3f7a90c1...                                                  │
│ chosen: n2 → n0 → n3                                                 │
│ walk:   n2 ▸ n0 ▸ n1 ▸ n3                                            │
└──────────────────────────────────────────────────────────────────────┘
```

### Panels

- **status** — current mode (`CH` / `MOD`), replication factor `R`,
  healthy/total nodes, transient messages, and the hotkey legend. Also
  hosts the inline `key>` prompt when you press `k`.

- **ring** *(CH mode only)* — circular projection of the SHA-1 hash space
  (top = 0, clockwise). Each colored `●` is one **vnode**; color identifies
  the physical node. `x` means a vnode whose owner is currently DOWN. The
  red `K` marks the last key you traced — replicas are picked by walking
  clockwise from `K` until R distinct healthy nodes are collected.

- **nodes** — one row per physical node. `#` is the digit that toggles its
  health. `keys` shows how many keys it actually stores; `share` is its
  percent of the total. Useful for confirming replication is writing where
  you expect and that the load is balanced.

- **primary distribution** — after you press `s`, shows where the **primary**
  (first replica) lands for each of 500 random keys, as a bar chart. This is
  the panel that makes the modulo-vs-CH difference visible: modulo gives
  near-equal bars; CH with vnodes is close, low-vnode CH is lumpier. Kill a
  node and watch its bar collapse while neighbors absorb the load.

- **replica walk** — updates whenever you trace a key with `k`:
  - `chosen` — the final replica set in pick order.
  - `walk` — every physical node visited while collecting R replicas:
    - **highlighted** = picked (made it into the replica set)
    - **strikethrough** = skipped because the node is DOWN
    - **dim** = visited but skipped (already-chosen node, common in CH when
      another vnode of the same physical node comes up)

  In CH mode, consecutive hits on the same physical node are collapsed into
  one entry so the line stays readable.

## Hotkeys

| key   | action |
|-------|--------|
| `m`   | switch to modulo router |
| `c`   | switch to consistent-hash router |
| `0`–`5` | toggle node health (up ↔ down) |
| `+` / `-` | increase / decrease replication factor R |
| `k`   | open inline prompt — type a key, `Enter` to trace, `Esc` to cancel |
| `s`   | seed 500 random keys (writes via the current router) |
| `w`   | wipe all node stores |
| `q`   | quit |

`Backspace` works inside the `k` prompt; `Ctrl+C` quits from anywhere.

## Suggested walkthrough

1. Press `s` to seed 500 random keys. Bars appear in the distribution panel.
2. Press `k`, type `user:42`, hit Enter. The red `K` lights up on the ring
   and the walk line shows which three replicas were picked and in what
   order.
3. Press `2` to kill `n2`. Watch:
   - the `nodes` panel flip `n2` to `DOWN`,
   - the `primary distribution` bars rebalance — `n2`'s slice gets
     redistributed across its clockwise neighbors,
   - the `walk` line now shows `n2` with strikethrough and a replacement
     node taking its slot in `chosen`.
4. Press `m` to switch to modulo, repeat. Notice the distribution becomes
   almost perfectly flat — but killing a node shifts *every* key in that
   shard, unlike CH where only neighboring keys move.
5. Press `+` to bump R to 4, then 5. The `walk` line grows; once R exceeds
   the number of healthy nodes the status bar warns you.

## Why two routers

| | Modulo | Consistent Hash |
|---|---|---|
| Distribution | very even | even with enough vnodes |
| Add/remove node | every key may re-shard | only neighbors move |
| Hash visualization | trivial (modulo N) | meaningful ring |
| Health-aware skip | walk forward | walk clockwise |

The TUI exists so you can flip between them on the same key set and watch
exactly which keys move when membership changes — the central insight of
consistent hashing.

## Troubleshooting

- **Colors look wrong / no colors** — your terminal isn't reporting
  truecolor. Try `TERM=xterm-256color` or a modern terminal (alacritty,
  kitty, wezterm, recent gnome-terminal).
- **Ring panel is empty / tiny** — terminal is too narrow. Resize; the
  layout reflows on `WindowSizeMsg`.
- **`bubbletea` errors about TTY** — you're piping the binary or running
  it under a non-interactive shell. Run it directly in a terminal.
