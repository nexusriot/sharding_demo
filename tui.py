#!/usr/bin/env python3
"""
Live TUI dashboard for the sharding/replication PoC.

Visualizes the consistent-hash ring, node health, replica selection, and key
distribution in real time. Reuses Node / ModuloRouter / ConsistentHashRouter
from demo.py.

Keys:
  0..9  toggle node health
  m     switch to Modulo router
  c     switch to ConsistentHash router
  +/-   change replication factor (R)
  k     prompt for a key and trace its replica walk
  s     seed N random keys (PUT)
  w     wipe stores
  q     quit
"""
from __future__ import annotations

import hashlib
import math
import random
import select
import sys
import termios
import tty
from collections import Counter
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from rich.align import Align
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from demo import Node, ModuloRouter, ConsistentHashRouter


NUM_NODES = 6
VNODES = 64
RING_BITS = 160  # sha1
PALETTE = ["bright_red", "bright_green", "bright_yellow", "bright_blue",
           "bright_magenta", "bright_cyan", "red", "green", "yellow", "blue"]


@dataclass
class AppState:
    nodes: List[Node] = field(default_factory=list)
    modulo: ModuloRouter = field(init=False)
    ch: ConsistentHashRouter = field(init=False)
    mode: str = "ch"  # "ch" or "mod"
    R: int = 3
    last_key: Optional[str] = None
    last_replicas: List[str] = field(default_factory=list)
    last_walk_steps: List[Tuple[str, bool, bool]] = field(default_factory=list)
    # (node_name, healthy, picked)
    message: str = "ready"
    seeded_keys: List[str] = field(default_factory=list)
    prompt_active: bool = False
    prompt_buffer: str = ""

    def __post_init__(self) -> None:
        self.nodes = [Node(f"n{i}") for i in range(NUM_NODES)]
        self.modulo = ModuloRouter(self.nodes)
        self.ch = ConsistentHashRouter(self.nodes, vnodes=VNODES)

    @property
    def router(self):
        return self.ch if self.mode == "ch" else self.modulo

    def color(self, name: str) -> str:
        idx = int(name[1:]) if name[1:].isdigit() else 0
        return PALETTE[idx % len(PALETTE)]


def _h_sha1(s: str) -> int:
    return int(hashlib.sha1(s.encode()).hexdigest(), 16)


def _h_md5(s: str) -> int:
    return int(hashlib.md5(s.encode()).hexdigest(), 16)


def trace_walk(state: AppState, key: str) -> Tuple[List[str], List[Tuple[str, bool, bool]]]:
    """Replay the replica selection so we can show the walk step-by-step."""
    steps: List[Tuple[str, bool, bool]] = []
    chosen: List[str] = []
    seen = set()
    r = state.R

    if state.mode == "mod":
        nodes = state.modulo.nodes
        h = _h_md5(key)
        for i in range(len(nodes) * 2):
            if len(chosen) >= r:
                break
            n = nodes[(h + i) % len(nodes)]
            picked = n.healthy and n.name not in seen
            steps.append((n.name, n.healthy, picked))
            if picked:
                chosen.append(n.name)
                seen.add(n.name)
    else:
        ring = state.ch.ring
        if not ring:
            return [], []
        h = _h_sha1(key)
        import bisect as _bi
        i = _bi.bisect_left(ring, (h, None))  # type: ignore[arg-type]
        steps_taken = 0
        max_steps = len(ring) + VNODES
        while len(chosen) < r and steps_taken < max_steps:
            if i >= len(ring):
                i = 0
            node = ring[i][1]
            picked = node.healthy and node.name not in seen
            # Only record one step per *distinct node* encountered, otherwise
            # the walk shows hundreds of vnodes — keep it readable.
            if not steps or steps[-1][0] != node.name:
                steps.append((node.name, node.healthy, picked))
            if picked:
                chosen.append(node.name)
                seen.add(node.name)
            i += 1
            steps_taken += 1
    return chosen, steps


def render_ring(state: AppState, width: int = 40, height: int = 20) -> Panel:
    """Draw a circular ring with vnode positions colored by owning node."""
    if state.mode != "ch":
        body = Align.center(
            Text("Ring view only applies to ConsistentHash mode.\n"
                 "Press 'c' to switch.", style="dim"),
            vertical="middle",
        )
        return Panel(body, title="ring", border_style="cyan")

    canvas = [[" " for _ in range(width)] for _ in range(height)]
    cx, cy = width // 2, height // 2
    rx, ry = width // 2 - 2, height // 2 - 1

    # Plot vnodes
    for h, node in state.ch.ring:
        theta = (h / (2 ** RING_BITS)) * 2 * math.pi - math.pi / 2
        x = int(cx + rx * math.cos(theta))
        y = int(cy + ry * math.sin(theta))
        if 0 <= x < width and 0 <= y < height:
            ch = "●" if node.healthy else "✗"
            canvas[y][x] = f"[{state.color(node.name)}]{ch}[/]"

    # Plot last key position
    if state.last_key:
        h = _h_sha1(state.last_key)
        theta = (h / (2 ** RING_BITS)) * 2 * math.pi - math.pi / 2
        x = int(cx + rx * math.cos(theta))
        y = int(cy + ry * math.sin(theta))
        if 0 <= x < width and 0 <= y < height:
            canvas[y][x] = "[bold white on red]K[/]"

    lines = ["".join(row) for row in canvas]
    body = Text.from_markup("\n".join(lines))
    title = f"ring (vnodes={VNODES}, sha1)"
    return Panel(Align.center(body), title=title, border_style="cyan")


def render_nodes(state: AppState) -> Panel:
    table = Table(show_header=True, header_style="bold", expand=True)
    table.add_column("#", width=3)
    table.add_column("node")
    table.add_column("health", width=8)
    table.add_column("keys", justify="right", width=8)
    table.add_column("share", justify="right", width=8)

    total = sum(len(n.store) for n in state.nodes) or 1
    for i, n in enumerate(state.nodes):
        color = state.color(n.name)
        health = "[green]UP[/]" if n.healthy else "[red]DOWN[/]"
        share = f"{len(n.store)/total*100:5.1f}%"
        table.add_row(
            str(i),
            f"[{color}]{n.name}[/]",
            health,
            str(len(n.store)),
            share,
        )
    hint = Text("press 0-5 to toggle health", style="dim")
    return Panel(Group(table, hint), title="nodes", border_style="magenta")


def render_walk(state: AppState) -> Panel:
    if not state.last_key:
        body = Text("press 'k' and type a key to trace its replica walk",
                    style="dim")
        return Panel(body, title="replica walk", border_style="yellow")

    lines: List[Text] = []
    lines.append(Text.assemble(("key:    ", "dim"),
                                (state.last_key, "bold white")))
    lines.append(Text.assemble(("hash:   ", "dim"),
                                (hex(_h_sha1(state.last_key) if state.mode == "ch"
                                    else _h_md5(state.last_key))[:18], "white")))
    chosen = Text("chosen: ", style="dim")
    for i, name in enumerate(state.last_replicas):
        if i:
            chosen.append(" → ")
        chosen.append(name, style=f"bold {state.color(name)}")
    lines.append(chosen)

    walk = Text()
    walk.append("walk:   ", style="dim")
    for i, (name, healthy, picked) in enumerate(state.last_walk_steps):
        if i:
            walk.append(" ▸ ")
        col = state.color(name)
        if picked:
            walk.append(name, style=f"bold {col} on grey23")
        elif not healthy:
            walk.append(name, style=f"strike {col}")
        else:
            walk.append(name, style=f"dim {col}")
    lines.append(walk)

    return Panel(Group(*lines), title="replica walk",
                 border_style="yellow")


def render_distribution(state: AppState) -> Panel:
    """Bar chart of where primaries land for the seeded keyset."""
    if not state.seeded_keys:
        body = Text("press 's' to seed keys and see primary distribution",
                    style="dim")
        return Panel(body, title="primary distribution", border_style="green")

    primaries = Counter()
    for k in state.seeded_keys:
        reps = state.router.pick_replicas(k, 1)
        if reps:
            primaries[reps[0].name] += 1

    if not primaries:
        return Panel(Text("no healthy nodes", style="red"),
                     title="primary distribution", border_style="green")

    mx = max(primaries.values())
    rows: List[Text] = []
    for n in state.nodes:
        cnt = primaries.get(n.name, 0)
        pct = cnt / sum(primaries.values()) * 100 if primaries else 0
        bar_len = int((cnt / mx) * 30) if mx else 0
        bar = Text()
        bar.append(f"{n.name} ", style=state.color(n.name))
        bar.append("█" * bar_len, style=state.color(n.name))
        bar.append(f" {cnt:5d}  {pct:5.1f}%", style="dim")
        rows.append(bar)
    title = f"primary distribution ({len(state.seeded_keys)} keys, {state.mode})"
    return Panel(Group(*rows), title=title, border_style="green")


def render_header(state: AppState) -> Panel:
    txt = Text()
    txt.append("mode: ", style="dim")
    txt.append(f"{state.mode.upper():<5}", style="bold cyan")
    txt.append("  R=", style="dim")
    txt.append(str(state.R), style="bold")
    txt.append("  nodes=", style="dim")
    txt.append(f"{sum(n.healthy for n in state.nodes)}/{len(state.nodes)}",
               style="bold")
    txt.append("    ")
    if state.prompt_active:
        txt.append("key> ", style="bold green")
        txt.append(state.prompt_buffer, style="bold white on grey23")
        txt.append("▌", style="bold green blink")
    else:
        txt.append(state.message, style="italic yellow")
    keys = Text(
        "  [m]odulo  [c]h  [0-5]toggle  [+/-]R  [k]ey  [s]eed  [w]ipe  [q]uit",
        style="dim",
    )
    return Panel(Group(txt, keys), border_style="white")


def render(state: AppState) -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(render_header(state), name="header", size=4),
        Layout(name="main"),
        Layout(render_walk(state), name="walk", size=8),
    )
    layout["main"].split_row(
        Layout(render_ring(state), name="ring", ratio=1),
        Layout(name="right", ratio=1),
    )
    layout["right"].split_column(
        Layout(render_nodes(state), name="nodes"),
        Layout(render_distribution(state), name="dist"),
    )
    return layout


class RawTTY:
    """Context manager: put stdin into cbreak so we get single-key reads."""
    def __enter__(self):
        self.fd = sys.stdin.fileno()
        self.old = termios.tcgetattr(self.fd)
        tty.setcbreak(self.fd)
        return self

    def __exit__(self, *a):
        termios.tcsetattr(self.fd, termios.TCSADRAIN, self.old)


def read_key_nonblocking(timeout: float = 0.1) -> Optional[str]:
    r, _, _ = select.select([sys.stdin], [], [], timeout)
    if r:
        return sys.stdin.read(1)
    return None


def seed_random_keys(state: AppState, n: int = 500) -> None:
    rng = random.Random(0xC0FFEE)
    keys = [f"k{i}-{rng.getrandbits(32)}" for i in range(n)]
    state.seeded_keys = keys
    for k in keys:
        reps = state.router.pick_replicas(k, state.R)
        for nd in reps:
            nd.put(k, f"V:{k}")


def wipe(state: AppState) -> None:
    for n in state.nodes:
        n.store.clear()
    state.seeded_keys = []


def main() -> int:
    if not sys.stdin.isatty():
        print("tui.py needs an interactive terminal", file=sys.stderr)
        return 2

    console = Console()
    state = AppState()

    with RawTTY() as raw, Live(render(state), console=console,
                                screen=True, refresh_per_second=10) as live:
        while True:
            ch = read_key_nonblocking(0.1)
            if ch is None:
                live.update(render(state))
                continue

            if state.prompt_active:
                if ch in ("\r", "\n"):
                    key = state.prompt_buffer
                    state.prompt_active = False
                    state.prompt_buffer = ""
                    if key:
                        state.last_key = key
                        state.last_replicas, state.last_walk_steps = trace_walk(state, key)
                        state.message = f"traced {key!r}"
                    else:
                        state.message = "cancelled"
                elif ch == "\x1b":  # ESC
                    state.prompt_active = False
                    state.prompt_buffer = ""
                    state.message = "cancelled"
                elif ch in ("\x7f", "\b"):  # backspace
                    state.prompt_buffer = state.prompt_buffer[:-1]
                elif ch == "\x03":  # ctrl-c
                    break
                elif ch.isprintable():
                    state.prompt_buffer += ch
                live.update(render(state))
                continue

            if ch == "q":
                break
            elif ch in "0123456789":
                idx = int(ch)
                if idx < len(state.nodes):
                    n = state.nodes[idx]
                    new_health = not n.healthy
                    state.modulo.set_health(n.name, new_health)
                    state.ch.set_health(n.name, new_health)
                    state.message = f"{n.name} -> {'UP' if new_health else 'DOWN'}"
            elif ch == "m":
                state.mode = "mod"
                state.message = "modulo router"
            elif ch == "c":
                state.mode = "ch"
                state.message = "consistent-hash router"
            elif ch in ("+", "="):
                if state.R < len(state.nodes):
                    state.R += 1
                    state.message = f"R={state.R}"
            elif ch == "-":
                if state.R > 1:
                    state.R -= 1
                    state.message = f"R={state.R}"
            elif ch == "k":
                state.prompt_active = True
                state.prompt_buffer = ""
                state.message = "enter key, ESC to cancel"
            elif ch == "s":
                seed_random_keys(state)
                state.message = f"seeded {len(state.seeded_keys)} keys (R={state.R})"
            elif ch == "w":
                wipe(state)
                state.message = "wiped stores"

            # Refresh derived data: last walk may have gone stale after health
            # flip or router switch.
            if state.last_key:
                state.last_replicas, state.last_walk_steps = trace_walk(
                    state, state.last_key)

            live.update(render(state))

    return 0


if __name__ == "__main__":
    sys.exit(main())
