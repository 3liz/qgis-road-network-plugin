#!/usr/bin/env python3
"""
Create the diagrams of the documentation page docs/user-guide/graph-checks.md:
for each check of the road graph, the issue and its fix, in the style of references_impacts.svg.

Usage, from the repository root:

    python3 .docker/graph_checks_diagrams.py docs/media/user-guide

or with `make graph-checks-screenshots`.
"""

import math
import sys

from itertools import pairwise
from pathlib import Path

GREY = "#9e9e9e"
OTHER = "#78909c"
ISSUE = "#e53935"
FIXED = "#1565c0"
TEXT = "#212121"
MUTED = "#424242"
WIDTH = 900
HEIGHT = 250


class Panel:
    """A panel of a diagram: a title, and drawings in a local coordinate system."""

    def __init__(self, title: str, x0: float, width: float):
        """Create an empty panel.

        :param title: The title displayed at the top of the panel
        :param x0: The horizontal position of the panel in the diagram
        :param width: The width of the panel
        """
        self.title = title
        self.x0 = x0
        self.width = width
        self.items = []

    def line(
        self, points: list, color: str = GREY, width: float = 7, dash: str | None = None, arrow: bool = False
    ):
        """Add a polyline, with an optional arrow in its middle showing its direction.

        :param points: The points of the line, as (x, y) tuples in the panel
        :param color: The color of the line
        :param width: The width of the line
        :param dash: The dash array of the line, if dashed
        :param arrow: If an arrow is drawn on the longest segment, in the direction of the line
        """
        pts = " ".join(f"{self.x0 + x},{y}" for x, y in points)
        dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
        self.items.append(
            f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="{width}" '
            f'stroke-linejoin="round" stroke-linecap="round"{dash_attr}/>'
        )
        if arrow:
            # Arrow at the middle of the longest segment
            segments = list(pairwise(points))
            (x1, y1), (x2, y2) = max(segments, key=lambda s: math.dist(s[0], s[1]))
            self.arrow((x1 + x2) / 2, (y1 + y2) / 2, math.atan2(y2 - y1, x2 - x1), color)

    def arrow(self, x: float, y: float, angle: float, color: str):
        """Add a triangle showing a direction.

        :param x: The horizontal position of the triangle in the panel
        :param y: The vertical position of the triangle
        :param angle: The direction, in radians
        :param color: The color of the triangle border
        """
        size = 9
        pts = []
        for da, r in ((0, size), (2.5, size), (-2.5, size)):
            pts.append((x + r * math.cos(angle + da), y + r * math.sin(angle + da)))
        pts_str = " ".join(f"{self.x0 + px:.1f},{py:.1f}" for px, py in pts)
        self.items.append(f'<polygon points="{pts_str}" fill="#ffffff" stroke="{color}" stroke-width="2"/>')

    def node(self, x: float, y: float, color: str = "#ffffff", stroke: str = MUTED, r: float = 6):
        """Add a node of the graph.

        :param x: The horizontal position of the node in the panel
        :param y: The vertical position of the node
        :param color: The fill color
        :param stroke: The border color
        :param r: The radius
        """
        self.items.append(
            f'<circle cx="{self.x0 + x}" cy="{y}" r="{r}" fill="{color}" stroke="{stroke}" stroke-width="2"/>'
        )

    def marker(
        self,
        x: float,
        y: float,
        label: str,
        color: str = "#2e7d32",
        stroke: str = "#1b5e20",
        dash: bool = False,
    ):
        """Add a marker (PR) as a flag above the road.

        :param x: The horizontal position of the marker in the panel
        :param y: The vertical position of the road
        :param label: The label of the marker, or an empty string
        :param color: The fill color of the flag
        :param stroke: The border color of the flag and the color of the label
        :param dash: If the flag is dashed and not filled, for example for the old position of the marker
        """
        x = self.x0 + x
        fill = "none" if dash else color
        dash_attr = ' stroke-dasharray="3,2"' if dash else ""
        self.items.append(
            f'<path d="M {x} {y - 10} L {x} {y - 30} L {x + 16} {y - 24} L {x} {y - 18}" '
            f'fill="{fill}" stroke="{stroke}"{dash_attr}/>'
        )
        if label:
            self.items.append(
                f'<text x="{x + 6}" y="{y - 36}" font-size="13" font-weight="bold" fill="{stroke}" '
                f'text-anchor="middle">{label}</text>'
            )

    def text(
        self,
        x: float,
        y: float,
        text: str,
        size: float = 12.5,
        color: str = TEXT,
        anchor: str = "middle",
        weight: str = "normal",
    ):
        """Add a text.

        :param x: The horizontal position of the text in the panel
        :param y: The vertical position of the text
        :param text: The text
        :param size: The font size
        :param color: The color
        :param anchor: The text anchor: start, middle or end
        :param weight: The font weight
        """
        self.items.append(
            f'<text x="{self.x0 + x}" y="{y}" font-size="{size}" fill="{color}" text-anchor="{anchor}" '
            f'font-weight="{weight}">{text}</text>'
        )

    def cross(self, x: float, y: float, color: str = ISSUE):
        """Add a cross showing a point without node.

        :param x: The horizontal position of the cross in the panel
        :param y: The vertical position of the cross
        :param color: The color of the cross
        """
        x = self.x0 + x
        self.items.append(
            f'<path d="M {x - 7} {y - 7} L {x + 7} {y + 7} M {x - 7} {y + 7} L {x + 7} {y - 7}" '
            f'stroke="{color}" stroke-width="3"/>'
        )

    def svg(self) -> str:
        """SVG elements of the panel.

        :return: The SVG elements of the title and of the drawings
        """
        title = (
            f'<text x="{self.x0 + self.width / 2}" y="28" font-size="15" font-weight="bold" '
            f'fill="{TEXT}" text-anchor="middle">{self.title}</text>'
        )
        return "\n  ".join([title, *self.items])


def diagram(panels: list, height: float = HEIGHT) -> str:
    """SVG of a diagram made of panels separated by vertical lines.

    :param panels: The panels of the diagram, from left to right
    :param height: The height of the diagram
    :return: The SVG document
    """
    separators = [
        f'<line x1="{p.x0}" y1="12" x2="{p.x0}" y2="{height - 12}" stroke="#e0e0e0" stroke-width="2"/>'
        for p in panels[1:]
    ]
    body = "\n  ".join([*separators, *(p.svg() for p in panels)])
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{height}" '
        f'viewBox="0 0 {WIDTH} {height}" font-family="Arial, Helvetica, sans-serif">\n'
        f'  <rect width="{WIDTH}" height="{height}" fill="#ffffff"/>\n  {body}\n</svg>\n'
    )


def panels(*titles: str) -> list:
    """Create panels of the same width, from left to right.

    :param titles: The titles of the panels
    :return: The panels
    """
    width = WIDTH / len(titles)
    return [Panel(title, i * width, width) for i, title in enumerate(titles)]


def g01() -> str:
    """Diagram of the check G01.

    :return: The SVG of the diagram
    """
    issue, cut, bridge = panels("Issue", "cut_edges", "mark_as_bridge")
    # Issue: two roads crossing without node
    issue.line([(30, 130), (270, 130)])
    issue.line([(150, 50), (150, 210)], OTHER)
    for x, y in ((30, 130), (270, 130), (150, 50), (150, 210)):
        issue.node(x, y)
    issue.cross(150, 130)
    issue.text(150, 236, "the edges cross each other without node")
    # cut_edges: a node splits the two edges
    cut.line([(30, 130), (150, 130)], FIXED)
    cut.line([(150, 130), (270, 130)], FIXED)
    cut.line([(150, 50), (150, 130)], FIXED)
    cut.line([(150, 130), (150, 210)], FIXED)
    for x, y in ((30, 130), (270, 130), (150, 50), (150, 210)):
        cut.node(x, y)
    cut.node(150, 130, FIXED, "#0d47a1", 8)
    cut.text(150, 236, "a crossroads: the 2 edges are split in 4")
    # mark_as_bridge: the edges are not connected
    bridge.line([(30, 130), (270, 130)])
    bridge.line([(150, 50), (150, 210)], FIXED)
    bridge.line([(138, 112), (138, 148)], MUTED, 2)
    bridge.line([(162, 112), (162, 148)], MUTED, 2)
    for x, y in ((30, 130), (270, 130), (150, 50), (150, 210)):
        bridge.node(x, y)
    bridge.text(150, 236, "a bridge: no_intersection_cutting = True")
    return diagram([issue, cut, bridge])


def g02() -> str:
    """Diagram of the check G02.

    :return: The SVG of the diagram
    """
    issue, fixed = panels("Issue", "extend_edge")
    for p, extended in ((issue, False), (fixed, True)):
        if extended:
            p.line([(40, 150), (225, 150)], FIXED)
            p.line([(225, 150), (410, 150)], FIXED)
            p.line([(225, 60), (225, 150)], FIXED)
            p.node(225, 150, FIXED, "#0d47a1", 8)
        else:
            p.line([(40, 150), (410, 150)])
            p.line([(225, 60), (225, 128)], OTHER)
            p.line([(225, 128), (225, 150)], ISSUE, 3, "5,4")
            p.node(225, 128)
        p.node(40, 150)
        p.node(410, 150)
        p.node(225, 60)
    issue.text(225, 200, "the road stops less than 2 m before the other road")
    issue.text(225, 218, "(T junction), without being connected to it")
    fixed.text(225, 200, "the edge is extended to the other road,")
    fixed.text(225, 218, "which is split by the node")
    return diagram([issue, fixed])


def g03() -> str:
    """Diagram of the check G03.

    :return: The SVG of the diagram
    """
    issue, fixed = panels("Issue", "merge_nodes")
    issue.line([(40, 140), (215, 140)])
    issue.line([(232, 152), (410, 152)], OTHER)
    issue.line([(232, 152), (232, 60)], OTHER)
    issue.node(40, 140)
    issue.node(410, 152)
    issue.node(232, 60)
    issue.node(215, 140, "#ffffff", ISSUE)
    issue.node(232, 152, ISSUE, ISSUE)
    issue.text(225, 200, "two nodes less than 1 m from each other:")
    issue.text(225, 218, "the roads are not connected")
    fixed.line([(40, 140), (215, 140)])
    fixed.line([(215, 140), (410, 152)], FIXED)
    fixed.line([(215, 140), (232, 60)], FIXED)
    fixed.node(40, 140)
    fixed.node(410, 152)
    fixed.node(232, 60)
    fixed.node(215, 140, FIXED, "#0d47a1", 8)
    fixed.text(225, 200, "the edges of the other node are moved to the node")
    fixed.text(225, 218, "with the most edges, then the other node is deleted")
    return diagram([issue, fixed])


def g04() -> str:
    """Diagram of the check G04.

    :return: The SVG of the diagram
    """
    issue, fixed = panels("Issue", "delete_node")
    issue.line([(40, 130), (225, 130)], GREY, arrow=True)
    issue.line([(225, 130), (410, 130)], GREY, arrow=True)
    issue.node(40, 130)
    issue.node(410, 130)
    issue.node(225, 130, ISSUE, ISSUE)
    issue.text(132, 160, "e1  D12", 13, MUTED)
    issue.text(318, 160, "e2  D12", 13, MUTED)
    issue.text(225, 205, "a node between two edges of the same road,")
    issue.text(225, 223, "one arriving and the other one leaving it")
    fixed.line([(40, 130), (410, 130)], FIXED, arrow=True)
    fixed.node(40, 130)
    fixed.node(410, 130)
    fixed.text(225, 160, "e1  D12", 13, MUTED)
    fixed.text(225, 205, "the node is deleted:")
    fixed.text(225, 223, "the two edges are merged into the edge with the lowest id")
    return diagram([issue, fixed])


def g05() -> str:
    """Diagram of the check G05.

    :return: The SVG of the diagram
    """
    issue, fixed = panels("Issue", "reverse_edges")
    for p, color in ((issue, ISSUE), (fixed, FIXED)):
        xs = [30, 130, 230, 330, 420]
        for i in range(4):
            x1, x2 = xs[i], xs[i + 1]
            reversed_edge = i in (1, 2) and p is issue
            c = color if i in (1, 2) else GREY
            pts = [(x2, 140), (x1, 140)] if reversed_edge else [(x1, 140), (x2, 140)]
            p.line(pts, c, arrow=True)
        for x in xs:
            p.node(x, 140)
        for x, label in ((30, "PR 0"), (180, "PR 1"), (380, "PR 2")):
            p.marker(x, 140, label)
    issue.text(225, 190, "2 edges in the opposite direction of the road:")
    issue.text(225, 208, "the markers PR 0, PR 1, PR 2 give the direction")
    fixed.text(225, 190, "the edges are reversed: the triggers swap their nodes,")
    fixed.text(225, 208, "and calculate again the references of the road")
    return diagram([issue, fixed])


def g06() -> str:
    """Diagram of the check G06.

    :return: The SVG of the diagram
    """
    issue, fixed = panels("Issue", "Manual fix")
    for p in (issue, fixed):
        p.line([(40, 150), (225, 150)], GREY, arrow=True)
        p.line([(225, 150), (410, 150)], GREY, arrow=True)
        branch = ISSUE if p is issue else FIXED
        p.line([(225, 150), (330, 70)], branch, arrow=True)
        p.node(40, 150)
        p.node(410, 150)
        p.node(330, 70)
        p.node(225, 150, ISSUE if p is issue else "#ffffff", ISSUE if p is issue else MUTED)
        p.text(132, 175, "D12", 13, MUTED)
        p.text(318, 175, "D12", 13, MUTED)
    issue.text(310, 120, "D12", 13, ISSUE, weight="bold")
    fixed.text(330, 120, "D12_ramp", 13, FIXED, weight="bold")
    issue.text(225, 210, "3 edges of the road D12 at the node: the branch")
    issue.text(225, 228, "is the edge out of the most aligned edges")
    fixed.text(225, 210, "the branch gets its own road code")
    return diagram([issue, fixed])


def g07() -> str:
    """Diagram of the check G07.

    :return: The SVG of the diagram
    """
    issue, fixed = panels("Issue", "Manual fix")
    for p in (issue, fixed):
        second = ISSUE if p is issue else FIXED
        p.line([(40, 100), (410, 100)], GREY, arrow=True)
        p.line([(410, 140), (40, 140)], second, arrow=True)
        for x, y in ((40, 100), (410, 100), (40, 140), (410, 140)):
            p.node(x, y)
        p.text(225, 88, "D12", 13, MUTED)
    issue.text(225, 162, "D12", 13, ISSUE, weight="bold")
    fixed.text(225, 162, "D12_G", 13, FIXED, weight="bold")
    issue.line([(380, 104), (380, 136)], MUTED, 1.5, "3,3")
    issue.text(392, 124, "&lt; 30 m", 12, MUTED, "start")
    issue.text(225, 200, "two edges of the same road, not connected,")
    issue.text(225, 218, "running along each other: two carriageways")
    fixed.text(225, 200, "one of the carriageways gets its own road code")
    return diagram([issue, fixed])


def g08() -> str:
    """Diagram of the check G08.

    :return: The SVG of the diagram
    """
    issue, fixed = panels("Issue", "collapse_edge")
    for p in (issue, fixed):
        p.line([(40, 120), (210, 120)], GREY, arrow=True)
        p.line([(210, 50), (210, 120)], OTHER)
        p.text(120, 145, "D12", 13, MUTED)
        p.text(330, 145, "D12", 13, MUTED)
    issue.line([(210, 120), (240, 120)], ISSUE, 7)
    issue.line([(240, 120), (410, 120)], GREY, arrow=True)
    issue.line([(240, 120), (240, 180)], OTHER)
    for x, y in ((40, 120), (410, 120), (210, 50), (240, 180), (210, 120)):
        issue.node(x, y)
    issue.node(240, 120, "#ffffff", ISSUE)
    issue.text(225, 105, "2 m", 12, ISSUE, weight="bold")
    issue.text(225, 210, "two junctions 2 m from each other,")
    issue.text(225, 228, "linked by a short edge of the road D12")
    fixed.line([(210, 120), (240, 120), (410, 120)], FIXED, arrow=True)
    fixed.line([(210, 120), (240, 180)], FIXED)
    for x, y in ((40, 120), (410, 120), (210, 50), (240, 180)):
        fixed.node(x, y)
    fixed.node(210, 120, FIXED, "#0d47a1", 8)
    fixed.text(225, 210, "the short edge is added to the next edge of D12,")
    fixed.text(225, 228, "the edge of the other road is moved to the kept node")
    return diagram([issue, fixed])


def g09() -> str:
    """Diagram of the check G09.

    :return: The SVG of the diagram
    """
    issue, fixed = panels("Issue", "move_marker")
    for p in (issue, fixed):
        p.line([(40, 150), (410, 150)], GREY, arrow=True)
        p.node(40, 150)
        p.node(410, 150)
    issue.line([(40, 150), (90, 150)], ISSUE, 7)
    issue.node(40, 150)
    issue.marker(90, 150, "PR 0")
    issue.text(65, 172, "&lt; 1 m", 12, ISSUE, weight="bold")
    issue.text(225, 200, "the first marker of the road is just after the start")
    issue.text(225, 218, "of the first edge: the start has no reference")
    fixed.marker(90, 150, "", dash=True)
    fixed.marker(40, 150, "PR 0", FIXED, "#0d47a1")
    fixed.text(225, 200, "the marker is moved to the start of the road,")
    fixed.text(225, 218, "the references of the road are calculated again")
    return diagram([issue, fixed])


def g10() -> str:
    """Diagram of the check G10.

    :return: The SVG of the diagram
    """
    issue, fixed = panels("Issue", "Manual fix")
    for p in (issue, fixed):
        p.line([(30, 150), (420, 150)], GREY, arrow=True)
        p.node(30, 150)
        p.node(420, 150)
        p.marker(30, 150, "PR 0")
        p.marker(120, 150, "PR 1")
        p.marker(390, 150, "PR 4")
    issue.marker(210, 150, "PR 3", ISSUE, "#b71c1c")
    issue.marker(300, 150, "PR 2")
    fixed.marker(210, 150, "", dash=True)
    fixed.marker(300, 150, "PR 2")
    fixed.marker(345, 150, "PR 3", FIXED, "#0d47a1")
    issue.text(225, 200, "PR 3 is before PR 2 along the road:")
    issue.text(225, 218, "it is out of the longest sequence of markers in order")
    fixed.text(225, 200, "the marker is moved, or its code is changed")
    return diagram([issue, fixed])


def main() -> int:
    """Write the diagrams in the directory given as argument.

    :return: The exit code
    """
    output_dir = Path(sys.argv[1])
    for name, function in (
        ("g01", g01),
        ("g02", g02),
        ("g03", g03),
        ("g04", g04),
        ("g05", g05),
        ("g06", g06),
        ("g07", g07),
        ("g08", g08),
        ("g09", g09),
        ("g10", g10),
    ):
        (output_dir / f"graph_checks_{name}.svg").write_text(function())
    return 0


if __name__ == "__main__":
    sys.exit(main())
