"""Read-only geometry/topology audit for the fixed 100-robot warehouse.

This audits a discrete planning skeleton and finite stopped-body probes. It is
not a proof of arbitrary-position fault tolerance or dynamic deadlock freedom.
All obstacle tests retain the original loaded-body collision envelope.
"""
from collections import defaultdict
import hashlib
import json
import math
from pathlib import Path

from ..day1.data import load_scene
from ..day1.nav import Navigator
from ..routing import segment_distance
from ..scheduling_v2.motion import Roadmap, RADIUS_SUM


def _components(nodes, edges, removed=frozenset(), blocked_edges=frozenset()):
    unseen = set(nodes) - removed
    components = []
    while unseen:
        root = unseen.pop()
        component, stack = {root}, [root]
        while stack:
            node = stack.pop()
            for other in edges.get(node, ()):
                if other in unseen and frozenset((node, other)) not in blocked_edges:
                    unseen.remove(other)
                    component.add(other)
                    stack.append(other)
        components.append(component)
    return sorted(components, key=len, reverse=True)


def topology_report(nodes, edges):
    """Iterative Tarjan; no recursion limit changes, deterministic JSON output."""
    discover, low, parent, children = {}, {}, {}, defaultdict(int)
    articulations, bridges, clock = set(), [], 0
    for root in nodes:
        if root in discover:
            continue
        clock += 1
        discover[root] = low[root] = clock
        stack = [(root, iter(edges.get(root, ())))]
        while stack:
            node, iterator = stack[-1]
            try:
                other = next(iterator)
            except StopIteration:
                stack.pop()
                if node not in parent:
                    if children[node] > 1:
                        articulations.add(node)
                    continue
                above = parent[node]
                low[above] = min(low[above], low[node])
                if above in parent and low[node] >= discover[above]:
                    articulations.add(above)
                if low[node] > discover[above]:
                    bridges.append([above, node])
                continue
            if other not in discover:
                parent[other] = node
                children[node] += 1
                clock += 1
                discover[other] = low[other] = clock
                stack.append((other, iter(edges.get(other, ()))))
            elif parent.get(node) != other:
                low[node] = min(low[node], discover[other])
    components = _components(nodes, edges)
    return {
        "nodes": len(nodes), "undirected_edges": sum(map(len, edges.values())) // 2,
        "component_sizes": [len(c) for c in components],
        "articulation_nodes": sorted(articulations), "bridges": sorted(bridges),
        "connected": len(components) == 1,
        "single_vertex_failure_preserves_connectivity": len(components) == 1 and not articulations,
    }


def _distance(a, b, point):
    return segment_distance((a["x"], a["y"]), (b["x"], b["y"]), (point["x"], point["y"]))


class _SegmentIndex:
    def __init__(self, segments, margin, step=2.):
        self.segments, self.step = segments, step
        self.grid = defaultdict(list)
        for token, (a, b) in segments.items():
            for x in range(math.floor((min(a["x"], b["x"]) - margin) / step),
                           math.floor((max(a["x"], b["x"]) + margin) / step) + 1):
                for y in range(math.floor((min(a["y"], b["y"]) - margin) / step),
                               math.floor((max(a["y"], b["y"]) + margin) / step) + 1):
                    self.grid[x, y].append(token)

    def blocked(self, point, clearance):
        return {token for token in self.grid.get((math.floor(point["x"] / self.step),
                                                   math.floor(point["y"] / self.step)), ())
                if _distance(*self.segments[token], point) < clearance - 1e-7}


def _terminal_groups(roadmap):
    groups = {}
    private = getattr(roadmap.base, "private", {})
    for location, point in roadmap.locations.items():
        signature = (point["x"], point["y"], location if location in private else "")
        if signature not in groups:
            groups[signature] = {"point": point, "ids": [], "links": roadmap.connections(location)}
        groups[signature]["ids"].append(location)
    return list(groups.values())


def handover_candidates(roadmap, donor_point, *, distances=(1.2, 1.35, 1.5), headings=32, limit=12):
    """Physically adjacent receiver positions with static, donor-clear access.

    The execution layer must still plan and reserve the receiver's trip, dwell,
    return and cargo transfer. These are candidates, NOT an assistance permit.
    A donor at a private bay/unsafe point may yield no candidate at all.
    """
    if headings < 4 or limit < 1 or not distances or any(not math.isfinite(d) or d < RADIUS_SUM for d in distances):
        raise ValueError("handover geometry must preserve the loaded separation")
    spec = roadmap.base.scene["spec"]
    radius = roadmap.base.radius
    node_grid = defaultdict(list)
    for node, coordinate in roadmap.nodes.items():
        node_grid[math.floor(coordinate["x"] / 2), math.floor(coordinate["y"] / 2)].append(node)
    output = []
    for distance in distances:
        for direction in range(headings):
            angle = direction * 2 * math.pi / headings
            point = {"x": donor_point["x"] + distance * math.cos(angle),
                     "y": donor_point["y"] + distance * math.sin(angle)}
            if not (radius <= point["x"] <= spec["width"] - radius and
                    radius <= point["y"] <= spec["depth"] - radius):
                continue
            if not roadmap.base.clear(point, point, set()):
                continue
            cell_x, cell_y = math.floor(point["x"] / 2), math.floor(point["y"] / 2)
            local = [node for x in range(cell_x - 2, cell_x + 3) for y in range(cell_y - 2, cell_y + 3)
                     for node in node_grid.get((x, y), ())]
            nearby = sorted(local, key=lambda k: math.hypot(roadmap.nodes[k]["x"] - point["x"],
                                                           roadmap.nodes[k]["y"] - point["y"]))[:120]
            links = []
            for node in nearby:
                if (_distance(point, roadmap.nodes[node], donor_point) >= RADIUS_SUM - 1e-7 and
                        roadmap.base.clear(point, roadmap.nodes[node], set())):
                    links.append(node)
                    if len(links) == 4:
                        break
            if links:
                output.append({"point": point, "distance_m": distance, "connections": links})
    # A short connection helps avoid manufacturing huge detours for a nominally
    # adjacent point. Keep several heading options, not just the first quadrant.
    output.sort(key=lambda item: (min(math.hypot(roadmap.nodes[k]["x"] - item["point"]["x"],
                                                 roadmap.nodes[k]["y"] - item["point"]["y"])
                                       for k in item["connections"]), item["distance_m"],
                                 item["point"]["x"], item["point"]["y"]))
    return output[:limit]


def audit_handover_geometry(roadmap, *, distances=(1.2, 1.5), headings=16):
    """Finite geometry coverage for all unique business approach positions."""
    business = {}
    for name, point in roadmap.locations.items():
        if name.startswith(("R", "IN-", "OUT-")):
            business.setdefault((point["x"], point["y"]), {"point": point, "ids": []})["ids"].append(name)
    missing, examples = [], []
    for record in business.values():
        candidates = handover_candidates(roadmap, record["point"], distances=distances, headings=headings, limit=4)
        if not candidates:
            missing.append(record)
        if len(examples) < 6 or record["ids"][0].startswith(("IN-", "OUT-")):
            examples.append({"donor": record, "candidates": candidates})
    return {"schema": "warehouse-adjacent-handover-geometry-v3",
            "settings": {"headings": headings, "center_distances_m": list(distances),
                         "required_separation_m": RADIUS_SUM},
            "counts": {"tested": len(business), "with_candidate": len(business) - len(missing)},
            "missing": missing, "examples": examples,
            "limitations": ["Static candidate geometry is not a dynamic path/energy/reservation guarantee",
                            "Does not cover arbitrary intermediate points or private parking/charging bays"]}


def _circle_audit(roadmap, terminal_groups, points):
    edges = {frozenset((node, other)): (roadmap.nodes[node], roadmap.nodes[other])
             for node, neighbors in roadmap.edges.items() for other in neighbors}
    edge_index = _SegmentIndex(edges, RADIUS_SUM)
    connectors = {(index, node): (group["point"], roadmap.nodes[node])
                  for index, group in enumerate(terminal_groups) for node in group["links"]}
    connector_index = _SegmentIndex(connectors, RADIUS_SUM)
    node_segments = {node: (point, point) for node, point in roadmap.nodes.items()}
    node_index = _SegmentIndex(node_segments, RADIUS_SUM)
    split_cases, isolated_cases, examples = 0, 0, []
    isolated_by_type = defaultdict(int)
    business_examples = []
    max_outside, max_isolated, max_affected = 0, 0, 0
    for point in points:
        removed = node_index.blocked(point, RADIUS_SUM)
        blocked = edge_index.blocked(point, RADIUS_SUM)
        components = _components(roadmap.nodes, roadmap.edges, removed, blocked)
        largest = components[0] if components else set()
        outside = sum(len(c) for c in components[1:])
        split_cases += bool(outside)
        max_outside = max(max_outside, outside)
        blocked_connectors = connector_index.blocked(point, RADIUS_SUM)
        # When the public graph splits, evaluate every terminal against its
        # largest remaining component; otherwise only nearby connectors matter.
        candidates = (range(len(terminal_groups)) if outside else
                      {index for index, node in blocked_connectors})
        directly_affected, isolated = [], []
        for index in candidates:
            group = terminal_groups[index]
            if math.hypot(group["point"]["x"] - point["x"], group["point"]["y"] - point["y"]) < RADIUS_SUM - 1e-7:
                directly_affected.extend(group["ids"])
            elif not any(node in largest and (index, node) not in blocked_connectors for node in group["links"]):
                isolated.extend(group["ids"])
        max_affected = max(max_affected, len(directly_affected))
        max_isolated = max(max_isolated, len(isolated))
        isolated_cases += bool(isolated)
        for prefix, category in [("P", "homes"), ("C-", "docks"), ("R", "rack_slots"),
                                 ("IN-", "inbound_ports"), ("OUT-", "outbound_ports")]:
            isolated_by_type[category] += any(location.startswith(prefix) for location in isolated)
        if any(location.startswith(("R", "IN-", "OUT-")) for location in isolated) and len(business_examples) < 8:
            business_examples.append({"point": dict(point), "unaffected_terminal_ids_isolated": isolated[:24]})
        if (outside or isolated) and len(examples) < 12:
            examples.append({"point": dict(point), "remaining_component_sizes": [len(c) for c in components],
                             "unaffected_terminal_ids_isolated": isolated[:24],
                             "directly_occupied_terminal_ids": directly_affected[:24]})
    return {
        "probe_count": len(points), "combined_exclusion_radius_m": RADIUS_SUM,
        "coverage": "all roadmap node centers and unique terminal approach points; not arbitrary continuous positions or multiple simultaneous faults",
        "graph_split_cases": split_cases, "max_nodes_outside_largest_component": max_outside,
        "unaffected_terminal_isolation_cases": isolated_cases,
        "max_unaffected_terminal_ids_isolated": max_isolated,
        "max_directly_occupied_terminal_ids": max_affected,
        "all_probes_preserve_unaffected_terminal_access": isolated_cases == 0,
        "isolation_probe_counts_by_terminal_type": dict(isolated_by_type),
        "business_isolation_examples": business_examples, "examples": examples,
        "interpretation": "Occupied task points may be temporarily unavailable; isolation is a conservative roadmap result, not a continuous-space impossibility proof.",
    }


def audit_roadmap(roadmap, *, circle_probes=True):
    """Audit a roadmap without changing scene, edges, or execution reservations."""
    topology = topology_report(roadmap.nodes, roadmap.edges)
    components = _components(roadmap.nodes, roadmap.edges)
    main = components[0] if components else set()
    groups = _terminal_groups(roadmap)
    unreachable = [location for group in groups if not any(k in main for k in group["links"])
                   for location in group["ids"]]
    bad_connectors = []
    for group in groups:
        location = group["ids"][0]
        for node in group["links"]:
            if not roadmap.base.clear(group["point"], roadmap.nodes[node], {location}):
                bad_connectors.append([location, node])
    bad_edges = [[node, other] for node, neighbors in roadmap.edges.items() for other in neighbors
                 if node < other and not roadmap.base.clear(roadmap.nodes[node], roadmap.nodes[other], set())]
    result = {
        "schema": "warehouse-layout-safety-audit-v3", "topology": topology,
        "terminals": {"location_ids": len(roadmap.locations), "unique_approach_points": len(groups),
                      "unreachable_ids": unreachable, "unsafe_connectors": bad_connectors},
        "unsafe_public_edges": bad_edges,
        "initial_roadmap_usable": topology["connected"] and not unreachable and not bad_connectors and not bad_edges,
        "limitations": ["No dynamic liveness/deadlock proof", "No arbitrary-position or multi-fault robustness proof",
                        "A coarse roadmap may conservatively reject a continuous-space route"],
    }
    if circle_probes:
        points = {(round(p["x"], 9), round(p["y"], 9)): p for p in roadmap.nodes.values()}
        points.update({(round(group["point"]["x"], 9), round(group["point"]["y"], 9)): group["point"] for group in groups})
        result["stopped_body_probes"] = _circle_audit(roadmap, groups, list(points.values()))
    return result


def audit_layout(scene_path, *, circle_probes=True, handover_probes=False):
    """Construct the original geometry/roadmap and emit JSON-serializable evidence."""
    scene, slots = load_scene(scene_path)
    navigator = Navigator(scene, slots)
    roadmap = Roadmap(navigator)
    report = audit_roadmap(roadmap, circle_probes=circle_probes)
    spec, bays = scene["spec"], scene["homes"] + scene["docks"]
    radius = navigator.radius
    loaded_radius = math.hypot(spec["robotBodyWidth"] / 2, spec["robotBodyDepth"] / 2)
    bad_bays = [bay["id"] for bay in bays
                if not (bay["x"] + radius <= bay["point"]["x"] <= bay["x"] + bay["w"] - radius and
                        bay["y"] + radius <= bay["point"]["y"] <= bay["y"] + bay["d"] - radius)]
    # Public roads must not cross a fully occupied home or charging pad's
    # loaded-body safety envelope, even when there is no moving reservation.
    public_edges = [(roadmap.nodes[node], roadmap.nodes[other]) for node, neighbors in roadmap.edges.items()
                    for other in neighbors if node < other]
    index = _SegmentIndex({i: pair for i, pair in enumerate(public_edges)}, RADIUS_SUM)
    intrusions = [{"bay": bay["id"], "public_edge_count": len(index.blocked(bay["point"], RADIUS_SUM))}
                  for bay in bays if index.blocked(bay["point"], RADIUS_SUM)]
    nearest = min((math.hypot(a["point"]["x"] - b["point"]["x"], a["point"]["y"] - b["point"]["y"])
                   for i, a in enumerate(bays) for b in bays[i + 1:]), default=math.inf)
    report.update({
        "scene_path": str(Path(scene_path).resolve()),
        "scene_sha256": hashlib.sha256(Path(scene_path).read_bytes()).hexdigest(),
        "scene_version": spec["version"],
        "counts": {"homes": len(scene["homes"]), "docks": len(scene["docks"]), "slots": len(slots),
                   "boundary_stations": len(scene["stations"])},
        "geometry": {"planning_radius_m": radius, "loaded_rectangle_circumradius_m": loaded_radius,
                     "planning_envelope_contains_loaded_rectangle": radius >= loaded_radius,
                     "bays_not_containing_body": bad_bays,
                     "nearest_bay_center_distance_m": nearest,
                     "parked_bodies_keep_required_separation": nearest >= RADIUS_SUM - 1e-7,
                     "parked_body_public_edge_intrusions": intrusions,
                     "minimum_declared_trunk_width_m": min(min(t["w"], t["d"]) for t in scene["trunks"])},
    })
    report["initial_layout_usable"] = (report["initial_roadmap_usable"] and radius >= loaded_radius and
                                       not bad_bays and not intrusions and nearest >= RADIUS_SUM - 1e-7)
    if handover_probes:
        report["handover_geometry"] = audit_handover_geometry(roadmap)
    return report


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--scene", required=True)
    parser.add_argument("--output")
    parser.add_argument("--skip-circle-probes", action="store_true")
    parser.add_argument("--handover-probes", action="store_true")
    args = parser.parse_args()
    value = json.dumps(audit_layout(args.scene, circle_probes=not args.skip_circle_probes,
                                   handover_probes=args.handover_probes), ensure_ascii=False, indent=2) + "\n"
    if args.output:
        target = Path(args.output)
        if target.exists():
            raise FileExistsError("audit output exists; preserve earlier evidence")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(value, encoding="utf-8")
    else:
        print(value)
