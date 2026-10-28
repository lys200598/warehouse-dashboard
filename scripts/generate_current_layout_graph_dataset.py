"""Generate a graph package that exactly matches the unchanged demo warehouse."""
from __future__ import annotations

import csv
import json
from pathlib import Path

from generate_warehouse100_dataset import generate


ROOT = Path(__file__).resolve().parents[1]
LAYOUT_DIR = ROOT / "data/generated/current-layout"


def build_layout() -> tuple[Path, Path]:
    LAYOUT_DIR.mkdir(parents=True, exist_ok=True)
    racks, locations = [], []
    rack_id = 1
    for column, x in enumerate([8.0, 13.0, 18.0, 23.0, 28.0]):
        for row, y in enumerate([4.3, 9.4, 14.5]):
            rid = f"R{rack_id:02d}"; racks.append({"id": rid, "x": x, "y": y, "w": 2.8, "d": 1.15, "kind": "rack", "capacity": 36, "zone": chr(65 + row), "column": column, "row": row})
            for level in range(1, 4):
                for depth in range(1, 3):
                    for bay in range(1, 7):
                        locations.append({"location_id": f"{rid}-L{level}-C{bay}-D{depth}", "kind": "slot", "rack_id": rid, "level": level, "face": depth, "bay": bay, "x": x + bay * .35, "y": y + depth * .35})
            rack_id += 1
    for kind, y in (("inbound", 19.0), ("outbound", .9)):
        for index in range(1, 9): locations.append({"location_id": f"{'IN' if kind == 'inbound' else 'OUT'}-{index}", "kind": kind, "rack_id": "", "level": "", "face": "", "bay": "", "x": 1.5 + index * 3, "y": y})
    layout = {"spec": {"version": "warehouse-demo-layout-v1", "width": 32, "depth": 22, "slotsPerRack": 36, "robotCount": 100}, "racks": racks}
    layout_path, locations_path = LAYOUT_DIR / "layout.json", LAYOUT_DIR / "task-locations.csv"
    layout_path.write_text(json.dumps(layout, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    with locations_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=("location_id", "kind", "rack_id", "level", "face", "bay", "x", "y")); writer.writeheader(); writer.writerows(locations)
    return layout_path, locations_path


if __name__ == "__main__":
    layout, locations = build_layout()
    print(json.dumps(generate(ROOT / "data/generated/current-layout-day1", layout, locations, __import__("datetime").date(2026, 10, 27), 20261027, 4, True), ensure_ascii=False, indent=2))
