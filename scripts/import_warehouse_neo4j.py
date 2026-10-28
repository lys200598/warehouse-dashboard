"""Import a warehouse100 generated package into the dedicated Neo4j service."""

from __future__ import annotations

import argparse
import getpass
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.warehouse_graph import create_schema, import_graph, load_graph_package, validation_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import the warehouse100 static knowledge graph.")
    parser.add_argument("--package-dir", type=Path, default=Path("data/generated/warehouse100-day1"))
    parser.add_argument("--layout", type=Path, default=Path("lys-warehouse-design-20261027/layout/warehouse100-v1.json"))
    parser.add_argument("--locations", type=Path, default=Path("lys-warehouse-design-20261027/layout/task-locations.csv"))
    parser.add_argument("--uri", default=os.environ.get("WAREHOUSE_NEO4J_URI", "bolt://127.0.0.1:47687"))
    parser.add_argument("--user", default=os.environ.get("WAREHOUSE_NEO4J_USER", "neo4j"))
    parser.add_argument("--database", default=os.environ.get("WAREHOUSE_NEO4J_DATABASE", "neo4j"))
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--apply", action="store_true", help="actually connect and write; omitted means offline dry-run")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    package = load_graph_package(args.package_dir, args.layout, args.locations)
    print(json.dumps({"mode": "apply" if args.apply else "dry-run", **validation_report(package)}, ensure_ascii=False, indent=2))
    if not args.apply:
        return
    try:
        from neo4j import GraphDatabase
    except ImportError as exc:
        raise SystemExit("缺少 neo4j 驱动；先运行：./.venv/bin/python -m pip install 'neo4j>=5.26,<6'") from exc
    password = os.environ.get("WAREHOUSE_NEO4J_PASSWORD") or getpass.getpass("Neo4j 密码: ")
    driver = GraphDatabase.driver(args.uri, auth=(args.user, password))
    try:
        driver.verify_connectivity()
        with driver.session(database=args.database) as session:
            create_schema(session)
            summary = import_graph(session, package, args.batch_size)
        print(json.dumps({"imported": summary, "uri": args.uri, "database": args.database}, ensure_ascii=False, indent=2))
    finally:
        driver.close()


if __name__ == "__main__":
    main()
