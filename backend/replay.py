"""Replay one retained journal run into Neo4j, without running robots."""
import argparse
import json
from .knowledge import build_knowledge_store


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal-path", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--batch-size", type=int, default=25)
    args = parser.parse_args()
    if not 1 <= args.batch_size <= 100:
        parser.error("batch-size must be 1..100")
    store = build_knowledge_store("neo4j", args.journal_path)
    try:
        if store.latest_checkpoint(args.run_id) is None:
            raise ValueError("journal run not found")
        store.flush(args.run_id, batch_size=args.batch_size)
        status = store.status(args.run_id)
        print(json.dumps(status))
        if status["lag"] or status["last_error"]:
            raise RuntimeError("replay incomplete")
    finally:
        store.close()


if __name__ == "__main__":
    main()
