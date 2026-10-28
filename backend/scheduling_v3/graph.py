"""Additive run-scoped v3 assistance edges; custody stays in DayOneMirror.

All writes use the same Neo4j transaction as the existing durable commit. Old
commit replays still validate their checksum, but never rewrite collaboration
edges/properties or regress mutable cargo locations.
"""
from ..day1.graph import DayOneMirror
from ..knowledge import KnowledgeError


RECOVERY_KINDS = {"traffic_retimed", "traffic_rerouted", "robot_stopped", "assistance_requested"}


def _robot_id(value):
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 100:
        raise ValueError("v3 collaboration requires robot IDs 1..100")
    return value


def _participant(event, role):
    primary, alias = event.get(role + "_robot_id"), event.get(role + "_id")
    if primary is not None and alias is not None and primary != alias:
        raise ValueError("conflicting collaboration participant aliases")
    return _robot_id(primary if primary is not None else alias)


def _handoff_row(run, event):
    donor, receiver = _participant(event, "donor"), _participant(event, "receiver")
    if donor == receiver:
        raise ValueError("cargo handoff needs two distinct robots")
    if not event.get("task_id") or not event.get("goods_id"):
        raise ValueError("cargo handoff missing task/cargo identity")
    if event.get("location_id") != f"ROBOT-{receiver}":
        raise ValueError("cargo handoff custody must name the receiver")
    return {"event_key": event["event_id"], "donor_robot_id": donor, "receiver_robot_id": receiver,
            "donor_key": f"{run}:{donor}", "receiver_key": f"{run}:{receiver}",
            "task_key": f"{run}:{event['task_id']}", "cargo_key": f"{run}:{event['goods_id']}"}


class V3Mirror(DayOneMirror):
    @staticmethod
    def check_batches(batches):
        if not batches:
            return
        DayOneMirror.check_batches(batches)
        run = batches[0]["run_id"]
        if any(batch["run_id"] != run for batch in batches):
            raise KnowledgeError("mixed runs in v3 graph transaction")
        for batch in batches:
            for event in batch["events"]:
                if event["kind"] == "cargo_handoff":
                    _handoff_row(run, event)
                if event["kind"] in RECOVERY_KINDS:
                    affected = event.get("affected_robot_ids", event.get("affected_robots",
                        [event["robot_id"]] if event.get("robot_id") else []))
                    if not isinstance(affected, (list, tuple)):
                        raise ValueError("affected_robot_ids must be an array")
                    for owner in affected:
                        _robot_id(owner)

    @staticmethod
    def _apply(tx, batches):
        if not batches:
            return
        V3Mirror.check_batches(batches)
        run = batches[0]["run_id"]
        current = tx.run("MATCH (r:WHRun {run_id:$run}) RETURN r.committed_seq AS seq", run=run).single()
        head = int(current["seq"] or 0) if current else 0
        # Parent validates sequence/checksums and performs custody/history writes.
        DayOneMirror._apply(tx, batches)
        forward = [batch for batch in batches if batch["seq"] > head]
        if not forward:
            return
        handoffs, recovery = [], []
        for batch in forward:
            for event in batch["events"]:
                if event["kind"] == "cargo_handoff":
                    handoffs.append(_handoff_row(run, event))
                elif event["kind"] in RECOVERY_KINDS:
                    recovery.append({"event_key": event["event_id"], "kind": event["kind"],
                                     "reason": event.get("reason"),
                                     "affected_robot_ids": sorted(set(event.get("affected_robot_ids", event.get("affected_robots",
                                         [event["robot_id"]] if event.get("robot_id") else []))))})
        if handoffs:
            tx.run("""UNWIND $rows AS p
                MATCH (e:WHEvent {key:p.event_key}), (d:WHRobot {key:p.donor_key}),
                      (a:WHRobot {key:p.receiver_key}), (t:WHTask {key:p.task_key}), (g:WHCargo {key:p.cargo_key})
                SET e.donor_robot_id=p.donor_robot_id,e.receiver_robot_id=p.receiver_robot_id
                MERGE (e)-[:DONOR]->(d) MERGE (e)-[:RECEIVER]->(a)
                MERGE (e)-[:ABOUT_TASK]->(t) MERGE (e)-[:TRANSFERS]->(g)""", rows=handoffs).consume()
        if recovery:
            tx.run("""UNWIND $rows AS p MATCH (e:WHEvent {key:p.event_key})
                SET e.affected_robot_ids=p.affected_robot_ids,e.reason=p.reason
                WITH e,p UNWIND p.affected_robot_ids AS owner
                MATCH (a:WHRobot {key:$run+':'+toString(owner)})
                MERGE (e)-[:AFFECTS]->(a)""", rows=recovery, run=run).consume()


def audit_helpers(mirror, run_id, *, expected_handoffs=None):
    """Index-anchored, run-bounded handoff linkage audit, no schema mutation.

    Does not compare cargo *current* location with the receiver: the cargo may
    already have been delivered after its historical handoff.
    """
    rows = mirror.query("""MATCH (r:WHRun {run_id:$run}) USING INDEX r:WHRun(run_id)
        MATCH (r)<-[:IN_RUN]-(e:WHEvent) WHERE e.kind='cargo_handoff'
        OPTIONAL MATCH (e)-[:DONOR]->(d:WHRobot)
        OPTIONAL MATCH (e)-[:RECEIVER]->(a:WHRobot)
        OPTIONAL MATCH (e)-[:ABOUT_TASK]->(t:WHTask)
        OPTIONAL MATCH (e)-[:TRANSFERS]->(g:WHCargo)
        RETURN e.key AS event_id,e.donor_robot_id AS donor_robot_id,e.receiver_robot_id AS receiver_robot_id,
               e.task_id AS task_id,e.goods_id AS goods_id,
               collect(DISTINCT d.robot_id) AS donor_ids,collect(DISTINCT a.robot_id) AS receiver_ids,
               collect(DISTINCT t.task_id) AS task_ids,collect(DISTINCT g.cargo_id) AS goods_ids""", {"run": run_id})
    invalid = [row["event_id"] for row in rows if
               row["donor_ids"] != [row["donor_robot_id"]] or
               row["receiver_ids"] != [row["receiver_robot_id"]] or
               row["task_ids"] != [row["task_id"]] or row["goods_ids"] != [row["goods_id"]]]
    if invalid:
        raise AssertionError("handoff graph linkage mismatch: " + ", ".join(invalid[:8]))
    if expected_handoffs is not None and len(rows) != expected_handoffs:
        raise AssertionError("handoff graph/source count mismatch")
    return {"handoff_events": len(rows), "valid_linkages": True}
