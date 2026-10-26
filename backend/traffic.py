from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


@dataclass
class TrafficDecision:
    accepted: dict[int, bool]
    blocked_robot_ids: list[int]


class TrafficManager:
    """Deterministic continuous-space reservation for one simulation tick."""

    def __init__(self, cell_size: float = 0.38):
        self.cell_size = cell_size

    @staticmethod
    def _distance(left: dict[str, float], right: dict[str, float]) -> float:
        return math.hypot(left["x"] - right["x"], left["y"] - right["y"])

    def resolve(self, proposals: list[dict[str, Any]]) -> TrafficDecision:
        accepted = {int(proposal["robot_id"]): True for proposal in proposals}
        priority = {"high": 0, "medium": 1, "normal": 2}

        # Reject both sides of an edge swap. Choosing one side would move it into
        # the other robot's still-occupied start position.
        for index, left in enumerate(proposals):
            if not left["moving"]:
                continue
            for right in proposals[index + 1 :]:
                if not right["moving"]:
                    continue
                if (
                    self._distance(left["to"], right["from"]) < self.cell_size
                    and self._distance(right["to"], left["from"]) < self.cell_size
                ):
                    accepted[int(left["robot_id"])] = False
                    accepted[int(right["robot_id"])] = False

        # Phase one: a robot performing a stationary action owns its local safety
        # radius for this tick.
        # Idle/charging robots are considered parked in docking bays outside the
        # traversable lane even though the 2D dashboard projects them nearby.
        stationary = [
            proposal
            for proposal in proposals
            if not proposal["moving"] and proposal.get("stage") not in {"idle", "charging"}
        ]
        for proposal in proposals:
            robot_id = int(proposal["robot_id"])
            if not proposal["moving"] or not accepted[robot_id]:
                continue
            if any(
                int(other["robot_id"]) != robot_id
                and self._distance(proposal["to"], other["from"]) < self.cell_size
                for other in stationary
            ):
                accepted[robot_id] = False

        # Phase two: choose one winner for colliding movement proposals. A reject
        # is not recursively propagated through the whole convoy in the same tick;
        # every robot is reconsidered against fresh positions on the next tick.
        movers = [proposal for proposal in proposals if proposal["moving"]]
        for index, left in enumerate(movers):
            left_id = int(left["robot_id"])
            for right in movers[index + 1 :]:
                right_id = int(right["robot_id"])
                if not accepted[left_id] or not accepted[right_id]:
                    continue
                if self._distance(left["to"], right["to"]) >= self.cell_size:
                    continue
                left_clearance = self._distance(left["to"], right["from"])
                right_clearance = self._distance(left["from"], right["to"])
                if left_clearance >= self.cell_size > right_clearance:
                    accepted[right_id] = False
                elif right_clearance >= self.cell_size > left_clearance:
                    accepted[left_id] = False
                elif left_clearance < self.cell_size and right_clearance < self.cell_size:
                    accepted[left_id] = False
                    accepted[right_id] = False
                else:
                    left_rank = (priority.get(left.get("priority", "normal"), 2), left_id)
                    right_rank = (priority.get(right.get("priority", "normal"), 2), right_id)
                    loser_id = right_id if left_rank <= right_rank else left_id
                    accepted[loser_id] = False

        blocked = sorted(robot_id for robot_id, is_accepted in accepted.items() if not is_accepted)
        return TrafficDecision(accepted=accepted, blocked_robot_ids=blocked)
