import json
from dataclasses import dataclass
from typing import Dict, Optional


# Gerencia o relógio vetorial para rastreamento causal
class VectorClock:
    def __init__(self, d: Optional[Dict[str, int]] = None):
        self.v = dict(d) if d else {}

    # Incrementa o contador do nó local antes de um evento
    def increment(self, site: str):
        self.v[site] = self.v.get(site, 0) + 1

    def copy(self):
        return VectorClock(self.v)

    def to_dict(self):
        return dict(self.v)

    @staticmethod
    def from_dict(d):
        return VectorClock(d or {})

    def happens_before(self, other: "VectorClock") -> bool:
        less_or_equal = True
        strictly_less = False
        all_sites = set(list(self.v.keys()) + list(other.v.keys()))
        for s in all_sites:
            a = self.v.get(s, 0)
            b = other.v.get(s, 0)
            if a > b:
                less_or_equal = False
                break
            if a < b:
                strictly_less = True
        return less_or_equal and strictly_less

    def concurrent(self, other: "VectorClock") -> bool:
        return (
            (not self.happens_before(other))
            and (not other.happens_before(self))
            and (self.v != other.v)
        )

    def __repr__(self):
        return f"VC{self.v}"

    def serialize(self):
        return self.to_dict()

    @staticmethod
    def deserialize(d):
        return VectorClock.from_dict(d)


@dataclass(order=False)
class PositionID:
    vclock: VectorClock
    site: str

    def serialize(self):
        return {"vclock": self.vclock.serialize(), "site": self.site}

    @staticmethod
    def deserialize(d):
        if d is None:
            return None
        return PositionID(VectorClock.deserialize(d["vclock"]), d["site"])

    def __repr__(self):
        return f"PID(site={self.site},vclock={self.vclock.v})"

    def before(self, other: "PositionID") -> bool:
        if self.vclock.happens_before(other.vclock):
            return True
        if other.vclock.happens_before(self.vclock):
            return False
        return self.site < other.site


@dataclass
class Char:
    value: str
    id: PositionID
    deleted: bool = False

    def serialize(self):
        return {
            "value": self.value,
            "id": self.id.serialize() if self.id else None,
            "deleted": self.deleted
        }

    @staticmethod
    def deserialize(d):
        return Char(
            d["value"],
            PositionID.deserialize(d["id"]),
            d.get("deleted", False)
        )


# helper: op id - we encode operation identity as same as PositionID for inserts; for deletes use target id
def op_id_from_position(pid: PositionID) -> str:
    return json.dumps(pid.serialize(), sort_keys=True)
