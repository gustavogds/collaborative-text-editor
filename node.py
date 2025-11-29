import json
import threading
import traceback
from typing import List, Tuple

from .utils import VectorClock, PositionID, Char


class Node:
    def __init__(
        self, site_id: str, host: str, port: int, peer_addrs: List[Tuple[str, int]]
    ):
        self.site_id = site_id
        self.host = host
        self.port = port
        self.peer_addrs = peer_addrs
        self.vclock = VectorClock()
        self.lock = threading.Lock()
        self.replica: List[Char] = []
        self.seen_ops = set()

        self.server_sock = None
        self.peer_sockets = {}
        self.listener_thread = None
        self.stop_event = threading.Event()

        self._start_networking()

    def insert(self, caractere: str, position_index: int):
        with self.lock:
            self.vclock.increment(self.site_id)
            pid = PositionID(self.vclock.copy(), self.site_id)

            pos_id = None
            visible = [c for c in self.replica if not c.deleted]
            if position_index < -1:
                position_index = -1
            if position_index == -1:
                pos_id = None
            else:
                if position_index >= len(visible):
                    if visible:
                        pos_id = visible[-1].id
                    else:
                        pos_id = None
                else:
                    pos_id = visible[position_index].id

            op = {
                "type": "insert",
                "site_id": self.site_id,
                "pos_id": pos_id.serialize() if pos_id else None,
                "char": caractere,
                "op_id": pid.serialize(),
            }
            self.merge(op, origin_local=True)
            self._broadcast(op)

    def delete(self, position_index: int):
        with self.lock:
            visible = [c for c in self.replica if not c.deleted]
            if position_index < 0 or position_index >= len(visible):
                print("Invalid delete index")
                return
            target = visible[position_index]
            # increment clock (deletion is an operation)
            self.vclock.increment(self.site_id)
            # op id for delete: we can compose {target_id, deleter site, local counter}
            del_op_id = {
                "target": target.id.serialize(),
                "deleter_site": self.site_id,
                "vclock": self.vclock.serialize(),
            }
            op = {
                "type": "delete",
                "site_id": self.site_id,
                "target_id": target.id.serialize(),
                "op_id": del_op_id,
            }
            self.merge(op, origin_local=True)
            self._broadcast(op)

    def merge(self, mensagem_op: dict, origin_local: bool = False):
        try:
            with self.lock:
                # compute a canonical op key to check duplicates
                op_key = json.dumps(mensagem_op.get("op_id"), sort_keys=True)
                if op_key in self.seen_ops:
                    return
                # mark seen
                self.seen_ops.add(op_key)

                typ = mensagem_op.get("type")
                if typ == "insert":
                    # deserialize PID of new element (op_id)
                    pid = PositionID.deserialize(mensagem_op.get("op_id"))
                    char_val = mensagem_op.get("char")
                    new_char = Char(char_val, pid, deleted=False)
                    pos_id_serial = mensagem_op.get("pos_id")
                    ref_pid = (
                        PositionID.deserialize(pos_id_serial) if pos_id_serial else None
                    )

                    for c in self.replica:
                        if c.id and c.id.serialize() == pid.serialize():
                            return

                    inserted = False
                    if ref_pid:
                        # find index of element with ref_pid in replica
                        for idx, c in enumerate(self.replica):
                            if c.id and c.id.serialize() == ref_pid.serialize():
                                self.replica.insert(idx + 1, new_char)
                                inserted = True
                                break
                    if not inserted:
                        i = 0
                        while i < len(self.replica):
                            cur = self.replica[i]
                            if cur.id is None:
                                i += 1
                                continue
                            if new_char.id.before(cur.id):
                                break
                            i += 1
                        self.replica.insert(i, new_char)
                    for s, c in pid.vclock.v.items():
                        self.vclock.v[s] = max(self.vclock.v.get(s, 0), c)

                elif typ == "delete":
                    target_serial = mensagem_op.get("target_id")
                    if target_serial is None:
                        return
                    target_pid = PositionID.deserialize(target_serial)
                    # locate by id and set deleted=True
                    for c in self.replica:
                        if c.id and c.id.serialize() == target_pid.serialize():
                            c.deleted = True
                            break
                    opid = mensagem_op.get("op_id")
                    if isinstance(opid, dict) and opid.get("vclock"):
                        for s, c in opid["vclock"].items():
                            self.vclock.v[s] = max(self.vclock.v.get(s, 0), c)
                else:
                    print("Unknown op type:", typ)
        except Exception:
            traceback.print_exc()
