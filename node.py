import json
import socket
import threading
import time
import traceback
from typing import List, Tuple

from utils import VectorClock, PositionID, Char


class Node:
    def __init__(
        self, site_id: str, host: str, port: int, peer_addrs: List[Tuple[str, int]]
    ):
        self.site_id = site_id
        self.host = host
        self.port = port
        self.peer_addrs = peer_addrs
        self.vclock = VectorClock()
        self.lock = threading.RLock()

        # Replica state: list of Char objects (including deleted ones)
        # We maintain this list in a stable deterministic order (using PositionID comparator + insertion heuristics)
        self.replica: List[Char] = []

        # seen operations to prevent reapplying
        self.seen_ops = set()

        # Networking
        self.server_sock = None
        self.peer_sockets = {}
        self.listener_thread = None
        self.stop_event = threading.Event()

        # start listening and connect to peers
        self._start_networking()

    def insert(self, caractere: str, position_index: int):
        with self.lock:
            # increment local clock
            self.vclock.increment(self.site_id)
            pid = PositionID(self.vclock.copy(), self.site_id)

            # Determine pos_id (PositionID of previous char)
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

            # Create op message
            op = {
                "type": "insert",
                "site_id": self.site_id,
                "pos_id": pos_id.serialize() if pos_id else None,
                "char": caractere,
                "op_id": pid.serialize(),
            }
            # apply locally via merge
            self.merge(op, origin_local=True)
            # broadcast
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
    
    # Networking methods
    def _start_networking(self):
        # start server
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind((self.host, self.port))
        self.server_sock.listen(5)
        self.listener_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self.listener_thread.start()

        # start connector thread to peers
        ct = threading.Thread(target=self._connect_to_peers_loop, daemon=True)
        ct.start()

    def _accept_loop(self):
        while not self.stop_event.is_set():
            try:
                client, addr = self.server_sock.accept()
                t = threading.Thread(target=self._handle_conn, args=(client, addr), daemon=True)
                t.start()
            except Exception:
                pass

    def _connect_to_peers_loop(self):
        # try to connect to peers (idempotent)
        while not self.stop_event.is_set():
            for (ph, pp) in self.peer_addrs:
                addr_str = f"{ph}:{pp}"
                if addr_str in self.peer_sockets:
                    continue
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(2.0)
                    s.connect((ph, pp))
                    s.settimeout(None)
                    self.peer_sockets[addr_str] = s
                    t = threading.Thread(target=self._handle_conn, args=(s, (ph,pp)), daemon=True)
                    t.start()
                    self._send_message(s, {"type":"sync_request", "site_id": self.site_id})
                except Exception:
                    time.sleep(0.1)
            time.sleep(1.0)

    def _handle_conn(self, conn: socket.socket, addr):
        # handle messages line-delimited JSON
        try:
            buf = b""
            while not self.stop_event.is_set():
                data = conn.recv(4096)
                if not data:
                    break
                buf += data
                # messages separated by newline
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    try:
                        msg = json.loads(line.decode())
                        self._process_incoming(msg, conn)
                    except Exception:
                        traceback.print_exc()
                        continue
        except Exception:
            pass
        finally:
            # remove from peer_sockets if present
            try:
                conn.close()
            except Exception:
                pass
            keys = [k for k,v in self.peer_sockets.items() if v==conn]
            for k in keys:
                del self.peer_sockets[k]

    def _process_incoming(self, msg: dict, conn: socket.socket):
        typ = msg.get("type")
        if typ == "sync_request":
            snapshot = [c.serialize() for c in self.replica]
            resp = {"type":"sync_response", "site_id": self.site_id, "snapshot": snapshot}
            self._send_message(conn, resp)
            return
        if typ == "sync_response":
            snapshot = msg.get("snapshot", [])
            for cobj in snapshot:
                pseudo = {
                    "type":"insert",
                    "site_id": msg.get("site_id"),
                    "pos_id": None,
                    "char": cobj["value"],
                    "op_id": cobj["id"]
                }
                if cobj.get("deleted"):
                    # apply insert first then delete
                    self.merge(pseudo)
                    del_op = {"type":"delete", "site_id": self.site_id, "target_id": cobj["id"], "op_id": {"target": cobj["id"], "deleter_site": msg.get("site_id"), "vc": {}}}
                    self.merge(del_op)
                else:
                    self.merge(pseudo)
            return
        # apply merge
        self.merge(msg)

    def _send_message(self, conn: socket.socket, msg: dict):
        try:
            payload = (json.dumps(msg, sort_keys=True) + "\n").encode()
            conn.sendall(payload)
        except Exception:
            # if send fails, remove socket
            keys = [k for k,v in self.peer_sockets.items() if v==conn]
            for k in keys:
                try:
                    del self.peer_sockets[k]
                except Exception:
                    pass

    def _broadcast(self, msg: dict):
        dead = []
        for k,s in list(self.peer_sockets.items()):
            try:
                self._send_message(s, msg)
            except Exception:
                dead.append(k)
        for k in dead:
            try:
                del self.peer_sockets[k]
            except Exception:
                pass

    def visible_text(self) -> str:
        with self.lock:
            return "".join([c.value for c in self.replica if not c.deleted])

    def show_full(self):
        with self.lock:
            for idx, c in enumerate(self.replica):
                print(f"{idx}: '{c.value}' id={c.id} deleted={c.deleted}")

    def stop(self):
        self.stop_event.set()
        try:
            self.server_sock.close()
        except Exception:
            pass
        for s in list(self.peer_sockets.values()):
            try:
                s.close()
            except Exception:
                pass