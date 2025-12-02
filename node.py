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

        # lista de objetos Char (incluindo deletados)
        self.replica: List[Char] = []

        # operações já vistas (para evitar duplicações)
        self.seen_op = set()

        # inserts pendentes cujo parent ainda não chegou
        self.pending_inserts = {}

        # Networking
        self.server_sock = None
        self.peer_sockets = {}
        self.listener_thread = None
        self.stop_event = threading.Event()

        # começa o networking
        self._start_networking()

    # Insert baseado na posição visível
    def insert(self, caractere: str, position_index: int):
        with self.lock:
            # incrementa relógio local
            self.vclock.increment(self.site_id)
            pid = PositionID(self.vclock.copy(), self.site_id)

            # Determina pos_id (PositionID do caractere anterior, em termos de texto visível)
            visible = [c for c in self.replica if not c.deleted]

            if position_index < 0:
                position_index = 0
            if position_index > len(visible):
                position_index = len(visible)

            if not visible:
                # texto vazio: "antes de tudo" -> pos_id = None
                pos_id = None
            elif position_index == 0:
                # inserir antes do primeiro -> pos_id = None (head)
                pos_id = None
            else:
                # inserir na posição i -> depois do caractere i-1
                pos_id = visible[position_index - 1].id

            op = {
                "type": "insert",
                "site_id": self.site_id,
                "pos_id": pos_id.serialize() if pos_id else None,
                "char": caractere,
                "op_id": pid.serialize(),
            }

            # aplica localmente via merge
            self.merge(op, origin_local=True)
            # broadcast
            self._broadcast(op)

    # Delete baseado na posição visível
    def delete(self, position_index: int):
        with self.lock:
            visible = [c for c in self.replica if not c.deleted]
            if position_index < 0 or position_index >= len(visible):
                print("Invalid delete index")
                return
            target = visible[position_index]

            # increment clock (deletion is an operation)
            self.vclock.increment(self.site_id)

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

    # Aplica uma operação (local ou remota) na réplica
    def merge(self, mensagem_op: dict, origin_local: bool = False):
        try:
            with self.lock:
                typ = mensagem_op.get("type")

                # Para inserts, só marcamos seen_op quando realmente inserirmos o Char.
                # Para deletes, podemos marcar direto, pois não há dependência.
                if typ != "insert":
                    op_key = json.dumps(mensagem_op.get("op_id"), sort_keys=True)
                    if op_key in self.seen_op:
                        return
                    self.seen_op.add(op_key)

                if typ == "insert":
                    self._merge_insert(mensagem_op)
                elif typ == "delete":
                    self._merge_delete(mensagem_op)
                else:
                    print("Unknown op type:", typ)
        except Exception:
            traceback.print_exc()

    # Aplica um insert estilo RGA simplificado, seguindo uma lógica de predecessor (parent), com buffer de pendentes
    def _merge_insert(self, op: dict):
        pid = PositionID.deserialize(op.get("op_id"))
        char_val = op.get("char")
        parent_serial = op.get("pos_id")
        parent_id = PositionID.deserialize(parent_serial) if parent_serial else None

        # evita duplicata se o Char já existe na réplica
        for c in self.replica:
            if c.id and c.id.serialize() == pid.serialize():
                return

        # Se há parent e ele ainda não existe, guarda em pendentes
        if parent_id is not None and not self._has_char_with_id(parent_id):
            parent_key = json.dumps(parent_id.serialize(), sort_keys=True)
            self.pending_inserts.setdefault(parent_key, []).append(op)
            return
        
        # Marca operação como vista
        op_key = json.dumps(op.get("op_id"), sort_keys=True)
        if op_key in self.seen_op:
            return
        self.seen_op.add(op_key)

        new_char = Char(char_val, pid, parent_id, deleted=False)

        # RGA simplificado: insere após o parent, ordenando irmãos por ID
        parent_idx = -1
        if parent_id is not None:
            for i, c in enumerate(self.replica):
                if c.id and c.id.serialize() == parent_id.serialize():
                    parent_idx = i
                    break

        insert_idx = parent_idx + 1

        # Avança enquanto o elemento atual tiver o mesmo parent
        # e o ID dele for "menor" que o new_char (para ordenar irmãos)
        while insert_idx < len(self.replica):
            cur = self.replica[insert_idx]

            same_parent = (
                (parent_id is None and cur.parent is None)
                or (
                    parent_id is not None
                    and cur.parent is not None
                    and cur.parent.serialize() == parent_id.serialize()
                )
            )

            if not same_parent:
                # encontramos um caractere com outro parent → fim do bloco de irmãos
                break

            # se o new_char deve vir antes de cur, paramos aqui
            # só compara IDs se estiver dentro da sublista de IRMÃOS
            if same_parent:
                if new_char.id.before(cur.id):
                    break
            else:
                # encontrou um elemento que NÃO é da mesma família
                break


            insert_idx += 1

        self.replica.insert(insert_idx, new_char)

        # atualiza relogio local com o max do vclock do pid
        for s, c in pid.vclock.v.items():
            self.vclock.v[s] = max(self.vclock.v.get(s, 0), c)

        # processa filhos pendentes deste novo char
        self._apply_pending_children(pid)

        # exporta o texto atual para o arquivo
        self.export_to_file()
  
    def _has_char_with_id(self, pid: PositionID) -> bool:
        for c in self.replica:
            if c.id and c.id.serialize() == pid.serialize():
                return True
        return False

    # quando um parent é inserido, aplicamos todos os inserts pendentes que apontavam para ele
    def _apply_pending_children(self, parent_pid: PositionID):
        
        parent_key = json.dumps(parent_pid.serialize(), sort_keys=True)
        children = self.pending_inserts.pop(parent_key, [])
        for child_op in children:
            # agora que garantimos que o parent existe; chamamos _merge_insert de novo
            self._merge_insert(child_op)

    def _merge_delete(self, op: dict):
        target_serial = op.get("target_id")
        if target_serial is None:
            return

        target_pid = PositionID.deserialize(target_serial)

        # localiza por id e seta deleted = True
        for c in self.replica:
            if c.id and c.id.serialize() == target_pid.serialize():
                c.deleted = True
                break

        opid = op.get("op_id")
        if isinstance(opid, dict) and opid.get("vclock"):
            for s, c in opid["vclock"].items():
                self.vclock.v[s] = max(self.vclock.v.get(s, 0), c)

    def _start_networking(self):
        # inicia servidor para aceitar conexões de peers
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind((self.host, self.port))
        self.server_sock.listen(5)
        self.listener_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self.listener_thread.start()

        # inicia thread de conexão aos peers
        ct = threading.Thread(target=self._connect_to_peers_loop, daemon=True)
        ct.start()

    # loop que aceita conexões de entrada
    def _accept_loop(self):
        while not self.stop_event.is_set():
            try:
                client, addr = self.server_sock.accept()
                t = threading.Thread(target=self._handle_conn, args=(client, addr), daemon=True)
                t.start()
            except Exception:
                pass

    # tenta conectar aos peers periodicamente
    def _connect_to_peers_loop(self):
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
                    t = threading.Thread(target=self._handle_conn, args=(s, (ph, pp)), daemon=True)
                    t.start()
                    self._send_message(s, {"type": "sync_request", "site_id": self.site_id})
                except Exception:
                    time.sleep(0.1)
            time.sleep(1.0)

    # lida com as mensagens recebidas
    def _handle_conn(self, conn: socket.socket, addr):
        try:
            buf = b""
            while not self.stop_event.is_set():
                data = conn.recv(4096)
                if not data:
                    break
                buf += data
                # mensagens separadas por \n
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
            # remove de peer_sockets se desconectar
            try:
                conn.close()
            except Exception:
                pass
            keys = [k for k, v in self.peer_sockets.items() if v == conn]
            for k in keys:
                del self.peer_sockets[k]

    def _process_incoming(self, msg: dict, conn: socket.socket):
        typ = msg.get("type")
        if typ == "sync_request":
            snapshot = [c.serialize() for c in self.replica]
            resp = {"type": "sync_response", "site_id": self.site_id, "snapshot": snapshot}
            self._send_message(conn, resp)
            return

        if typ == "sync_response":
            snapshot = msg.get("snapshot", [])
            for cobj in snapshot:
                pseudo_insert = {
                    "type": "insert",
                    "site_id": msg.get("site_id"),
                    "pos_id": cobj.get("parent"),
                    "char": cobj["value"],
                    "op_id": cobj["id"],
                }
                if cobj.get("deleted"):
                    # aplica insert primeiro, depois delete
                    self.merge(pseudo_insert)
                    del_op = {
                        "type": "delete",
                        "site_id": self.site_id,
                        "target_id": cobj["id"],
                        "op_id": {
                            "target": cobj["id"],
                            "deleter_site": msg.get("site_id"),
                            "vclock": {},
                        },
                    }
                    self.merge(del_op)
                else:
                    self.merge(pseudo_insert)
            return

        # aplica merge genérico (insert/delete)
        self.merge(msg)

    def _send_message(self, conn: socket.socket, msg: dict):
        try:
            payload = (json.dumps(msg, sort_keys=True) + "\n").encode()
            conn.sendall(payload)
        except Exception:
            # se o envio falhar, remove o socket
            keys = [k for k, v in self.peer_sockets.items() if v == conn]
            for k in keys:
                try:
                    del self.peer_sockets[k]
                except Exception:
                    pass

    def _broadcast(self, msg: dict):
        dead = []
        for k, s in list(self.peer_sockets.items()):
            try:
                self._send_message(s, msg)
            except Exception:
                dead.append(k)
        for k in dead:
            try:
                del self.peer_sockets[k]
            except Exception:
                pass

    # Visualização e utils
    def visible_text(self) -> str:
        with self.lock:
            return "".join([c.value for c in self.replica if not c.deleted])

    def show_full(self):
        with self.lock:
            for idx, c in enumerate(self.replica):
                print(f"{idx}: '{c.value}' id={c.id} parent={c.parent} deleted={c.deleted}")

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

    # Exporta o texto visível para um arquivo local, ex: site_1.txt
    def export_to_file(self):
        filename = f"site_{self.site_id}.txt"
        text = self.visible_text()
        try:
            with open(filename, "w", encoding="utf-8") as f:
                f.write(text)
        except Exception as e:
            print(f"[ERRO ao salvar arquivo]: {e}")