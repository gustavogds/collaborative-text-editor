import argparse

from node import Node


def parse_peer_list(s: str):
    if not s:
        return []
    parts = s.split(",")
    out = []
    for p in parts:
        if not p.strip():
            continue
        hp = p.strip().split(":")
        out.append((hp[0], int(hp[1])))
    return out


def repl(node: Node):
    print("Commands: insert <index> <char>, delete <index>, show, peers, quit")
    while True:
        try:
            line = input("> ").strip()
        except EOFError:
            break
        if not line:
            continue
        parts = line.split()
        cmd = parts[0].lower()
        if cmd == "insert":
            if len(parts) < 3:
                print("usage: insert <index> <char>")
                continue
            idx = int(parts[1])
            ch = " ".join(parts[2:])
            node.insert(ch, idx)
            print("after insert, visible:", node.visible_text())
        elif cmd == "delete":
            if len(parts) != 2:
                print("usage: delete <index>")
                continue
            idx = int(parts[1])
            node.delete(idx)
            print("after delete, visible:", node.visible_text())
        elif cmd == "show":
            print("Visible text:", node.visible_text())
            print("Full replica (including deleted):")
            node.show_full()
        elif cmd == "peers":
            print("Peer sockets:", list(node.peer_sockets.keys()))
        elif cmd == "quit":
            node.stop()
            break
        else:
            print("unknown cmd")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--site-id", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--peers", default="", help="comma separated host:port list")
    args = parser.parse_args()
    peers = parse_peer_list(args.peers)
    node = Node(args.site_id, args.host, args.port, peers)
    try:
        repl(node)
    finally:
        node.stop()
