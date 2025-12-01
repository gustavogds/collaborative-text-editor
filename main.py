from node import Node

# Configuração fixa dos nós.
NODES_CONFIG = {
    "1": {
        "host": "127.0.0.1",
        "port": 5001,
        "peers": [("127.0.0.1", 5002), ("127.0.0.1", 5003)],
    },
    "2": {
        "host": "127.0.0.1",
        "port": 5002,
        "peers": [("127.0.0.1", 5001), ("127.0.0.1", 5003)],
    },
    "3": {
        "host": "127.0.0.1",
        "port": 5003,
        "peers": [("127.0.0.1", 5001), ("127.0.0.1", 5002)],
    },
}


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
    print("=== Collaborative Text Editor (CRDT) ===")
    print("Nós disponíveis: 1, 2, 3")
    site_id = input("Digite o site ID (1, 2 ou 3): ").strip()

    if site_id not in NODES_CONFIG:
        print(f"Site ID inválido: {site_id}")
        exit(1)

    cfg = NODES_CONFIG[site_id]
    host = cfg["host"]
    port = cfg["port"]
    peers = cfg["peers"]

    print(f"Iniciando nó {site_id} em {host}:{port}")
    print(f"Peers: {peers}")

    node = Node(site_id, host, port, peers)
    try:
        repl(node)
    finally:
        node.stop()
