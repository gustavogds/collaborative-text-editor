import time
from node import Node


def build_nodes():
    """
    Cria três nós (1, 2, 3) com a mesma configuração usada no main.py.
    Todos são criados no mesmo processo, cada um com sua própria porta.
    """
    n1 = Node("1", "127.0.0.1", 5001, [("127.0.0.1", 5002), ("127.0.0.1", 5003)])
    n2 = Node("2", "127.0.0.1", 5002, [("127.0.0.1", 5001), ("127.0.0.1", 5003)])
    n3 = Node("3", "127.0.0.1", 5003, [("127.0.0.1", 5001), ("127.0.0.1", 5002)])
    return n1, n2, n3


def test_hello_world_scenario():
    """
    Cenário de teste:
      - Site 1 escreve "Hello "
      - Site 2 escreve "World"
      - Site 3 escreve "! :D"
    Esperado: todos os nós convergem para "Hello World! :D"
    """
    n1, n2, n3 = build_nodes()

    try:
        # Dá um tempo para os sockets conectarem 
        time.sleep(1.0)

        # --- Site 1: "Hello "
        n1.insert("H", 0)
        n1.insert("e", 1)
        n1.insert("l", 2)
        n1.insert("l", 3)
        n1.insert("o", 4)
        n1.insert(" ", 5)

        # Espera propagação para os outros nós
        time.sleep(1.0)

        # --- Site 2: "World"
        # Neste ponto, todos os nós já devem ver "Hello " (6 caracteres),
        # então usamos os índices 6..10 para continuar o texto.
        n2.insert("W", 6)
        n2.insert("o", 7)
        n2.insert("r", 8)
        n2.insert("l", 9)
        n2.insert("d", 10)

        time.sleep(1.0)

        # --- Site 3: "! :D"
        n3.insert("!", 11)
        n3.insert(" ", 12)
        n3.insert(":", 13)
        n3.insert("D", 14)

        # Aguarda todas as mensagens viajarem
        time.sleep(2.0)

        # Verifica o texto visível em cada nó
        expected = "Hello World! :D"
        texts = [n1.visible_text(), n2.visible_text(), n3.visible_text()]

        print("\n===== Resultado do teste Hello World distribuído =====")
        for i, txt in enumerate(texts, start=1):
            print(f"Site {i} vê: '{txt}'")
        print("======================================================\n")

        # Checa se todos convergiram para o texto esperado
        assert all(t == expected for t in texts), (
            "Nem todos os nós convergiram para o texto esperado. "
            f"Esperado: '{expected}', Obtido: {texts}"
        )

        print("✔ Todos os nós convergiram para:", expected)

    finally:
        # encerra sockets e threads
        n1.stop()
        n2.stop()
        n3.stop()


if __name__ == "__main__":
    test_hello_world_scenario()
