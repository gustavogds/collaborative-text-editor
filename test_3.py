# test_professor.py
import time
import threading
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


def test_concurrent_insert_same_position():
    """
    Caso pedido pelo professor:
      - Nó ID1 insere 'X' e Nó ID2 insere 'Y' na mesma posição (início do texto).
      - O CRDT deve garantir que 'X' e 'Y' apareçam na MESMA ordem em todos os nós,
        por exemplo: 'XY' em todos, ou 'YX' em todos.

    Aqui:
      - começamos com documento vazio
      - colocamos as duas inserções para rodar "ao mesmo tempo"
    """
    n1, n2, n3 = build_nodes()

    try:
        time.sleep(1.0)  # tempo para os sockets conectarem

        def insert_x_site1():
            # insere 'X' na posição 0
            n1.insert("X", 0)

        def insert_y_site2():
            # insere 'Y' na posição 0 (mesma posição lógica)
            n2.insert("Y", 0)

        t1 = threading.Thread(target=insert_x_site1)
        t2 = threading.Thread(target=insert_y_site2)

        t1.start()
        t2.start()

        t1.join()
        t2.join()

        # dá tempo para propagação
        time.sleep(1.5)

        texts = [n1.visible_text(), n2.visible_text(), n3.visible_text()]

        print("\n===== Teste: Concorrência de inserção na mesma posição =====")
        for i, txt in enumerate(texts, start=1):
            print(f"Site {i} vê: '{txt}'")
        print("=============================================================\n")

        # 1) Todos devem ter exatamente 2 caracteres
        assert all(len(t) == 2 for t in texts), (
            "Algum nó não ficou com 2 caracteres. Textos: " + repr(texts)
        )

        # 2) Todos devem ver a MESMA string
        assert texts[0] == texts[1] == texts[2], (
            "Os nós não convergiram para a mesma string. "
            f"Textos: {texts}"
        )

        # 3) A string deve ser ou 'XY' ou 'YX'
        final = texts[0]
        assert final in ("XY", "YX"), (
            "A ordem final não é 'XY' nem 'YX'. "
            f"Texto final: '{final}'"
        )

        print("✔ Concorrência de inserção: todos os nós convergiram para:", final)
        print("  Ordem entre X e Y é determinística em todos os nós.")

    finally:
        n1.stop()
        n2.stop()
        n3.stop()


def test_concurrent_insert_delete():
    """
    Caso pedido pelo professor:
      - Nó ID1 insere um caractere 'Z'.
      - Concorrentemente, Nó ID2 deleta o caractere anterior a 'Z'.

    Estratégia:
      1) Primeiro criamos um texto base 'AB' (via site 1).
      2) Depois rodamos em concorrência:
         - Site 1 insere 'Z' após 'B' (posição 2).
         - Site 2 deleta o caractere na posição 1 (que é o 'B').
      3) De acordo com a semântica do nosso RGA:
         - deletar 'B' não deleta 'Z', apenas marca 'B' como tombstone.
         - resultado esperado em todos os nós: 'AZ'.

      O importante para o enunciado é mostrar que:
        ou todos mantêm 'Z',
        ou todos o removem, mas de forma consistente.
      Aqui escolhemos explicitamente o caso em que 'Z' permanece.
    """
    n1, n2, n3 = build_nodes()

    try:
        time.sleep(1.0)

        # Passo 1: construir texto base 'AB' no site 1
        n1.insert("A", 0)
        n1.insert("B", 1)

        time.sleep(1.0)  # propagar 'AB' para todos

        # Checagem opcional: todos veem "AB"
        base_texts = [n1.visible_text(), n2.visible_text(), n3.visible_text()]
        print("\nTexto base antes da concorrência (esperado 'AB'):")
        for i, txt in enumerate(base_texts, start=1):
            print(f"Site {i} vê: '{txt}'")

        # Passo 2: concorrência inserção/remoção
        def insert_z_after_b_site1():
            # No texto 'AB', índice 2 insere após 'B'
            n1.insert("Z", 2)

        def delete_b_site2():
            # No texto 'AB', 'B' está na posição 1
            n2.delete(1)

        t1 = threading.Thread(target=insert_z_after_b_site1)
        t2 = threading.Thread(target=delete_b_site2)

        t1.start()
        t2.start()

        t1.join()
        t2.join()

        time.sleep(2.0)  # aguarda propagação

        texts = [n1.visible_text(), n2.visible_text(), n3.visible_text()]

        print("\n===== Teste: Concorrência Inserção/Remoção =====")
        for i, txt in enumerate(texts, start=1):
            print(f"Site {i} vê: '{txt}'")
        print("=================================================\n")

        # Todos devem ter a MESMA string
        assert texts[0] == texts[1] == texts[2], (
            "Os nós não convergiram para o mesmo texto após inserção/remoção. "
            f"Textos: {texts}"
        )

        final = texts[0]

        # Pela nossa semântica, esperamos que 'Z' permaneça e 'B' seja removido: "AZ"
        # (se você quiser ser mais genérica pro relatório, pode só testar consistência, sem fixar o valor exato)
        assert final == "AZ", (
            "Resultado inesperado para o cenário inserção/remoção concorrentes. "
            f"Esperado: 'AZ', obtido: '{final}'"
        )

        print("✔ Concorrência inserção/remoção: todos os nós convergiram para:", final)
        print("  O caractere 'Z' inserido permanece em todos os nós, e 'B' foi removido.")

    finally:
        n1.stop()
        n2.stop()
        n3.stop()


if __name__ == "__main__":
    print("\nRodando teste de concorrência de inserção (X/Y no início)...")
    test_concurrent_insert_same_position()

    print("\nRodando teste de concorrência inserção/remoção (Z e delete do anterior)...")
    test_concurrent_insert_delete()
