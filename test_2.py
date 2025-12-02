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

def test_interleaved_front_inserts():
    """
    Cenário de teste de concorrência:
      - Site 1 insere as letras "A", "B", "C", SEMPRE na posição 0.
      - Site 2 insere as letras "x", "y", "z", SEMPRE na posição 0.
      - Site 3 insere os números "1", "2", "3", SEMPRE na posição 0.
    Todas as inserções são feitas em threads concorrentes.

    Objetivo:
      - Ver que todos os nós convergem para a MESMA string.
      - A string final deve ter exatamente os 6 caracteres: A, B, C, x, y, z, 1, 2, 3
    """
    n1, n2, n3 = build_nodes()

    try:
        time.sleep(1.0)  # dar tempo para conectar

        def writer_site1():
            # Insere sempre em index 0 no site 1
            for ch in "ABC":
                n1.insert(ch, 0)
                # pequeno delay para aumentar chance de intercalar com site 2
                time.sleep(0.05)

        def writer_site2():
            # Insere sempre em index 0 no site 2
            for ch in "xyz":
                n2.insert(ch, 0)
                time.sleep(0.05)

        def writer_site3():
            # Insere sempre em index 0 no site 2
            for ch in "123":
                n3.insert(ch, 0)
                time.sleep(0.05)

        t1 = threading.Thread(target=writer_site1)
        t2 = threading.Thread(target=writer_site2)
        t3 = threading.Thread(target=writer_site3)

        t1.start()
        t2.start()
        t3.start()

        t1.join()
        t2.join()
        t3.join()

        # aguarda propagação das operações
        time.sleep(2.0)

        texts = [n1.visible_text(), n2.visible_text(), n3.visible_text()]

        print("\n===== Resultado do teste de inserções intercaladas na posição 0 =====")
        for i, txt in enumerate(texts, start=1):
            print(f"Site {i} vê: '{txt}'")
        print("=====================================================================\n")

        # Todos devem ter a MESMA string
        assert texts[0] == texts[1] == texts[2], (
            "Os nós não convergiram para o mesmo texto. "
            f"Textos obtidos: {texts}"
        )

        final_text = texts[0]
        expected_chars = set("ABCxyz123")

        # Deve ter 6 caracteres
        assert len(final_text) == 9, (
            f"O texto final não tem 9 caracteres. "
            f"Texto: '{final_text}' (len={len(final_text)})"
        )

        # O conjunto de caracteres deve ser exatamente {A,B,C,x,y,z,1,2,3}
        assert set(final_text) == expected_chars, (
            "O texto final não contém exatamente os caracteres esperados. "
            f"Esperado (como conjunto): {expected_chars}, obtido: {set(final_text)}"
        )

        print("✔ Todos os nós convergiram para a mesma string com caracteres {A,B,C,x,y,z,1,2,3}.")
        print("  Ordem resultante é a ordem determinística definida pelo CRDT (parent + PositionID).")

    finally:
        n1.stop()
        n2.stop()
        n3.stop()


if __name__ == "__main__":
    print("\nRodando teste de inserções intercaladas...")
    test_interleaved_front_inserts()
