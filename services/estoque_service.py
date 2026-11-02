import json
import redis


class EstoqueService:
    """
    Padrão SOA: Fila de Trabalho Assíncrona (Work Queue / Point-to-Point) com Redis Lists.
    Diferente do Pub/Sub (onde todos recebem), na fila (LPUSH / RPOP) cada tarefa
    é consumida por apenas uma instância de worker e permanece armazenada no Redis
    até ser processada, garantindo entrega mesmo que o consumidor esteja offline no momento do envio.
    """
    FILA_RESERVA = "soa:fila:estoque_reservas"

    def __init__(self, client: redis.Redis):
        self.client = client

    def enfileirar_baixa_estoque(self, pedido_id: int, itens: list[dict]):
        tarefa = {
            "pedido_id": pedido_id,
            "itens": itens
        }
        # LPUSH insere no início da lista no Redis
        tamanho_fila = self.client.lpush(self.FILA_RESERVA, json.dumps(tarefa))
        print(f"  [FILA LPUSH] Pedido #{pedido_id} enviado para fila '{self.FILA_RESERVA}' (Itens na fila: {tamanho_fila})")

    def processar_fila_estoque(self) -> int:
        """
        Consome todas as tarefas pendentes na fila em ordem FIFO (usando RPOP).
        """
        processados = 0
        while True:
            # RPOP remove e retorna o elemento mais antigo da lista
            mensagem = self.client.rpop(self.FILA_RESERVA)
            if not mensagem:
                break

            tarefa = json.loads(mensagem)
            pedido_id = tarefa["pedido_id"]
            resumo_itens = ", ".join(f"{i['qtd']}x {i['produto']}" for i in tarefa["itens"])
            print(f"  [WORKER RPOP] Reserva de estoque concluída para Pedido #{pedido_id}: [{resumo_itens}]")
            processados += 1

        return processados
