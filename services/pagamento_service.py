import json
from datetime import datetime
import redis


class PagamentoService:
    """
    Padrão SOA: Idempotência e Lock Distribuído com Redis (SET NX EX).
    Em arquiteturas orientadas a serviços, falhas de rede podem causar retentativas
    (retries) da mesma requisição. A chave de idempotência garante que um pedido
    nunca seja cobrado duas vezes.
    """
    IDEMPOTENCY_PREFIX = "soa:idempotencia:pagamento:"

    def __init__(self, client: redis.Redis):
        self.client = client

    def processar_pagamento(self, pedido_id: int, cliente_id: int, valor: float, token_idempotencia: str) -> dict:
        chave_lock = f"{self.IDEMPOTENCY_PREFIX}{token_idempotencia}"

        recibo = {
            "pedido_id": pedido_id,
            "cliente_id": cliente_id,
            "valor": valor,
            "status": "APROVADO",
            "token_idempotencia": token_idempotencia,
            "processado_em": datetime.now().isoformat(timespec="seconds")
        }

        # SET com NX=True (apenas se não existir) e EX=300 (expira em 5 minutos)
        # Operação atômica no Redis!
        bloqueio_adquirido = self.client.set(
            chave_lock,
            json.dumps(recibo),
            nx=True,
            ex=300
        )

        if bloqueio_adquirido:
            # Incrementa contador global de métricas de transações no Redis
            self.client.incr("soa:metricas:pagamentos_processados")
            print(f"  [PAGAMENTO NOVO] Transação #{pedido_id} (R$ {valor:.2f}) processada com sucesso! [Token: {token_idempotencia}]")
            return {"duplicado": False, "recibo": recibo}
        else:
            # Já foi processado anteriormente: retorna o recibo salvo sem cobrar novamente
            recibo_anterior = json.loads(self.client.get(chave_lock))
            self.client.incr("soa:metricas:pagamentos_duplicados_bloqueados")
            print(f"  [IDEMPOTÊNCIA ATIVA] Retentativa detectada para o Token '{token_idempotencia}'!")
            print(f"                       Cobrança duplicada evitada. Retornando recibo original de {recibo_anterior['processado_em']}.")
            return {"duplicado": True, "recibo": recibo_anterior}
