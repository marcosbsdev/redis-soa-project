import json
import threading
import time
from datetime import datetime
import redis
from infra.redis_client import RedisConnectionManager


class BarramentoEventosSOA:
    """
    Padrão SOA: Event-Driven SOA / Enterprise Service Bus leve via Redis Pub/Sub.
    Permite que um serviço publique eventos de domínio sem conhecer quem são os
    consumidores (Desacoplamento Temporal e Espacial).
    Inclui consumidores reais rodando em threads separadas (Notificação e Auditoria).
    """
    CANAL_PEDIDOS = "soa:eventos:pedidos"
    LISTA_AUDITORIA = "soa:auditoria:historico_eventos"

    def __init__(self, client: redis.Redis):
        self.client = client
        self._stop_event = threading.Event()
        self._ready_count = 0
        self._ready_lock = threading.Lock()
        self._threads: list[threading.Thread] = []

    def _iniciar_assinante(self, nome_servico: str, callback):
        """
        Executa um subscriber dedicado em uma thread em background.
        """
        sub_client = RedisConnectionManager.get_client()
        pubsub = sub_client.pubsub(ignore_subscribe_messages=False)
        pubsub.subscribe(self.CANAL_PEDIDOS)

        while not self._stop_event.is_set():
            msg = pubsub.get_message(timeout=0.1)
            if msg is None:
                continue

            if msg["type"] == "subscribe":
                with self._ready_lock:
                    self._ready_count += 1
                continue

            if msg["type"] == "message":
                dados = json.loads(msg["data"])
                callback(nome_servico, dados)

        pubsub.unsubscribe()
        pubsub.close()

    def _handler_notificacao(self, nome_servico: str, evento: dict):
        tipo = evento.get("evento")
        pedido_id = evento.get("pedido_id")
        cliente = evento.get("cliente_nome", "Cliente")
        print(f"    -> [{nome_servico}] E-mail/Push enviado para {cliente}: Evento '{tipo}' do Pedido #{pedido_id}")

    def _handler_auditoria(self, nome_servico: str, evento: dict):
        registro = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "evento": evento.get("evento"),
            "pedido_id": evento.get("pedido_id"),
            "valor": evento.get("total")
        }
        # Persiste o log de auditoria em uma lista limitada (LTRIM) no Redis
        self.client.lpush(self.LISTA_AUDITORIA, json.dumps(registro))
        self.client.ltrim(self.LISTA_AUDITORIA, 0, 19)
        print(f"    -> [{nome_servico}] Evento '{evento.get('evento')}' (Pedido #{evento.get('pedido_id')}) gravado na trilha de auditoria.")

    def iniciar_consumidores_background(self):
        """
        Inicia dois serviços assinantes independentes (Notificação e Auditoria)
        e aguarda até que ambos confirmem inscrição no canal Redis.
        """
        consumidores = [
            ("ServicoNotificacao", self._handler_notificacao),
            ("ServicoAuditoria", self._handler_auditoria),
        ]
        for nome, handler in consumidores:
            t = threading.Thread(
                target=self._iniciar_assinante,
                args=(nome, handler),
                daemon=True
            )
            t.start()
            self._threads.append(t)

        # Aguarda confirmação de subscribe no Redis Cloud (até 5s)
        inicio = time.time()
        while time.time() - inicio < 5.0:
            with self._ready_lock:
                if self._ready_count >= len(consumidores):
                    break
            time.sleep(0.05)

        print(f"  [PUB/SUB LISTENERS] {self._ready_count} serviços assinantes ativos escutando o canal '{self.CANAL_PEDIDOS}'")

    def publicar_evento(self, tipo_evento: str, pedido_id: int, cliente_nome: str, total: float):
        payload = {
            "evento": tipo_evento,
            "pedido_id": pedido_id,
            "cliente_nome": cliente_nome,
            "total": total,
            "timestamp": datetime.now().isoformat(timespec="seconds")
        }
        mensagem = json.dumps(payload)
        print(f"\n  [PUB/SUB PUBLISH] Publicando evento '{tipo_evento}' no canal '{self.CANAL_PEDIDOS}'...")
        inscritos = self.client.publish(self.CANAL_PEDIDOS, mensagem)
        print(f"  [PUB/SUB ACK]     Entregue a {inscritos} assinantes ativos no Redis Cloud:")
        # Pausa para permitir que as threads de background processem e imprimam em ordem
        time.sleep(0.5)

    def encerrar_consumidores(self):
        self._stop_event.set()
        for t in self._threads:
            t.join(timeout=1.5)
