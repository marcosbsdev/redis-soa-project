import json
import time
import redis


class ClienteService:
    """
    Padrão SOA: Cache-Aside (Lazy Loading) + Invalidação de Cache.
    Reduz a carga no banco de dados relacional principal e diminui a latência
    de respostas entre múltiplos serviços consumidores.
    """
    CACHE_PREFIX = "soa:cache:cliente:"

    # Simulação de Banco de Dados Relacional (ex: PostgreSQL/Oracle)
    _BANCO_DADOS_MOCK = {
        101: {
            "cliente_id": 101,
            "nome": "Maria Silva",
            "email": "maria.silva@exemplo.com",
            "perfil": "premium",
            "limite_credito": 15000.00
        },
        102: {
            "cliente_id": 102,
            "nome": "Carlos Souza",
            "email": "carlos.souza@exemplo.com",
            "perfil": "standard",
            "limite_credito": 3500.00
        }
    }

    def __init__(self, client: redis.Redis):
        self.client = client

    def buscar_cliente(self, cliente_id: int) -> dict | None:
        chave = f"{self.CACHE_PREFIX}{cliente_id}"
        inicio = time.perf_counter()

        # 1. Tenta buscar no Cache Redis primeiro
        cache_hit = self.client.get(chave)
        if cache_hit:
            tempo_ms = (time.perf_counter() - inicio) * 1000
            dados = json.loads(cache_hit)
            print(f"  [CACHE HIT]  Cliente {cliente_id} recuperado do Redis em {tempo_ms:.2f} ms -> {dados['nome']} ({dados['perfil']})")
            return dados

        # 2. Cache Miss: Simula consulta lenta ao Banco de Dados Relacional (250ms)
        print(f"  [CACHE MISS] Cliente {cliente_id} não encontrado no cache. Consultando Banco de Dados...")
        time.sleep(0.25)
        dados_db = self._BANCO_DADOS_MOCK.get(cliente_id)

        if dados_db:
            # 3. Salva no Redis com TTL de 120 segundos
            self.client.set(chave, json.dumps(dados_db), ex=120)
            tempo_ms = (time.perf_counter() - inicio) * 1000
            print(f"  [DB -> CACHE] Cliente {cliente_id} carregado do DB em {tempo_ms:.2f} ms e salvo no Redis (TTL: 120s)")
            return dados_db

        return None

    def atualizar_perfil_cliente(self, cliente_id: int, novo_perfil: str):
        """
        Atualiza o registro no banco de dados e invalida o cache no Redis
        para garantir consistência eventual entre os serviços SOA.
        """
        if cliente_id in self._BANCO_DADOS_MOCK:
            self._BANCO_DADOS_MOCK[cliente_id]["perfil"] = novo_perfil
            chave = f"{self.CACHE_PREFIX}{cliente_id}"
            self.client.delete(chave)
            print(f"  [CACHE INVALIDATION] Perfil do cliente {cliente_id} atualizado no DB para '{novo_perfil}'. Chave '{chave}' removida do cache.")

    def limpar_cache(self, cliente_id: int):
        self.client.delete(f"{self.CACHE_PREFIX}{cliente_id}")
