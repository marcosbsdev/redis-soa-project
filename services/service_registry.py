import json
from datetime import datetime
import redis


class ServiceRegistry:
    """
    Padrão SOA: Service Registry & Discovery (Registro e Descoberta de Serviços).
    Permite que serviços se registrem dinamicamente no Redis com um heartbeat (TTL),
    evitando endereços fixos (hardcoded) e facilitando a governança da arquitetura.
    """
    REGISTRY_PREFIX = "soa:registry:service:"

    def __init__(self, client: redis.Redis):
        self.client = client

    def registrar_servico(self, nome: str, versao: str, endpoint: str, descricao: str, ttl_segundos: int = 300):
        chave = f"{self.REGISTRY_PREFIX}{nome}"
        metadados = {
            "nome": nome,
            "versao": versao,
            "endpoint": endpoint,
            "descricao": descricao,
            "status": "UP",
            "ultimo_heartbeat": datetime.now().isoformat(timespec="seconds")
        }
        # Armazena como Hash ou JSON com TTL de expiração automática caso o serviço caia
        self.client.set(chave, json.dumps(metadados), ex=ttl_segundos)
        print(f"  [REGISTRY] Serviço '{nome}' (v{versao}) registrado em '{endpoint}' [TTL: {ttl_segundos}s]")

    def descobrir_servico(self, nome: str) -> dict | None:
        chave = f"{self.REGISTRY_PREFIX}{nome}"
        dados = self.client.get(chave)
        return json.loads(dados) if dados else None

    def listar_servicos_ativos(self) -> list[dict]:
        chaves = self.client.keys(f"{self.REGISTRY_PREFIX}*")
        servicos = []
        for chave in sorted(chaves):
            dados = self.client.get(chave)
            if dados:
                info = json.loads(dados)
                info["ttl_restante_s"] = self.client.ttl(chave)
                servicos.append(info)
        return servicos
