import os
import redis
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

REDIS_URL = os.getenv("REDIS_URL")
HOST = os.getenv("REDIS_HOST", "localhost")
PORT = int(os.getenv("REDIS_PORT", 6379))
PASSWORD = os.getenv("REDIS_PASSWORD", "")
USER = os.getenv("REDIS_USER", "default")
DB_NAME = os.getenv("REDIS_DATABASE_NAME", "redis-soa-db")


class RedisConnectionManager:
    """
    Gerenciador centralizado de conexão com o Redis Cloud utilizando Connection Pool.
    Em uma arquitetura SOA, o pool evita o overhead de abrir e fechar conexões TCP
    a cada chamada entre serviços.
    """
    _pool = None

    @classmethod
    def get_client(cls) -> redis.Redis:
        if cls._pool is None:
            if REDIS_URL:
                cls._pool = redis.ConnectionPool.from_url(
                    REDIS_URL,
                    decode_responses=True,
                    max_connections=10
                )
            else:
                cls._pool = redis.ConnectionPool(
                    host=HOST,
                    port=PORT,
                    username=USER,
                    password=PASSWORD,
                    decode_responses=True,
                    max_connections=10
                )
        return redis.Redis(connection_pool=cls._pool)


def obter_conexao() -> redis.Redis | None:
    """
    Estabelece e valida a conexão com o Redis Cloud via PING.
    """
    try:
        client = RedisConnectionManager.get_client()
        if client.ping():
            print(f"[INFRA] Conectado com sucesso ao Redis Cloud ({DB_NAME} em {HOST}:{PORT})!")
            return client
    except redis.AuthenticationError:
        print("[ERRO] Falha de autenticação. Verifique a senha no arquivo .env.")
    except redis.ConnectionError as e:
        print(f"[ERRO] Falha na conexão com Redis -> {e}")
    except Exception as e:
        print(f"[ERRO] Inesperado: {e}")

    return None
