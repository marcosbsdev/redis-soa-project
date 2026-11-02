# Redis SOA Project — Arquitetura Orientada a Serviços em Python

Projeto prático e completo para estudo de **SOA (Service-Oriented Architecture)** integrando múltiplos padrões arquiteturais com **Redis Cloud** em Python.

---

## Estrutura do Projeto

```text
redis-soa-project/
├── .env                         # Credenciais e configurações do Redis Cloud
├── requirements.txt             # Dependências Python (redis, python-dotenv)
├── main.py                      # Orquestrador SOA que executa todos os fluxos de ponta a ponta
├── DOCUMENTACAO_SOA.md          # Guia completo de Arquitetura SOA, Padrões e Estruturas Redis
├── README.md                    # Guia rápido do repositório
├── infra/
│   └── redis_client.py          # Connection Pool compartilhado e Health Check (PING)
└── services/
    ├── service_registry.py      # Registro e Descoberta Dinâmica de Serviços (Service Discovery)
    ├── cliente_service.py       # Padrão Cache-Aside (Hit/Miss) e Invalidação de Cache
    ├── pagamento_service.py     # Idempotência e Lock Distribuído Atômico (SET NX EX)
    ├── estoque_service.py       # Fila de Trabalho Assíncrona (Work Queue com LPUSH / RPOP)
    └── eventos_service.py       # Barramento de Eventos Pub/Sub com Consumidores em Threads Reais
```

---

## O que este projeto demonstra na prática?

1. **Service Registry & Discovery (`ServiceRegistry`)**: Serviços se registram dinamicamente no Redis com metadados de versão, endpoint, status e *heartbeat* (TTL), permitindo descoberta em tempo de execução sem acoplamento fixo.
2. **Cache-Aside & Invalidação (`ClienteService`)**: Compara a latência real entre uma consulta ao banco de dados (~250ms em *Cache Miss*) e uma leitura em memória no Redis (<20ms em *Cache Hit*), além de demonstrar a invalidação automática quando o registro é alterado.
3. **Idempotência e Lock Distribuído (`PagamentoService`)**: Utiliza o comando atômico `SET key value NX EX` para impedir que retentativas de rede (*retries*) gerem cobranças duplicadas para o mesmo pedido.
4. **Work Queue / Fila Point-to-Point (`EstoqueService`)**: Utiliza listas do Redis (`LPUSH` e `RPOP`) para desacoplar o processamento de baixa de estoque em ordem FIFO, garantindo persistência das tarefas.
5. **Barramento de Eventos Pub/Sub (`BarramentoEventosSOA`)**: Levanta dois serviços consumidores reais em background (`ServicoNotificacao` e `ServicoAuditoria`) que escutam o canal `soa:eventos:pedidos` em tempo real e reagem imediatamente aos eventos publicados.
6. **Governança e Telemetria SOA**: Ao final da execução, exibe um painel consolidado lendo diretamente do Redis os serviços ativos, contadores atômicos (`INCR`) de pagamentos e a trilha de auditoria (`LRANGE`).

---

## Como Executar

1. **Instale as dependências**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure o arquivo `.env`** com as credenciais do seu Redis Cloud:
   ```env
   REDIS_HOST=seu-host.redislabs.com
   REDIS_PORT=12675
   REDIS_USER=default
   REDIS_PASSWORD=sua_senha_aqui
   REDIS_DATABASE_NAME=database-MVZDP708
   ```

3. **Execute a simulação completa**:
   ```bash
   python main.py
   ```

---

## Documentação Completa de Estudo

Consulte o arquivo **[DOCUMENTACAO_SOA.md](./DOCUMENTACAO_SOA.md)** para o material didático aprofundado sobre:
- O que é SOA de verdade e seus 8 princípios fundamentais;
- Diferenças entre Monolito, SOA e Microsserviços;
- Explicação detalhada de cada padrão implementado e dos comandos Redis utilizados;
- Diagramas de arquitetura e sequência de chamadas.
