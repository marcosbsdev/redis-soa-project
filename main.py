import json
import sys
from infra.redis_client import obter_conexao
from services.service_registry import ServiceRegistry
from services.cliente_service import ClienteService
from services.pagamento_service import PagamentoService
from services.estoque_service import EstoqueService
from services.eventos_service import BarramentoEventosSOA

# Garante saída UTF-8 limpa no terminal Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def exibir_painel_telemetria(client, registry: ServiceRegistry):
    print("\n" + "=" * 75)
    print(" 6. PAINEL DE GOVERNANÇA E TELEMETRIA SOA (ESTADO NO REDIS CLOUD)")
    print("=" * 75)

    servicos = registry.listar_servicos_ativos()
    print(f"\n  [Catálogo de Serviços Ativos ({len(servicos)})]:")
    for s in servicos:
        print(f"    - {s['nome']} (v{s['versao']}) | {s['endpoint']} | Status: {s['status']} | TTL: {s['ttl_restante_s']}s")

    pagamentos_ok = client.get("soa:metricas:pagamentos_processados") or "0"
    pagamentos_dup = client.get("soa:metricas:pagamentos_duplicados_bloqueados") or "0"
    print("\n  [Métricas Globais Acumuladas]:")
    print(f"    - Pagamentos Processados: {pagamentos_ok}")
    print(f"    - Cobranças Duplicadas Bloqueadas (Idempotência): {pagamentos_dup}")

    ultimos_eventos = client.lrange(BarramentoEventosSOA.LISTA_AUDITORIA, 0, 3)
    print("\n  [Últimos Eventos na Trilha de Auditoria (Redis List)]:")
    for item in ultimos_eventos:
        ev = json.loads(item)
        print(f"    - [{ev['timestamp']}] {ev['evento']} | Pedido #{ev['pedido_id']} | R$ {ev['valor']:.2f}")


def executar_demonstracao_soa():
    print("=" * 75)
    print(" ARQUITETURA ORIENTADA A SERVIÇOS (SOA) COM REDIS CLOUD - SIMULAÇÃO COMPLETA")
    print("=" * 75)

    client = obter_conexao()
    if not client:
        print("\n[FALHA] Não foi possível prosseguir sem conexão ativa com o Redis.")
        return

    # Instancia os serviços da arquitetura SOA
    registry = ServiceRegistry(client)
    cliente_service = ClienteService(client)
    pagamento_service = PagamentoService(client)
    estoque_service = EstoqueService(client)
    barramento_eventos = BarramentoEventosSOA(client)

    try:
        # ---------------------------------------------------------------------
        # ETAPA 1: Service Registry & Discovery
        # ---------------------------------------------------------------------
        print("\n" + "-" * 75)
        print(" 1. SERVICE REGISTRY & DISCOVERY (REGISTRO E DESCOBERTA DE SERVIÇOS)")
        print("-" * 75)
        registry.registrar_servico("ClienteService", "1.2.0", "soa://clientes.interno:8081", "Gestão e Cache de Clientes")
        registry.registrar_servico("PagamentoService", "2.0.1", "soa://pagamentos.interno:8082", "Processamento Idempotente de Pagamentos")
        registry.registrar_servico("EstoqueService", "1.0.4", "soa://estoque.interno:8083", "Fila de Reserva de Inventário")
        registry.registrar_servico("NotificacaoService", "1.1.0", "soa://notificacoes.interno:8084", "Envio de Alertas via Pub/Sub")

        servico_pag = registry.descobrir_servico("PagamentoService")
        print(f"  [DISCOVERY] Orquestrador localizou 'PagamentoService' dinamicamente em: {servico_pag['endpoint']}")

        # ---------------------------------------------------------------------
        # ETAPA 2: Padrão Cache-Aside e Invalidação de Cache
        # ---------------------------------------------------------------------
        print("\n" + "-" * 75)
        print(" 2. PADRÃO CACHE-ASIDE E INVALIDAÇÃO (DESEMPENHO E CONSISTÊNCIA)")
        print("-" * 75)
        # Limpa a chave antes do teste para demonstrar claramente o Cache Miss seguido de Cache Hit
        cliente_service.limpar_cache(101)

        # 1ª chamada: Cache Miss (busca no Banco de Dados simulado)
        cliente = cliente_service.buscar_cliente(101)
        # 2ª chamada: Cache Hit (retorno imediato direto do Redis)
        cliente = cliente_service.buscar_cliente(101)

        # Atualização de dados com Invalidação de Cache
        cliente_service.atualizar_perfil_cliente(101, "vip_diamond")
        # 3ª chamada: Novo Cache Miss após invalidação, trazendo dado atualizado
        cliente = cliente_service.buscar_cliente(101)

        # ---------------------------------------------------------------------
        # ETAPA 3: Idempotência e Lock Distribuído em Pagamentos
        # ---------------------------------------------------------------------
        print("\n" + "-" * 75)
        print(" 3. IDEMPOTÊNCIA E PREVENÇÃO DE DUPLICIDADE (SET NX EX)")
        print("-" * 75)
        pedido_id = 9845
        valor_pedido = 459.90
        token_unico = f"tx-pedido-{pedido_id}-2026"

        # Limpa chave de teste anterior para demonstrar o fluxo completo
        client.delete(f"{PagamentoService.IDEMPOTENCY_PREFIX}{token_unico}")

        # 1ª Tentativa: Processa normalmente
        pagamento_service.processar_pagamento(pedido_id, cliente["cliente_id"], valor_pedido, token_unico)
        # 2ª Tentativa (Simulando falha de rede / retry automático do cliente SOA): Bloqueada!
        pagamento_service.processar_pagamento(pedido_id, cliente["cliente_id"], valor_pedido, token_unico)

        # ---------------------------------------------------------------------
        # ETAPA 4: Fila Assíncrona de Trabalho (Work Queue - LPUSH / RPOP)
        # ---------------------------------------------------------------------
        print("\n" + "-" * 75)
        print(" 4. FILA DE TRABALHO ASSÍNCRONA (WORK QUEUE COM REDIS LISTS)")
        print("-" * 75)
        estoque_service.enfileirar_baixa_estoque(9845, [
            {"produto": "Teclado Mecânico SOA", "qtd": 1},
            {"produto": "Mouse Sem Fio Pro", "qtd": 2}
        ])
        estoque_service.enfileirar_baixa_estoque(9846, [
            {"produto": "Monitor Ultrawide 34", "qtd": 1}
        ])
        estoque_service.processar_fila_estoque()

        # ---------------------------------------------------------------------
        # ETAPA 5: Barramento de Eventos Pub/Sub com Consumidores em Tempo Real
        # ---------------------------------------------------------------------
        print("\n" + "-" * 75)
        print(" 5. BARRAMENTO DE EVENTOS EM TEMPO REAL (PUB/SUB MULTICAST)")
        print("-" * 75)
        barramento_eventos.iniciar_consumidores_background()

        barramento_eventos.publicar_evento(
            tipo_evento="NOVO_PEDIDO_CRIADO",
            pedido_id=pedido_id,
            cliente_nome=cliente["nome"],
            total=valor_pedido
        )
        barramento_eventos.publicar_evento(
            tipo_evento="PAGAMENTO_CONFIRMADO",
            pedido_id=pedido_id,
            cliente_nome=cliente["nome"],
            total=valor_pedido
        )

        # ---------------------------------------------------------------------
        # ETAPA 6: Painel de Governança e Telemetria
        # ---------------------------------------------------------------------
        exibir_painel_telemetria(client, registry)

    finally:
        barramento_eventos.encerrar_consumidores()

    print("\n" + "=" * 75)
    print(" SIMULAÇÃO SOA CONCLUÍDA COM SUCESSO!")
    print("=" * 75)


if __name__ == "__main__":
    executar_demonstracao_soa()
