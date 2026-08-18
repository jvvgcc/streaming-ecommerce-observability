#  Streaming E-commerce Observability Pipeline

Pipeline de dados em tempo real que simula eventos de e-commerce (cliques, carrinho, compras e cancelamentos), processa esses eventos com validação de qualidade de dados e expõe tanto métricas de negócio quanto a **saúde do próprio pipeline** em um dashboard interativo.

Projeto pessoal de portfólio em engenharia de dados, construído 100% com ferramentas gratuitas e sem serviços de nuvem pagos.

##  A dor de mercado

Empresas que trabalham com dados em tempo real enfrentam um problema recorrente: **dado errado que chega silenciosamente**. Um pipeline pode continuar rodando "normalmente" enquanto processa eventos duplicados, incompletos ou fora das regras de negócio — e ninguém percebe até um relatório sair torto ou uma decisão errada ser tomada em cima dele.

Esse projeto simula esse cenário de ponta a ponta: gera eventos reais (e também "sujos" de propósito), valida cada um contra um conjunto de regras, separa o que é confiável do que não é, e alerta automaticamente quando a qualidade cai — aplicando o conceito de **data observability** em um pipeline de streaming.

##  Arquitetura

```
┌─────────────┐      ┌──────────┐      ┌─────────────┐      ┌────────────┐      ┌───────────┐
│  generator  │ ───▶ │ Redpanda │ ───▶ │  consumer   │ ───▶ │  Postgres  │ ───▶ │ Streamlit │
│ (Python)    │      │ (Kafka)  │      │  (Python +  │      │ (válidos / │      │ dashboard │
│             │      │          │      │  validação) │      │ quarentena │      │           │
└─────────────┘      └──────────┘      └──────┬──────┘      │ / métricas)│      └───────────┘
                                               │             └────────────┘
                                               ▼
                                        ┌─────────────┐
                                        │   Alertas   │
                                        │  (e-mail)   │
                                        └─────────────┘
```

**Fluxo:**
1. `generator.py` simula uma jornada de usuário (click → add_to_cart → purchase → cancellation) e publica eventos no Redpanda, injetando propositalmente ~15% de eventos "sujos"
2. `consumer.py` consome os eventos, valida cada um contra 7 tipos de regras de qualidade, e persiste separadamente eventos válidos e inválidos no Postgres, além de métricas agregadas por janela de tempo
3. Quando a taxa de erro de uma janela ultrapassa um limite configurado, um alerta é disparado automaticamente por e-mail
4. `dashboard.py` expõe tudo isso em duas visões: negócio (funil, produtos, receita) e saúde do pipeline (taxa de erro, motivos de falha, quarentena)

##  Stack

| Camada | Ferramenta |
|---|---|
| Broker de eventos | [Redpanda](https://redpanda.com/) (Kafka-compatible) |
| Orquestração local | Docker Compose |
| Processamento | Python (`confluent-kafka`) |
| Armazenamento | PostgreSQL |
| Alertas | SMTP (e-mail) |
| Visualização | Streamlit + Plotly |

Tudo roda localmente via Docker — sem custos de nuvem.

##  Validações de qualidade de dados

Cada evento é checado contra 7 cenários de erro antes de ser considerado válido:

1. **Campo obrigatório faltando**
2. **Tipo de dado incorreto** (ex: quantidade como texto)
3. **Timestamp inválido ou no futuro**
4. **Evento duplicado** (mesmo `event_id`)
5. **Valor fora do intervalo plausível** (ex: preço negativo)
6. **Violação de regra de negócio** (ex: compra sem passar por "adicionar ao carrinho")
7. **Tipo de evento desconhecido** (fora do domínio esperado)

Eventos que falham vão para uma tabela de **quarentena**, com o motivo da falha registrado — nada é descartado silenciosamente.

##  Dashboard

- **Visão de negócio**: funil de conversão, produtos mais populares, receita de compras válidas
- **Visão de saúde do pipeline**: taxa de erro ao longo do tempo, distribuição dos motivos de erro, eventos em quarentena

##  Limitações conhecidas

- O consumidor mantém estado de sessões (para validar a regra "compra após carrinho") apenas em memória. Ao reiniciar, esse estado é perdido, o que pode gerar falsos positivos de `sequencia_invalida` para sessões iniciadas antes do consumidor subir. Em um cenário de produção, esse estado seria mantido em um armazenamento externo (ex: Redis) para sobreviver a reinícios.
- O pipeline não roda 24/7 (depende da máquina local estar ligada) — o objetivo aqui é demonstrar os conceitos, não operar em produção.

##  Como rodar

```bash
# 1. Suba a infraestrutura
docker compose up -d

# 2. Crie o tópico no Redpanda
docker exec -it redpanda rpk topic create ecommerce-events

# 3. Crie as tabelas no Postgres
docker cp init.sql postgres_streaming:/init.sql
docker exec -it postgres_streaming psql -U streaming_user -d streaming_dw -f /init.sql

# 4. Instale as dependências Python
python -m venv venv
venv\Scripts\activate.bat   # Windows
pip install confluent-kafka psycopg2-binary streamlit pandas plotly sqlalchemy

# 5. Configure as variáveis de ambiente para alertas por e-mail (opcional)
set ALERT_SENDER_EMAIL=seuemail@gmail.com
set ALERT_SENDER_APP_PASSWORD=sua_senha_de_app
set ALERT_RECIPIENT_EMAIL=seuemail@gmail.com

# 6. Rode cada componente em um terminal separado
python generator.py
python consumer.py
streamlit run dashboard.py
```

##  Próximos passos

- Adicionar cooldown nos alertas por e-mail para evitar excesso de notificações
- Persistir o estado de sessões em Redis para sobreviver a reinícios do consumidor
- Empacotar os componentes Python em containers Docker próprios

---

Projeto desenvolvido para fins de estudo e portfólio em engenharia de dados.
