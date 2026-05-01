# Análise Técnica Completa — Sistema RAG Híbrido de Crédito (BNB)

**Data da análise:** 2026-03-03
**Escopo:** Revisão técnica completa da arquitetura, pipeline RAG, controles de qualidade e avaliação de reaproveitamento para projeto de séries temporais econômicas.

---

## 1. Arquitetura Geral

### 1.1 Visão de Componentes

```
┌─────────────────────────────────────────────────────────────────────┐
│                      FRONTEND (Streamlit)                          │
│  ┌──────────┐  ┌───────────────┐  ┌───────────┐  ┌─────────────┐  │
│  │   Home   │  │Chat Inteligente│  │ Dashboards│  │  Exemplos   │  │
│  └──────────┘  └───────┬───────┘  └─────┬─────┘  └──────┬──────┘  │
└─────────────────────────┼───────────────┼────────────────┼─────────┘
                          │               │                │
┌─────────────────────────▼───────────────▼────────────────▼─────────┐
│                    ORQUESTRADOR (HybridRAG)                        │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────────────┐    │
│  │QuestionClass.│─►│  Roteamento  │─►│  Context Assembly      │    │
│  │   (ML/KW)    │  │ anal/sem/hyb │  │  SQL + Semântico       │    │
│  └──────────────┘  └──────────────┘  └───────────┬───────────┘    │
│                                                   │                │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────▼───────────┐    │
│  │PromptBuilder │  │  LLM Client  │◄─┤  ResponseValidator    │    │
│  │ (sys+user)   │─►│  (Factory)   │  │  (grounding check)    │    │
│  └──────────────┘  └──────────────┘  └───────────────────────┘    │
└───────────────────────────────────────────────────────────────────┘
         │                    │                      │
┌────────▼────────┐  ┌───────▼───────┐  ┌───────────▼──────────┐
│  SQL Engine     │  │ VectorSearch  │  │  LLM Providers       │
│  (views+query)  │  │  (pgvector)   │  │  ┌─────────────────┐ │
│                 │  │               │  │  │ Corporate(Azure) │ │
│  22 views SQL   │  │  cosine sim   │  │  │ Gemini (Google)  │ │
│  few-shot prmpt │  │  IVFFlat idx  │  │  │ LM Studio(local) │ │
│  sanitizer      │  │  emb cache    │  │  └─────────────────┘ │
└────────┬────────┘  └───────┬───────┘  └──────────────────────┘
         │                   │
┌────────▼───────────────────▼──────────────────────────────────┐
│              PostgreSQL 15 + pgvector                         │
│  ┌──────────────────┐  ┌─────────────────────────────────┐   │
│  │  solicitacao      │  │  22 views + 3 materialized views│   │
│  │  (37K registros)  │  │  IVFFlat index (embedding col)  │   │
│  │  embedding vector │  │  search_similar_solicitations() │   │
│  └──────────────────┘  └─────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
```

### 1.2 Fluxo de Dados End-to-End

```
[CSVs brutos]──► ETL Pipeline ──► JSON consolidado ──► load_data.py
  SINC, S522,        │                  │                    │
  FINANCIA          join.py         ciclo_de_vida_       INSERT +
                    consolidate     consolidado.json     embeddings
                                                            │
                                                            ▼
                                                    PostgreSQL + pgvector
                                                            │
                                                    ┌───────┴───────┐
                                                    ▼               ▼
                                               SQL views      vector index
                                                    │               │
                                                    └───────┬───────┘
                                                            ▼
                                              HybridRAG.answer_question()
                                                            │
                                               classify ─► route ─► retrieve
                                                            │
                                                    build_prompt ─► LLM ─► validate
                                                            │
                                                            ▼
                                                    Resposta + Confiança
```

---

## 2. Ingestão e Embedding

### 2.1 Pipeline ETL

O pipeline ETL (`src/etl/`) processa dados de 3 sistemas legados do BNB:

| Sistema | Código | Arquivo de entrada | Processador |
|---------|--------|-------------------|-------------|
| FINANCIA | S567 | `inputFinancia.csv` | `back_financia_csv.py` |
| SINC | S035 | `SINC_AGPR.csv`, `SINC_CTRT.csv`, `SINC_HCTR.csv`, `SINC_HCLI.csv` | `back_sinc.py` |
| S522 | S522 | `S522_SOLI.csv`, `S522_HSTO.csv` | `back_s522.py` |

Fluxo: CSVs → processadores individuais → `join.py` → `consolidate_json.py` → `ciclo_de_vida_consolidado.json`

O output final é um JSON com ~37K registros, cada um representando uma solicitação de crédito com campos estruturados (estado, porte, programa, valor, duração, fases do processo).

### 2.2 Geração de Embeddings

**Modelos suportados (detecção automática via `model_cache.py`):**

| Ambiente | Modelo | Dimensões | Provider |
|----------|--------|-----------|----------|
| Produção (corporativo) | `bnb-text-embedding-ada-002` | 1536 | LiteLLM (Azure Container Apps) |
| Desenvolvimento | `paraphrase-multilingual-mpnet-base-v2` | 768 | HuggingFace / SentenceTransformers |
| Alternativo | `text-embedding-004` | 768 | Google Gemini |

**Detecção automática:** Se `CORPORATE_LLM_API_BASE` está configurado, usa corporativo. Caso contrário, HuggingFace local.

**Adapter Pattern:** `CorporateEmbeddingAdapter` implementa a mesma interface do `SentenceTransformer` (`encode()`, `get_sentence_embedding_dimension()`), permitindo troca transparente sem alterar código consumidor.

### 2.3 Estratégia de Chunking

**Não há chunking de documentos textuais.** Cada registro da tabela `solicitacao` é uma unidade atômica. O texto para embedding é gerado via `generate_embedding_text()` em `load_data.py`:

```
Campos priorizados (ordem semântica):
1. Finalidade
2. Objetivo de Crédito
3. Programa de Crédito
4. Aplicação do Recurso
5. Tipo de Operação
6. Fases Resumo (apêndice sem label)
```

**Estratégia:** Labels estruturados (`"Finalidade: X. Programa: Y."`), com deduplicação de valores repetidos entre campos e normalização de whitespace. Comprimento mínimo de 10 caracteres; fallback para `"Solicitação {ID}"`.

### 2.4 Metadados Armazenados

Cada registro na tabela `solicitacao` contém:

- `solicitacao_id`, `cliente_id` — identificadores
- `valor`, `porte`, `estado`, `agencia` — dados operacionais
- `tipo_operacao`, `programa_credito`, `tipo_proposta` — classificação
- `aplicacao_recurso`, `finalidade`, `objetivo_credito` — descrição do crédito
- `duracao_dias`, `total_fases`, `tempo_total_horas` — métricas de processo
- `fases_resumo` — texto descritivo das fases
- `data_inicio`, `data_fim`, `sistemas_envolvidos` — temporal/sistema
- `embedding` — vetor `vector(768)` ou `vector(1536)` (dimensão dinâmica)

---

## 3. Vector Store

### 3.1 Banco Vetorial: PostgreSQL + pgvector

O projeto **não usa** Chroma, Qdrant, FAISS ou Pinecone. Usa **pgvector** como extensão do PostgreSQL, mantendo dados estruturados e vetoriais na mesma base.

**Vantagem arquitetural:** SQL + busca vetorial no mesmo banco, sem sincronização entre sistemas.

### 3.2 Configuração de Índice

| Parâmetro | Valor | Arquivo |
|-----------|-------|---------|
| Tipo de índice | IVFFlat | `settings.py` |
| `ivfflat_lists` | 100 | `RAG_CONFIG` |
| Métrica de similaridade | Cosseno (via `search_similar_solicitations()`) | `vector_search.py` |
| Threshold mínimo hardcoded | 0.2 (na function SQL) | `vector_search.py` |
| Threshold configurável | 0.3 (`min_similarity`) | `settings.py` |

### 3.3 Função de Busca SQL

A busca vetorial é feita via stored function `search_similar_solicitations(query_embedding, threshold, limit)` no PostgreSQL, que retorna resultados com score de similaridade já calculado.

### 3.4 Parâmetros de Busca

| Parâmetro | Valor | Descrição |
|-----------|-------|-----------|
| `search_limit` | 20 | Limite inicial de busca |
| `max_results` | 25 | Máximo após filtragem |
| `min_similarity` | 0.3 | Threshold de similaridade |
| `max_semantic_cases` | 15 | Casos incluídos no contexto |

**Cache de embeddings:** Cache in-memory (dict) com limite de 500 entries, eviction FIFO. Reduz latência em 500ms-2s para perguntas repetidas.

---

## 4. LLM Layer

### 4.1 Providers Suportados

| Provider | Modelo | Uso | Client |
|----------|--------|-----|--------|
| Corporate | `bnb-gpt-5-mini` | Produção | `CorporateLLMClient` |
| Gemini | `gemini-2.5-flash-lite` | Desenvolvimento | `GeminiClient` |
| LM Studio | `mathstral-7b-v0.1` | Backup local | `LMStudioClient` |

**Detecção automática (`LLMFactory._detect_provider()`):**
1. Variável `LLM_PROVIDER` explícita
2. Se `GEMINI_API_KEY` → Gemini
3. Se `CORPORATE_LLM_API_BASE` → Corporate
4. Default → LM Studio

**Cache de clientes:** Singleton global (`_llm_client_cache`) evita recriar cliente a cada chamada.

### 4.2 Prompt Engineering

**System Prompts** diferenciados por tipo de query (3 variantes em `prompt_builder.py`):

| Tipo | Persona | Seções obrigatórias | Temperature |
|------|---------|---------------------|-------------|
| `analytical` | "Analista de crédito sênior, 15+ anos" | Resposta Direta → Análise → Insights → Recomendações | 0.1 |
| `semantic` | "Especialista em otimização de processos" | Resposta Direta → Padrões → Gargalos → Insights → Recomendações | 0.3 |
| `hybrid` | "Consultor executivo de crédito" | Resumo Executivo → Quanti → Quali → Integrados → Estratégicas | 0.2 |

**Componentes compartilhados em todos os prompts:**
- `BNB_CONTEXT`: contexto institucional (programas, glossário, regras de formatação)
- `_CRITICAL_RULES_BASE`: proibições explícitas (não inventar, não calcular, não estimar)
- `CITATION_RULE`: obrigatoriedade de citar fontes
- `ANTI_HALLUCINATION`: lembrete final anti-alucinação

**Não usa** function calling, tool use ou chain-of-thought explícito. É uma abordagem de prompt direto com contexto injetado.

### 4.3 Geração de SQL via LLM

O `SQLEngine.generate_sql_with_llm()` usa um segundo prompt específico para gerar SQL, com:
- Schema completo das 22 views (schema info)
- 6 exemplos few-shot de perguntas → SQL
- Instruções de sanitização (LIMIT, ORDER BY, sem JOIN)
- Fallback para mapeamento por keywords se LLM falhar

---

## 5. RAG Pipeline

### 5.1 Fluxo de Recuperação e Injeção

```
Pergunta
  │
  ├─► QuestionClassifier.classify(question)
  │     ├── ML: embedding da pergunta vs embeddings de ~100 exemplos
  │     │   (top-3 avg similarity por categoria)
  │     ├── Confiança < 0.4 → fallback keyword
  │     └── Scores próximos (diff < 0.1) → hybrid
  │
  ├─► Se analytical ou hybrid:
  │     ├── generate_sql_with_llm(question) → SQL + view_name
  │     ├── _sanitize_sql() → validação de segurança
  │     ├── execute_sql_query() → resultados (com cache TTL 5min)
  │     └── format_sql_context() → texto estruturado
  │
  ├─► Se semantic ou hybrid:
  │     ├── get_cached_embedding(question) → vetor
  │     ├── search_similar_documents() → top-K documentos
  │     └── format_semantic_context() → texto estruturado
  │
  ├─► Montagem do contexto:
  │     ├── Concatenação: SQL context + Semantic context
  │     └── Truncamento: max 24.000 caracteres
  │
  ├─► build_system_prompt(query_type) → system message
  ├─► build_user_message(question, context) → user message com anti-hallucination
  │
  ├─► LLM call:
  │     ├── messages = [system, user]
  │     ├── temperature/max_tokens adaptativos
  │     └── retry com backoff exponencial (3 tentativas)
  │
  └─► ResponseValidator.validate(answer, context, question)
        ├── Extração e verificação de números
        ├── Detecção de frases incertas
        ├── Verificação de relevância semântica
        ├── Verificação de citação de fontes
        └── Score de confiança (0.0-1.0)
```

### 5.2 Quantos Chunks/Resultados São Retornados

| Componente | Quantidade | Configuração |
|------------|-----------|--------------|
| SQL results | Até 25 registros | `max_records: 25` |
| Vetorial bruto | Até 20 | `search_limit: 20` |
| Vetorial filtrado | Até 25 | `max_results: 25` |
| No contexto final | Até 15 casos | `max_semantic_cases: 15` |
| Contexto total | Max 24.000 chars | `max_context_chars: 24000` |

### 5.3 Reranking

**Não há reranking explícito.** Os resultados vetoriais são ordenados por similaridade (cosine) e filtrados pelo threshold mínimo (0.3). Os resultados SQL são ordenados pelo ORDER BY da query gerada.

### 5.4 Filtragem por Metadados

**Não há filtragem por metadados na busca vetorial.** A stored function `search_similar_solicitations()` busca em toda a tabela. A filtragem por metadados acontece apenas no lado SQL (WHERE clauses nas views).

### 5.5 Cache

| Cache | TTL | Max Size | Uso |
|-------|-----|----------|-----|
| `sql_cache` | 5 min | 50 entries | Resultados de queries SQL |
| `response_cache` | 10 min | 30 entries | Respostas completas do pipeline |
| `_embedding_cache` | Sem TTL | 500 entries | Embeddings de perguntas (FIFO) |
| `question_cache` (classifier) | Sem TTL | 100 entries | Embeddings no classificador |
| `_llm_client_cache` | Sem TTL | Por provider | Instâncias de clientes LLM |

---

## 6. Controles de Qualidade

### 6.1 Classificador de Perguntas (ML)

`QuestionClassifier` usa similaridade semântica contra ~100 exemplos pré-definidos (balanceados: ~35 por categoria). Top-3 average similarity por categoria. Fallback para keywords se confiança < 0.4. Cache de embeddings dos exemplos em disco (`classifier_embeddings.pkl`) com hash-based invalidation.

### 6.2 Sanitização de SQL

`SQLEngine._sanitize_sql()`:
- Rejeita keywords perigosas: `DROP`, `DELETE`, `UPDATE`, `INSERT`, `ALTER`, `CREATE`, `TRUNCATE`, `GRANT`, `REVOKE`
- Valida que começa com `SELECT`
- Adiciona `LIMIT` automático (50) se ausente
- Valida nomes de views/tabelas contra `KNOWN_VIEWS`
- Remove markdown code fences

### 6.3 Validação Pós-Geração (`ResponseValidator`)

| Check | Penalidade | Descrição |
|-------|-----------|-----------|
| Números não verificados | -0.08 a -0.60 | Extrai números da resposta e verifica no contexto (tolerância 1%) |
| Frases incertas | -0.10 por frase (max 3) | 14 padrões: "parece", "provavelmente", "talvez"... |
| "dado não disponível" | Nenhuma | Comportamento correto reconhecido |
| Resposta curta | -0.20 | < 50 caracteres |
| Baixa relevância | -0.10 | < 20% das entidades da pergunta presentes na resposta |
| Sem citação de fontes | -0.05 | Resposta > 200 chars sem "conforme", "vw_", etc. |
| Resposta é erro | Inválida (0.0) | Começa com "Erro" |
| **Threshold de validade** | **0.5** | `min_confidence` em `RAG_CONFIG` |

### 6.4 Anti-Alucinação

Múltiplas camadas:
1. **No system prompt:** Regras explícitas ("NÃO invente", "NÃO calcule")
2. **No user message:** Lembrete final `ANTI_HALLUCINATION`
3. **Citation rule:** Obrigatoriedade de referenciar views e IDs
4. **Pós-geração:** Verificação numérica cruzada com o contexto
5. **Cacheamento seletivo:** Apenas respostas válidas (confiança ≥ 0.5) são cacheadas

### 6.5 Fallback

- **Classificação:** ML → keywords
- **SQL gerado:** LLM → mapeamento estático por keywords (21 regras)
- **Contexto insuficiente:** Prompt instrui LLM a declarar "dado não disponível"
- **LLM indisponível:** Mensagem de erro formatada retornada ao frontend

---

## 7. API/Backend

### 7.1 Framework

**Não há API REST (FastAPI/Flask).** A interface é exclusivamente via Streamlit, que opera como aplicação monolítica server-side. O sistema é acessado via `HybridRAGSystem.ask(question)` chamado diretamente pelo Streamlit.

### 7.2 Orquestração via Pipeline CLI

`pipeline.py` é o entry point unificado:

| Comando | Descrição |
|---------|-----------|
| `python pipeline.py setup` | ETL + PostgreSQL + Embeddings + Teste |
| `python pipeline.py etl` | Pipeline ETL isolado |
| `python pipeline.py load-db` | Carga PostgreSQL + embeddings |
| `python pipeline.py test` | Teste do sistema RAG |
| `python pipeline.py cache` | Gerar cache para dashboards |
| `python pipeline.py dash` | Iniciar Streamlit |
| `python pipeline.py full` | Setup completo + cache |
| `python pipeline.py status` | Status de todos os componentes |

### 7.3 Conexão com Banco

`PostgreSQLPool` implementa connection pool customizado (não usa pgbouncer ou similares):
- Min/max size configurável (default: 2-10)
- Thread-safe (Queue + Lock)
- Health check via `SELECT 1`
- Reconexão automática em caso de conexão inválida
- Context manager disponível

---

## 8. Frontend

### 8.1 Interface: Streamlit

Aplicação Streamlit em `src/dash/streamlit_hybrid_rag.py` (~845 linhas), com 5 páginas:

| Página | Funcionalidade |
|--------|----------------|
| 🏠 Home | KPIs, visão geral, quick start |
| 💬 Chat Inteligente | Input de pergunta + resposta com tipo, confiança, contexto expandível |
| 📊 Dashboards | 4 abas (Estados, Programas, Temporal, Porte) com gráficos Plotly |
| 📚 Exemplos | Consultas pré-definidas por categoria (5 analíticas, 5 semânticas, 3 híbridas) |
| ℹ️ Sobre | Status técnico, configuração LLM, teste de conectividade |

### 8.2 Features de UX

- **Classificação visual:** Cores distintas por tipo de consulta (azul=analytical, verde=semantic, amarelo=hybrid)
- **Indicador de confiança:** Verde (≥80%), amarelo (≥60%), vermelho (<60%)
- **Contexto expandível:** Usuário pode inspecionar o contexto usado pela LLM
- **Sugestões rápidas:** 3 botões de consulta pré-definida no chat
- **Sidebar dinâmica:** Status de conexão (PostgreSQL, LLM), config do banco, provider ativo
- **Lazy loading:** Dashboards carregam dados apenas quando a aba é selecionada
- **Cache de inicialização:** `@st.cache_resource` para sistema RAG
- **Data loaders com cache:** PKL files para dashboards (evita queries repetidas)
- **Detecção automática de provider:** Sem seletor manual; detecta via env vars

---

## 9. Dependências

### 9.1 requirements.txt

| Categoria | Pacotes |
|-----------|---------|
| Core Data | `pandas>=1.5.0`, `numpy>=1.21.0`, `requests>=2.28.0`, `python-dateutil>=2.8.0` |
| ML/AI | `scikit-learn>=1.1.0`, `joblib>=1.2.0` |
| RAG/Vector | `sentence-transformers>=2.2.2` |
| PostgreSQL | `psycopg2-binary>=2.9.7`, `pgvector>=0.2.0` |
| Web/UI | `streamlit>=1.28.0` |
| Visualização | `matplotlib>=3.5.0`, `seaborn>=0.11.0`, `plotly>=5.11.0` |
| Config | `python-dotenv>=1.0.0` |
| Testes | `pytest>=7.0.0`, `pytest-cov>=4.0.0`, `pytest-mock>=3.0.0` |

### 9.2 Modelos Locais

| Modelo | Framework | Uso |
|--------|-----------|-----|
| `paraphrase-multilingual-mpnet-base-v2` | SentenceTransformers | Embeddings (dev) |
| `mathstral-7b-v0.1` | LM Studio | LLM backup local |

### 9.3 Serviços Externos

| Serviço | Uso | Configuração |
|---------|-----|--------------|
| PostgreSQL 15 + pgvector | Dados + vetores | localhost:5432 |
| LiteLLM (Azure Container Apps) | LLM corporativa + embeddings | `CORPORATE_LLM_API_BASE` |
| Google Gemini API | LLM desenvolvimento | `GEMINI_API_KEY` |
| LM Studio | LLM local | localhost:1234 |

---

## 10. Pontos Fortes e Limitações

### 10.1 Pontos Fortes

1. **Arquitetura híbrida SQL+Vetorial num único banco:** Elimina sincronização entre stores, simplifica operações, permite queries que combinam filtros estruturados com similaridade semântica.

2. **Multi-provider LLM com fallback:** Factory pattern com detecção automática (corporate → gemini → lm_studio). Produção usa modelo institucional; dev usa Gemini; backup local com LM Studio.

3. **Validação pós-geração robusta:** `ResponseValidator` com verificação cruzada de números, detecção de frases incertas, checagem de relevância e citação. Score de confiança exposto ao usuário.

4. **Anti-alucinação em múltiplas camadas:** System prompt + user prompt + lembrete final + validação numérica pós-geração + cacheamento seletivo.

5. **Classificador ML de perguntas:** Supera keyword matching com similaridade semântica, mantém fallback gracioso. Cache de embeddings em disco para inicialização rápida.

6. **Sanitização de SQL:** Previne injeção e queries destrutivas antes de executar no banco.

7. **Observabilidade:** Logging estruturado JSON, timing via decorators, métricas de cache hit/miss, latência por componente.

8. **Modularização limpa:** Separação clara em módulos (sql_engine, vector_search, prompt_builder, response_validator, llm_factory, cache, observability). Fácil de testar e manter.

9. **Adapter pattern para embeddings:** `CorporateEmbeddingAdapter` permite trocar modelo de embedding sem alterar nenhum código consumidor.

10. **Suite de testes:** 15 arquivos de teste cobrindo classificador, cache, SQL engine, formatação, observabilidade, validação, busca vetorial, prompt builder.

### 10.2 Limitações

1. **Sem API REST:** Acoplado ao Streamlit. Impede consumo por outros serviços, mobile apps, ou integração com sistemas internos via HTTP.

2. **Sem reranking:** Resultados vetoriais usam apenas cosine similarity. Um cross-encoder reranker (ex: `cross-encoder/ms-marco-MiniLM-L-6-v2`) melhoraria precisão do top-K.

3. **Sem filtragem por metadados na busca vetorial:** A stored function busca em toda a tabela. Filtrar por estado/programa/porte antes da busca vetorial reduziria ruído.

4. **Cache FIFO ingênuo para embeddings:** Usa dict com eviction do primeiro inserido. Um LRU cache (via `functools.lru_cache` ou `cachetools`) seria mais eficiente.

5. **Sem chunking sofisticado:** Adequado para registros curtos, mas não escalável para documentos longos. O texto de embedding é limitado a campos concatenados.

6. **Connection pool customizado:** Funcional mas básico. Em produção, `psycopg2.pool.ThreadedConnectionPool` ou `pgbouncer` seriam mais robustos.

7. **Sem autenticação/autorização:** O Streamlit é acessível sem login. Em ambiente corporativo, falta RBAC.

8. **Contexto truncado por chars, não por tokens:** O truncamento em 24K caracteres é aproximado. Truncamento por tokens do modelo seria mais preciso.

9. **SQL gerado via LLM sem execução em sandbox:** Embora sanitizado, o SQL é executado diretamente. Um `SET statement_timeout` ou role read-only no PostgreSQL adicionaria segurança.

10. **Sem versionamento de embeddings:** Se o modelo muda, todos os embeddings precisam ser regenerados manualmente. Não há tracking de qual modelo gerou cada embedding.

---

## 11. Avaliação de Reaproveitamento para Projeto de Séries Temporais Econômicas

### 11.1 Contexto do Novo Projeto

Dados: séries temporais econômicas em Parquet (crédito, emprego, dados fiscais do Nordeste brasileiro).

### 11.2 O Que É Reaproveitável (Sem Mudanças ou Com Mudanças Mínimas)

| Componente | Reaproveitamento | Observação |
|------------|-----------------|------------|
| `LLMFactory` + 3 clientes | ✅ Direto | Provider detection, cache, retry — tudo reutilizável |
| `prompt_builder.py` (estrutura) | ✅ Adaptar prompts | Manter padrão de system prompt + user message + anti-hallucination |
| `ResponseValidator` | ✅ Direto | Verificação numérica, citação, incerteza — genérico |
| `cache.py` (TTLCache) | ✅ Direto | Genérico, qualquer key-value |
| `observability.py` | ✅ Direto | Timing, métricas, logging estruturado — genérico |
| `retry.py` | ✅ Direto | Backoff exponencial genérico |
| `connection_pool.py` | ✅ Direto | Pool PostgreSQL genérico |
| `model_cache.py` | ✅ Direto | Cache de modelos de embedding genérico |
| `embedding_adapter.py` | ✅ Direto | Adapter pattern para APIs corporativas |
| `HybridRAG` (orquestrador) | ✅ Adaptar | Padrão classify → retrieve → format → prompt → validate |
| Streamlit (estrutura geral) | ✅ Adaptar | Layout, sidebar, chat interface |
| `pipeline.py` | ✅ Adaptar | CLI unificado — trocar ETL steps |

### 11.3 O Que Precisa Mudar Significativamente

| Componente | Mudança Necessária | Complexidade |
|------------|-------------------|--------------|
| **ETL Pipeline** | Reescrever para Parquet (pandas/polars → read_parquet) | Alta |
| **Schema SQL + Views** | Redesenhar para séries temporais (dimensões: tempo, indicador, região, fonte) | Alta |
| **`sql_engine.py`** | Novas views, novo schema info, novos few-shot examples | Alta |
| **Estratégia de Embedding** | Texto de embedding diferente: descrições de indicadores + metadados temporais em vez de "solicitação de crédito" | Média |
| **`load_data.py`** | Reescrever `generate_embedding_text()` para séries temporais; adaptar INSERT para nova tabela | Média |
| **`question_classifier.py`** | Novos exemplos por categoria (perguntas sobre tendências, comparações regionais, correlações econômicas) | Média |
| **`constants.py`** | Novas keywords analíticas/semânticas para domínio econômico | Baixa |
| **`context_formatters.py`** | Reformatar para exibir séries temporais (datas, valores, indicadores) em vez de "casos de crédito" | Média |
| **`prompt_builder.py`** (conteúdo) | Novo `BNB_CONTEXT` para dados econômicos; novos formatos de resposta | Média |
| **Dashboards** | Gráficos de séries temporais (line charts, heatmaps sazonais) em vez de bar/radar charts | Alta |

### 11.4 Decisões Arquiteturais para o Novo Projeto

**1. Manter PostgreSQL + pgvector?**
Sim. Séries temporais se beneficiam de SQL (aggregations, window functions, time series queries) e a busca vetorial pode encontrar indicadores/períodos similares.

**2. Parquet → PostgreSQL ou Parquet direto?**
Recomendo **Parquet → PostgreSQL** (manter a mesma abordagem). Parquet é ótimo para armazenamento/intercâmbio, mas o PostgreSQL permite queries SQL complexas + pgvector + views materializadas. ETL: `pd.read_parquet()` → `psycopg2 INSERT`.

**3. Embedding de quê?**
Duas abordagens possíveis:
- **Opção A:** Embedding de descrições de indicadores (ex: "Crédito total concedido na Bahia em jan/2024: R$ 1.2bi"). Útil para busca semântica tipo "como está o crédito no Nordeste?"
- **Opção B:** Embedding de séries numéricas via modelos especializados (ex: TimesFM, Chronos). Útil para encontrar séries com padrão similar.
- **Recomendação:** Começar com Opção A (reutiliza toda a infra de embedding); adicionar Opção B como segunda fase.

**4. Classificação de perguntas?**
Manter `QuestionClassifier` com novos exemplos:
- `analytical`: "Qual o PIB do Ceará em 2024?", "Compare emprego formal entre estados"
- `semantic`: "Quais fatores explicam a queda no emprego no Maranhão?", "Como a política fiscal afeta o crédito?"
- `hybrid`: "Evolução do crédito na Bahia e impactos no emprego regional"

### 11.5 Estimativa de Esforço

| Fase | Esforço Estimado |
|------|-----------------|
| Setup base (copiar infra reaproveitável) | 1 dia |
| ETL Parquet → PostgreSQL (novo schema, views) | 3-5 dias |
| Adaptar embeddings e classificador | 2-3 dias |
| Adaptar prompts e formatadores de contexto | 2 dias |
| Adaptar dashboards para séries temporais | 3-5 dias |
| Testes e validação | 2-3 dias |
| **Total estimado** | **13-19 dias** |

---

## 12. Diagrama de Dependências entre Módulos

```
settings.py ◄──────────────────────────────────────────────────┐
     ▲                                                          │
     │                                                          │
constants.py ◄─────────────────────────┐                       │
     ▲                                  │                       │
     │                                  │                       │
     ├── question_classifier.py         │                       │
     │         ▲                        │                       │
     │         │                        │                       │
     │   model_cache.py ◄──────────────┼───── embedding_adapter.py
     │         ▲                        │              ▲
     │         │                        │              │
     │   hybrid_rag.py ────────────────┤        llm_factory.py
     │     ▲   │                        │         ▲    │
     │     │   ├── sql_engine.py ◄──────┘         │    ├── corporate_llm_client.py
     │     │   ├── vector_search.py               │    ├── gemini_client.py
     │     │   ├── context_formatters.py          │    └── lm_studio_client.py
     │     │   ├── prompt_builder.py              │
     │     │   ├── response_validator.py          │
     │     │   ├── cache.py                       │
     │     │   └── observability.py               │
     │     │                                      │
     │     └── connection_pool.py                 │
     │                                            │
streamlit_hybrid_rag.py ──────────────────────────┘
```

---

*Relatório gerado automaticamente a partir da análise estática de todos os arquivos .py, .env, .txt e .md do projeto `projeto_semantico_up`.*
