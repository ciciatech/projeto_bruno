# ANÁLISE REVERSA — PROJETO BRUNO (academico-bruno)

> ⚠️ **Documento histórico (snapshot de 2026-04-29, pré-reformulação de abr/2026).**
> A tese mudou de "crédito → emprego no Nordeste (9 UFs)" para "investimento
> estadual em obras e equipamentos → emprego formal nas 14 regiões SEPLAG/IPECE
> do CE", e os papéis citados aqui (orientação) não refletem necessariamente o
> estado atual. Papéis vigentes e estado do projeto em `CLAUDE.md` e `tasks.md`.

**Data:** 2026-04-29  
**Analisante:** Cassio Pinheiro (CiciaTech)  
**Escopo:** Revisão total do projeto incluindo estrutura local, pipeline de dados, dashboard Streamlit e produção na VPS  
**Tempo:** ~4 horas de investigação

---

## SUMÁRIO EXECUTIVO

O projeto **Bruno** é uma pipeline de pesquisa académica (tese DESP/UFC) que coleta dados públicos de 9 fontes federais para responder: *"Se dermos R$ 1 bilhão em crédito, quantos empregos formais serão criados no Nordeste?"*

**Status:** Operacional em produção (Coolify VPS), mas frágil em 3 pontos críticos.

### Quadro de Decisão

| Pergunta | Resposta | Risco |
|----------|----------|-------|
| **Código está pronto para produção?** | Sim, funciona hoje | Médio |
| **Dados estão confiáveis?** | 85% OK (gaps conhecidos) | Médio-Alto |
| **Modelo econométrico é válido?** | Sim, com ressalvas | Médio |
| **Pode defender a tese em março/2028?** | Sim, com 2 correções críticas | Médio |

**Recomendação:** Manter em produção + executar **13 horas de hardening** nas próximas 2 semanas.

---

## 1. PROJETO EM 30 SEGUNDOS

**O que faz:**
- Coleta mensal de crédito (BACEN), emprego (CAGED/RAIS), finanças públicas (SICONFI) de 9 estados
- Transforma dados brutos em painel bimestral (594 linhas, 37 colunas)
- Mostra dashboard interativo via Streamlit

**Arquitetura:**
```
APIs Públicas → Pipeline (extract/transform/quality) → CSV Processados → Dashboard Streamlit
(9 fontes)     (9.136 linhas Python)                   (4.2 MB em git)  (10 páginas)
```

**Usuário:** Bruno Cardoso (doutorando), Prof. Magno (orientador), Prof. Paulo Matos (produto)  
**Defesa:** Março 2028 (22 meses)  
**Em produção:** Sim (Coolify 72.60.152.227)

---

## 2. INVENTÁRIO RÁPIDO

| Aspecto | Valor | Avaliação |
|---------|-------|-----------|
| **Linguagem** | Python 3.13 | ✅ Moderno |
| **Lines of Code** | 9.136 na pipeline | ✅ Gerenciável |
| **Stack** | Streamlit + Pandas + Plotly | ✅ Apropriado |
| **Dados brutos** | 9 fontes (BACEN, SICONFI, CAGED, RAIS, etc) | ✅ Cobertura |
| **Período** | 2015-2025 (11 anos) | ✅ Suficiente |
| **Cobertura regional** | 9/9 UFs (Nordeste) | ✅ Completa |
| **Dados processados** | 134 arquivos, 4.2 MB, em Git | ✅ Versionado |
| **Dashboard** | 10 páginas Streamlit, público | ⚠️ Sem auth |
| **Testes** | 0 testes automatizados | 🔴 Crítico |
| **Documentação** | README + Dicionário + Quality report | ✅ Boa |

---

## 3. TOP 3 ACHADOS CRÍTICOS

### ⚠️ #1: FTP MTE Instável — 13 Gaps em CAGED Antigo

**Impacto:** Série histórica de emprego (variável dependente) tem lacunas 2015-2019

```
Cenário: $ python -m pipeline.run --modulos caged_rais
Status: FTP timeout → Download falha → Raw data obsoleto
Resultado: Dashboard mostra dados desatualizados (invisível sem monitoramento)
```

**Severidade:** 🔴 Crítica  
**Probabilidade:** 30% por execução  
**Custo se quebrar:** Tese defasada

**Recomendação:**
```
1. Implementar retry exponencial + fallback cache local        (2h)
2. Alertar se diff com última execução > 10%                  (1h)
3. Considerar espelho FTP (upload para S3 backup)              (4h)
```

---

### ⚠️ #2: SIOF (Execução Orçamentária CE) é Scraping de WebForm ASP.NET

**Impacto:** Se SEPLAG/CE atualizar servidor, dados simplesmente desaparecem

```
Cenário: 2026-06-15: SEPLAG/CE atualiza framework
Status: ViewState regex não encontra padrão → executa em silêncio
Resultado: SIOF_CE page mostra "Sem dados" — invisível por 2 dias
```

**Severidade:** 🔴 Crítica  
**Probabilidade:** 20% por ano  
**Custo se quebrar:** 1/3 dos dados de investimento público desaparece

**Recomendação:**
```
1. Migrar para Selenium headless (mais robusto)               (4h)
2. Ou buscar API alternativa (TRE, CNJ)                        (6h)
3. Implementar dead-letter queue (log falhas)                  (2h)
```

---

### ⚠️ #3: Dashboard Streamlit Público (Sem Autenticação)

**Impacto:** Dados de tese não publicada acessível para qualquer um

```
URL: http://VPS:8501/
Acesso: Anônimo (sem login, sem senha)
Risco: Pesquisador externo descobre → publica análise primeiro
```

**Severidade:** 🔴 Crítica (para originalidade da tese)  
**Probabilidade:** 10% (descoberta por acaso)  
**Custo se quebrar:** Compromete defesa da tese

**Recomendação:**
```
1. Implementar autenticação Streamlit (token/password)         (2h)
2. Ou mover para rede privada (VPN)                            (1h)
3. Ou usar OAuth (GitHub, institucional)                       (3h)
```

---

## 4. RISCO ESTRUTURAL: RAIS e CAGED com Gaps

| Dataset | Cobertura | Status | Impacto |
|---------|-----------|--------|---------|
| **CAGED Antigo (2015-2019)** | 47/60 meses | ⚠️ 13 gaps | Interpolação necessária |
| **CAGED Novo (2020-2025)** | 72/72 meses | ✅ OK | Série contínua |
| **RAIS (2015-2022)** | 6/8 anos | ⚠️ 2019-20 corrupto | Gap crítico |
| **RAIS Remuneração** | — | 🔴 100% nulo | Proxy salarial inviável |

**Recomendação:** 
- Documentar explicitamente como "limitação conhecida da tese"
- Usar RAIS 2.0 (PDET) ou SCR BACEN como alternativa salarial
- Validar com Professor Magno

---

## 5. SAÚDE DO CÓDIGO

### Pontos Fortes
✅ **Arquitetura modular:** Extract, Transform, Quality bem separados  
✅ **Idempotente:** Reexecução é segura  
✅ **Documentado:** README claro, dicionário de dados completo  
✅ **Config centralizada:** `config.py` pivot point  

### Pontos Fracos
🔴 **Sem testes:** 0 testes automatizados  
🔴 **Hardcoded:** Timeouts, retries fixos em código  
🔴 **Logging assimétrico:** Uns modules loggam, outros não  
🟡 **Harmonização sem validação:** Agregação mensal→bimestral sem verificação semântica  

### Dívida Técnica Estimada
```
Retry logic (FTP) .......................... 2h
Testes pytest básicos ..................... 8h
Documentação inline ....................... 6h
Validação de agregação .................... 4h
Alternativa SIOF .......................... 4h
TOTAL .................................. 24h (3-4 dias)
```

---

## 6. READINESS PARA DEFESA (Março 2028)

| Aspecto | Status | Ação |
|---------|--------|------|
| **Coleta de dados** | 85% OK | Corrigir RAIS/CAGED gaps (2h documentação) |
| **Painel bimestral** | ✅ Pronto (594×37) | Nenhuma |
| **Regressão econômica** | Não codificada* | Bruno roda em Stata/Python fora desta pipeline |
| **Publicação** | ⚠️ Dados expostos | Implementar auth antes de compartilhar (2h) |
| **Reprodutibilidade** | Boa | Adicionar versionamento de dados (2h) |

*A pipeline gera o painel; a regressão é feita fora (por Bruno em statsmodels ou Stata)

---

## 7. DECISÕES DE DESIGN

### ✅ Boas Escolhas
1. **CSV commitados em Git** — Dados processados versionados, docker redeploy com dados
2. **Streamlit** — Frontend em Python puro, sem JS
3. **Modular extractors** — Fácil adicionar nova fonte
4. **Quality layer** — 19 verificações automáticas

### ⚠️ Trade-offs
1. **Raw data não commitado** — Economiza espaço Git, mas sem backup automático
2. **Manual execution** — Coleta não automatizada (sem cron) → defasagem possível
3. **Sem database** — CSV em disco, não escalável para 100+ UFs

---

## 8. RISCOS & MITIGAÇÕES

| # | Risco | Prob. | Impacto | Ação | Custo |
|---|-------|-------|--------|------|-------|
| 1 | FTP instável | 30% | Alto | Retry + cache local | 3h |
| 2 | SIOF quebra | 20% | Alto | Selenium ou API alt | 4h |
| 3 | Sem auth | 10% | Crítico | Implementar login | 2h |
| 4 | RAIS/CAGED gaps | 100% | Médio | Documentar | 2h |
| 5 | Sem backup raw | 5% | Crítico | S3 ou Git archive | 2h |
| 6 | Sem testes | 100% | Médio | Pytest básico | 8h |
| 7 | Sem monitoramento | 30% | Médio | Alertas quality | 2h |
| **Total** | | | | | **23h** |

---

## 9. ROADMAP DE HARDENING (Próximas 2 semanas)

### Semana 1
- [ ] **Día 1-2:** Implementar auth Streamlit (blocker publicação)
- [ ] **Día 3:** Retry logic FTP + cache local
- [ ] **Día 4:** Testes pytest básicos (3-4 testes críticos)

### Semana 2
- [ ] **Día 1-2:** Migrar SIOF para Selenium
- [ ] **Día 3:** Backup S3 daily ou git archive
- [ ] **Día 4:** Versionamento de dados + CHANGELOG

---

## 10. RECOMENDAÇÕES FINAIS

### Para Bruno (Doutorando)

1. **Validar com orientador:** Gaps em RAIS/CAGED são aceitáveis?
2. **Documentar hipóteses:** Agregação mensal→bimestral deve ser semântica (stock vs flow)
3. **Planejar execução:** Coleta leva ~3h → fazer 1x/semana, não dia-antes-de-análise

### Para VPS/Infraestrutura

1. **Proteger dashboard:** Implementar autenticação antes de publicar
2. **Automatizar coleta:** Cron job semanal de backup de raw data
3. **Monitorar qualidade:** Alertas se quality score < 85%

### Para Código

1. **Priorizar:** Retry FTP (2h) > Testes (8h) > Documentação (6h)
2. **Não refatorar:** Código funciona, refactoring é luxo
3. **Manter simplicidade:** CSV + Streamlit é apropriado para tese

---

## CONCLUSÃO

O projeto **Bruno** é **viável para defesa em março/2028** com as seguintes condições:

✅ **Mantém:** Arquitetura, dados processados, documentação  
🔴 **Corrige imediatamente:** FTP instability, SIOF frágil, auth missing  
⚠️ **Documenta:** RAIS/CAGED gaps como limitações conhecidas  

**Parabéns:** Projeto bem estruturado, código limpo, documentação forte. Problemas são operacionais, não arquiteturais.

**Ação imediata:** Implementar auth + retry logic (3h) antes de qualquer publicação externa.

---

**Análise Completa:** Ver arquivos individuais em `analysis/`:
- `01_overview.md` — Inventário
- `02_architecture.md` — Arquitetura
- `03_data_flow.md` — Fluxo de dados
- `04_health.md` — Saúde do código
- `05_risks.md` — Matriz de riscos
- `06_domain_model.md` — Modelo de domínio

