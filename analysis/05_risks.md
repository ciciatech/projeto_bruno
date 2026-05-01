# FASE 5: RISCOS — O que vai quebrar?

## Matriz de Riscos Priorizada

### 🔴 CRÍTICO (Quebra Produção Hoje)

#### R1: Dependência do FTP MTE Instável

**Descrição:** Pipeline CAGED/RAIS depende de `ftp.mtps.gov.br` sem fallback

**Probabilidade:** 30% por execução (baseado em 13 gaps históricos)  
**Impacto:** Série temporal emprego incompleta → modelo inválido  
**Localização:** `pipeline/extract/caged_rais.py:45-70`

**Cenário:**
```
2026-05-01: $ python -m pipeline.run --modulos caged_rais
            FTP timeout → 7z corrupto → pausa silenciosa
            Raw data não atualiza → Dashboard mostra dado de 2026-04
            Usuários veem série defasada (invisível sem monitoramento)
```

**Recomendação:**
- [ ] Implementar timeout adaptativo + exponential backoff
- [ ] Cache local com versão anterior como fallback
- [ ] Alertar se diff com última execução > 10%

**Custo de falha:** Tese defasada, model validation comprometida

---

#### R2: WebForm ASP.NET SIOF Pode Quebrar Dia 0

**Descrição:** Scraping ViewState é frágil a atualizações SEPLAG/CE

**Probabilidade:** 20% por ano (atualizações de sistema)  
**Impacto:** Dados execução orçamentária CE desaparecem (crítico para tese)  
**Localização:** `pipeline/extract/siof.py:120-150`

**Cenário:**
```
2026-06-15: SEPLAG/CE atualiza portal (framework upgrade)
            ViewState ID muda para "__VIEWSTATE_ENC"
            siof.py regex não encontra padrão antigo
            Executa em silêncio, retorna DataFrame vazio
            Dashboard SIOF_CE mostra "Sem dados"
            Tese fica sem dados de investimento CE (1 de 3 UFs)
```

**Recomendação:**
- [ ] Migrar para Selenium headless (mais robusto)
- [ ] Ou buscar API alternativa (TRE, CNJ, CGFB)
- [ ] Ou usar Playwright com error handling de padrão não encontrado

**Custo de falha:** 1/3 dos dados estaduais perdidos

---

#### R3: Sem Autenticação no Streamlit → Qualquer um acessa

**Descrição:** Dashboard em http://VPS:8501 público, sem login/senha

**Probabilidade:** 100% (design atual)  
**Impacto:** Dados acadêmicos expostos, sem controle de acesso  
**Localização:** Arquitetura (não há `@st.session_state` ou middleware)

**Cenário:**
```
2026-08-01: Pesquisador externo descobre URL
            Acessa painel com dados de tese não publicada
            Publica análise baseada em dados não publicados
            Tese compromete originalidade
```

**Recomendação:**
- [ ] Implementar autenticação Streamlit (ST-Auth, Okta, ou simples token)
- [ ] Ou mover para rede privada (VPN/firewall)

**Custo de falha:** Comprometimento da originalidade da tese

---

#### R4: Nenhuma Estratégia de Backup dos Raw Data

**Descrição:** Dados brutos (~600MB) existem apenas no disco da VPS

**Probabilidade:** 5% por ano (falha disco)  
**Impacto:** Perda de dados históricos, impossível refazer analysis  
**Localização:** `/dados_nordeste/raw/` (não commitado, não backupado)

**Cenário:**
```
2027-02-01: Disco VPS falha
            Raw data de 2015-2027 perdido
            Não possível regenerar (FTP MTE pode ter deletado arquivos antigos)
            Tese defasada por 2 anos
```

**Recomendação:**
- [ ] Commit de raw data crítico (BACEN, SICONFI) em git (compressionado)
- [ ] Ou backup S3 mensal
- [ ] Ou script cron de snapshot

**Custo de falha:** 2 anos de trabalho

---

### 🟡 ALTO (Afeta Análise)

#### R5: RAIS 2019-2020 Corrupto, Remuneração 100% Nula

**Descrição:** Campo `remuneracao_media` inteiro é NULL no MTE

**Probabilidade:** 100% (característica de dataset)  
**Impacto:** Proxy salarial inviável → modelo de renda do trabalho quebrado  
**Localização:** `pipeline/extract/caged_rais.py:700+` (parsing)

**Scenário:**
```
Modelo econométrico:
  Y = Emprego
  X = Crédito
  Z = Salário (de RAIS remuneracao)
  
Problema: Z = NULL → não pode validar se R$1bi → X empregos de salário Y
Análise: "Emprego cresceu, mas podemos dizer se cresceu formalmente vs informalmente?"
Resposta: NÃO (sem salário)
```

**Recomendação:**
- [ ] Buscar dados salarial alternativos (CNAE/RAIS 2.0, SBA, SCR BACEN)
- [ ] Ou documentar como limitação explícita na tese

**Custo de falha:** Parte significativa do modelo (renda) inviável

---

#### R6: 13 Gaps em CAGED Antigo (2015-2019)

**Descrição:** 13 de 60 meses missing, série temporal quebrada

**Probabilidade:** 100% (histórico)  
**Impacto:** Série 2015-2019 com lacunas → interpolação necessária  
**Localização:** `dados_nordeste/raw/caged/caged_antigo_*.csv` (gaps na coleta)

**Risco:** Interpolação pode mascarar ciclos econômicos reais

**Recomendação:**
- [ ] Documentar explicitamente quais meses estão faltando
- [ ] Usar interpolação linear ou forward-fill com cautela
- [ ] Ter segunda fonte de validação (RAIS estoque, BD Trabalho)

---

#### R7: Apenas 3 de 9 Estados Cobertos em Execução Orçamentária

**Descrição:** AL, PI, CE têm dados; PE, RN, PB, MA, BA, SE não

**Probabilidade:** 100%  
**Impacto:** Análise de investimento público não é regional (só 3 UFs)  
**Localização:** `dados_nordeste/processed/execucao_orcamentaria/`

**Recomendação:**
- [ ] Priorizar coleta de PE (maior economia, SIOF pode ter API)
- [ ] Ou usar SICONFI DCA como proxy (anual, menos granular)

---

#### R8: Sem Controle de Versão de Dados (Git Histórico)

**Descrição:** Dados processados commitados, mas sem tag de data/versão

**Probabilidade:** 80% (já ocorre)  
**Impacto:** Impossível rastrear "qual versão usou em 2026-03-15?"  
**Localização:** Git commit messages não mencionam versão dos dados

**Cenário:**
```
2027-02-01: Tese defendida
2027-03-01: Usuário externo pede "como você gerou esse número?"
Resposta: "Commit d3eb09c, mas não sei qual dado foi usado"
Problema: Reprodutibilidade comprometida
```

**Recomendação:**
- [ ] Taggear commits com `data-YYYY-MM-DD-v1`
- [ ] Criar `CHANGELOG.csv` com histórico de versões

---

### 🟠 MÉDIO (Incomoda)

#### R9: Sem Monitoramento de Qualidade em Produção

**Descrição:** Quality report gerado localmente, não monitorado em VPS

**Probabilidade:** 30% (não há alertas)  
**Impacto:** Modelo roda com dados ruins, ninguém avisa  
**Recomendação:** Adicionar check de quality antes de commit

---

#### R10: API Key Portal Transparência em .env (Segurança)

**Descrição:** `PORTAL_TRANSPARENCIA_API_KEY` em variável de env

**Probabilidade:** 10% (exposição)  
**Impacto:** Se VPS comprometida, credenciais Portal expostas  
**Recomendação:** Usar Secret Manager (AWS Secrets, Coolify Secrets)

---

## Vulnerabilidades de Segurança

| CVE | Pacote | Versão | Severidade | Remediação |
|-----|--------|--------|-----------|-----------|
| - | streamlit | 1.54.0 | 🟡 Média | Atualizar para 1.65+ |
| - | pandas | 2.3.3 | 🟢 Baixa | Compatível |
| - | requests | 2.32.5 | 🟢 Baixa | Compatível |

**Scan realizado:** Manual (sem pip-audit executado)  
⚠️ INFERIDO: Nenhuma segurança audit formal realizado

---

## Dependências Externas Críticas

| Dependência | Tipo | Status | Risco |
|-------------|------|--------|-------|
| BACEN-SGS API | REST | ✅ Ativo | 2% (governamental) |
| SICONFI/STN API | REST | ✅ Ativo | 3% (governamental) |
| MTE/PDET FTP | FTP | ⚠️ Instável | 30% (gaps históricos) |
| SAGI/MDS Bolsa | API | ⚠️ Limite | 10% (rate limit) |
| SEPLAG/CE SIOF | WebForm | 🔴 Frágil | 20% (scraping) |
| Portal Transparência | Web + API | ⚠️ Limite | 15% (sem key) |

**Pontos únicos de falha:** R1, R2 acima

---

## Impacto no Cronograma da Tese

**Timeline:**
```
2026-05-01: Defesa prevista março/2028 (22 meses)
2026-06-01: Deve estar rodando coleta + modelo
2027-06-01: Modelo finalizado (12 meses antes defesa)
2027-09-01: Artigo escrito
2028-03-01: Defesa
```

**Riscos que atrasam:**
- R1, R2, R5, R6: Afetam modelo → retrabalho (2-4 semanas)
- R8: Afeta reprodutibilidade → regressão semana de defesa

---

## Matriz de Severidade

```
        ┌─────────────────────────────────────┐
        │ Probabilidade x Impacto              │
        ├─────────────────────────────────────┤
CRÍTICO │ R1 (FTP)        R2 (SIOF)  R3 (Auth)│
        │ R4 (Backup)                         │
        ├─────────────────────────────────────┤
ALTO    │ R5 (RAIS)  R6 (Gaps)  R7 (UFs)     │
        │ R8 (Git)                            │
        ├─────────────────────────────────────┤
MÉDIO   │ R9 (Monitoring)  R10 (Key)          │
        └─────────────────────────────────────┘
```

---

## Conclusão da Fase 5

**Risco geral:** Médio-alto (3 riscos críticos, 4 altos)

**Top ações para mitigar:**
1. Implementar retry + cache para FTP (R1) — 2h
2. Mover SIOF para Selenium (R2) — 4h
3. Implementar autenticação Streamlit (R3) — 3h
4. Documentar RAIS/gaps como limitação (R5, R6) — 2h
5. Criar plano de backup raw data (R4) — 2h

**Total:** 13h (2 dias)

**Próximo:** Fase 6 — Modelo de Domínio

