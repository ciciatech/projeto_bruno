# Projeto Bruno — Tese DESP/UFC

Pipeline de coleta + dashboard regional para a tese de Bruno Cardoso (DESP/UFC,
orientador Prof. Paulo Araújo) sobre o efeito do investimento estadual em obras
e equipamentos sobre o emprego formal nas 14 regiões SEPLAG/IPECE do Ceará.

## Stack

```
projeto-bruno/
├── pipeline/         # ETL Python 3.13 — coletas SICONFI, BACEN, STN, MTE, IBGE, IPEA
│   ├── extract/      # coletores por fonte
│   ├── transform/    # construção do painel modelo-pronto
│   ├── regioes_ce.py # mapeamento 184 municípios → 14 regiões SEPLAG
│   ├── config.py     # constantes (período, paths, UFs)
│   └── tests/        # smoke tests pytest
├── frontend/         # React 19 + Vite + TS + Tailwind v4 + react-router-dom
│   ├── src/
│   │   ├── components/   # Chrome, FilterBar, Panel, KPI, Sparkline,
│   │   │                 # Choropleth14CE, MapLegend, MapTooltip
│   │   ├── screens/      # Investimento (✓ real) · Emprego (✓ real) ·
│   │   │                 # Setores (✓ Composição de Receitas) ·
│   │   │                 # Causal (✓ OLS preliminar) · Pipeline (✓)
│   │   ├── lib/          # tokens.css, regioes.ts, format.ts, painel.ts
│   │   ├── test/         # setup Vitest
│   │   └── App.tsx       # router + theme toggle persistente em localStorage
│   ├── public/data/      # painel-index.json (no-cache) + painel.<hash>.json (1y)
│   ├── scripts/build-data.py    # CSV → JSON estático com hash content-addressed
│   ├── Dockerfile + nginx.conf  # multi-stage build (node → nginx:alpine + curl)
│   └── package.json      # +simple-statistics (OLS) +vitest +@vitest/coverage-v8
├── pages/            # [legado] dashboard Streamlit, será descontinuado (ver docs/)
├── app.py            # [legado] entrypoint Streamlit
├── dados_nordeste/
│   ├── raw/          # downloads brutos (gitignored)
│   └── processed/
│       ├── caged_municipal/, bolsa_familia/, bpc/, transferencias_municipais/
│       ├── invest_federal/, invest_municipal/, sefaz_ce_siconfi/
│       ├── populacao/, execucao_orcamentaria/ce/ (SIOF), estban/
│       └── model_ready/painel_regional_ce_mensal.csv
│              # output canônico (2016 linhas = 14 regiões × 144 meses 2015-2026)
├── scripts/          # auto-sync Mac Mini, run_coletas, qa, quality-gate
├── archive/          # design original do Prisma Regional (referência)
├── .claude/          # config, rules, agents, commands, task-pilot
├── .github/workflows/deploy-coolify.yml  # disparo de deploy via API Coolify
├── docker-compose.yml + Dockerfile       # [legado] container Streamlit
├── tasks.md          # roadmap de tarefas (entrada do ciciatech-task-pilot)
└── pyproject.toml    # config pytest + cobertura
```

## Aplicações em produção

| App | Stack | URL | Coolify UUID |
|-----|-------|-----|--------------|
| `prisma-frontend` | React/Vite/TS | https://prisma.bruno.ciciatech.cloud | `eomewrww9ecurlqvhb6vusml` |
| `bruno-dashboard` (legado) | Streamlit | https://bruno.ciciatech.cloud | `p4c0o8wkcgos8s0sscws8g8k` |

Deploy automático em push para `main` via GitHub Action `.github/workflows/deploy-coolify.yml`.
Plano de descontinuação do Streamlit em `docs/plano-descontinuacao-streamlit.md`.

## Comandos comuns

### Pipeline Python

```bash
source venv/bin/activate

# Pipeline regional CE completa (coletas + painel)
python -m pipeline.run --full-ce

# Só reconstruir painel
python -m pipeline.run --painel-ce

# Coletor específico
python -m pipeline.run --modulos-ce caged_municipal
python -m pipeline.run --modulos-ce invest_municipal     # SICONFI RREO bimestral
python -m pipeline.run --modulos-ce sefaz_ce             # SICONFI Anexo 03 (cota-parte ICMS/IPVA)
python -m pipeline.run --modulos-ce bolsa_familia_ce

# Coletor populacional IBGE (anual estática) — habilita Per capita
python -m pipeline.extract.populacao_ibge

# Testes
python -m pytest pipeline/tests/ -v       # 5 smoke + cobertura ≥10% sobre pipeline/
```

### Frontend

```bash
cd frontend
npm install
npm run dev             # http://localhost:5173
npm run build           # gera dist/
npm run lint            # ESLint
npm run test            # 19 testes Vitest
npm run test:cov        # com cobertura v8
npm run preview         # serve dist/

# Regenerar painel.json a partir do CSV mais recente
python3 frontend/scripts/build-data.py
# → gera painel.<hash>.json (1y immutable) + painel-index.json (no-cache)
```

### Quality gate

```bash
bash scripts/quality-gate.sh
```

Roda lint do frontend, build TS, pytest com cobertura sobre `pipeline/`,
e checagem de secrets staged. Usado pelo `ciciatech-task-pilot` antes de cada
commit autônomo.

### Mac Mini (coletas longas)

O Mac Mini local roda as ondas pesadas (BF/BPC/CAGED) via `scripts/run_coletas.sh`
e auto-pull a cada 5min via LaunchAgent. Conecta como `mac-mini-de-cassio.local`.

```bash
ssh mac-mini-de-cassio.local "bash dev/academico/bruno/scripts/run_coletas.sh onda1"
ssh mac-mini-de-cassio.local "tail -f ~/.local/log/bruno-pipeline-coletas.log"
```

### Deploy

```bash
# Push pra dev (auto-push via post-commit hook)
git commit -m "feat: ..."

# Push pra main → dispara deploy
git push origin dev:main

# Forçar redeploy via API Coolify
curl -sS -X POST \
  -H "Authorization: Bearer $COOLIFY_TOKEN" \
  "https://painel.ciciacademy.com.br/api/v1/deploy?uuid=eomewrww9ecurlqvhb6vusml&force=true"
```

## Convenções

- **Pipeline Python**: type hints em funções públicas, imports absolutos (`from pipeline.X`),
  reuso de `safe_request` / `save_dataframe` / `agregar_para_regiao`.
- **Frontend TS**: imports com alias `@/`, nomes em português para domínio
  (`regiao_codigo`, `salario_medio`), inglês para infra (`Dockerfile`, `nginx.conf`).
- **Schema canônico do painel**: `(regiao_codigo, regiao_nome, ano, mes, ...)`.
  Mapeamento município → região via `pipeline.regioes_ce.agregar_para_regiao`.
- **Dados sintéticos do design original (27 UFs Brasil) NÃO entram no app real** —
  só dados do painel `dados_nordeste/processed/model_ready/painel_regional_ce_mensal.csv`.
- **Rules detalhadas**: `.claude/rules/{code-style,coolify-deploy,dominio}.md`.

## Decisões aprovadas pelo Prof. Paulo (abr/2026)

Confirmado em 3 transcrições de áudio (consolidadas em
`docs/metodologia-composicao-investimento.md`):

- **Investimento total CE** = FBCF Brasil mensal × 2,2% (share PIB CE/BR), em
  R$ presente de dez/2024.
- **Ano-base do residual privado**: R$ 2024/2025 (não R$ 2010 da FBCF nativa).
  Pendência: aplicar deflator IPCA cheio.
- **CAGED municipal**: vale o custo (~24h FTP MTE — rodou em 59min em 2026-04-30).
- **SEFAZ-CE**: agora destravado via SICONFI Anexo 03 (substitui adapter manual
  bloqueado por bot). Adapter manual segue como fallback.
- **Investimento municipal**: SICONFI Anexo 01 automático (não a planilha do Bruno).
- **Investimento federal**: 3 componentes do RREO União (direto + NE×14,5% +
  nacional×2,2%) — já implementado em `pipeline/extract/invest_federal.py`.

## Decisões pendentes (bloqueiam tasks)

- **Especificação econométrica completa** do modelo causal (Tela 4):
  variáveis de controle, defasagens (lag), transformações (log/diff),
  tratamento de endogeneidade (IV ou Arellano-Bond), teste de Granger.
  Hoje a tela mostra OLS univariado preliminar com aviso editorial.
- **Previsão de FBCF para 2025/2026**: o IpeaData não publicou ainda;
  precisa modelo de extrapolação (ARIMA simples ou share fixo).

## Onde olhar quando algo quebrar

- **Coleta falhou no Mac Mini**: `~/.local/log/bruno-pipeline-{coletas,chain,autopull}.log`
- **Painel não reconstruiu**: `python -m pipeline.run --painel-ce` na máquina certa
  (precisa dos CSVs em `dados_nordeste/processed/`).
- **Frontend em branco**: F12 → Console; provavelmente `painel.json` com `NaN` ou
  outra invalidez de JSON. Re-rodar `python3 frontend/scripts/build-data.py`.
- **Deploy Coolify falhou**: `gh run list --branch main --limit 3` + UI Coolify
  em https://painel.ciciacademy.com.br.
- **Container saudável mas site fora do ar**: SSH `root@72.60.152.227 "docker ps | grep <UUID>"`.

## Referências internas

- `tasks.md` — roadmap consolidado
- `docs/changelog.md` — histórico das sprints (entradas + decisões + métricas)
- `docs/plano-descontinuacao-streamlit.md` — passos para retirar a app legada
- `docs/metodologia-composicao-investimento.md` — fórmulas das 4 esferas (citar em publicações)
- `scripts/qa-prisma-ux.md` — prompt completo para QA visual em browser tool
- `archive/prisma-regional-design/README.md` — mapa de port (design original → componentes atuais)
- `.claude/rules/coolify-deploy.md` — UUIDs, endpoints, SSH, Traefik
- `.claude/rules/dominio.md` — conceitos canônicos do exercício empírico
- `.claude/rules/code-style.md` — convenções Python + TS
- `archive/prisma-regional-design/README.md` — mapeamento JSX original → componente atual
