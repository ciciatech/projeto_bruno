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
│   │   ├── components/   # Chrome, Panel, KPI, Sparkline, Choropleth14CE
│   │   ├── screens/      # Investimento (✓), Emprego (✓), Setores/Causal/Pipeline
│   │   ├── lib/          # tokens.css, regioes.ts, format.ts, painel.ts
│   │   └── App.tsx       # router + theme toggle
│   ├── public/data/painel.json  # gerado em build-time pelo build-data.py
│   ├── scripts/build-data.py    # CSV → JSON estático
│   ├── Dockerfile + nginx.conf  # multi-stage build → nginx:alpine
│   └── package.json
├── pages/            # [legado] dashboard Streamlit, será descontinuado (ver docs/)
├── app.py            # [legado] entrypoint Streamlit
├── dados_nordeste/
│   ├── raw/          # downloads brutos (gitignored)
│   └── processed/
│       ├── caged_municipal/, bolsa_familia/, bpc/, transferencias_municipais/
│       ├── invest_federal/, invest_municipal/, sefaz_ce/, estban/, siof/
│       └── model_ready/painel_regional_ce_mensal.csv  # output canônico (1848 linhas)
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
python -m pipeline.run --modulos-ce bolsa_familia_ce

# Testes
python -m pytest pipeline/tests/ -v
```

### Frontend

```bash
cd frontend
npm install
npm run dev             # http://localhost:5173
npm run build           # gera dist/
npm run lint            # ESLint
npm run preview         # serve dist/

# Regenerar painel.json a partir do CSV mais recente
python3 frontend/scripts/build-data.py
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

- Ano-base do residual privado: **R$ 2024/2025** (não R$ 2010 da FBCF nativa).
- CAGED municipal: **vale o custo** (~24h FTP MTE — rodou em 59min em 2026-04-30).
- SEFAZ-CE manual: **prioridade**, mesmo sendo adapter (site bloqueia bots).
- Investimento municipal: **SICONFI automático** (não a planilha manual do Bruno).

## Decisões pendentes (bloqueiam tasks)

- Fonte do "investimento total privado" para residual.
- Variáveis de controle definitivas do modelo causal (Tela 4) — especificação econométrica.

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

- `tasks.md` — roadmap de tarefas (input do `ciciatech-task-pilot`)
- `docs/plano-descontinuacao-streamlit.md` — passos para retirar a app legada
- `.claude/rules/coolify-deploy.md` — UUIDs, endpoints, SSH, Traefik
- `.claude/rules/dominio.md` — conceitos canônicos do exercício empírico
- `.claude/rules/code-style.md` — convenções Python + TS
- `archive/prisma-regional-design/README.md` — mapeamento JSX original → componente atual
