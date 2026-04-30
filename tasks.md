# Tasks · Prisma Regional / Tese DESP-UFC

Roadmap de tarefas em aberto. Atualizado em 2026-04-30.

---

1. **Disparar coletor SICONFI invest_municipal no Mac Mini**
   `bash scripts/run_coletas.sh invest_municipal` — varre 184 municípios × 11 anos × 6 bimestres (~12k requests, ~2h). Saída: `dados_nordeste/processed/invest_municipal/invest_municipal_siconfi_ce.csv`. Quando concluir, regenerar `frontend/public/data/painel.json` e redeployar.

2. **Migrar Tela 2 (Emprego) de placeholder para dados reais CAGED**
   Usar `adm/des/sal/mov/sal_med` já presentes no `painel.json`. Mapa divergente YoY do saldo, série mensal regional sobreposta, tabela densa por região. Sinalizar cobertura atual (18/184 municípios = 10%).

3. **Forçar emissão TLS Let's Encrypt em `prisma.bruno.ciciatech.cloud`**
   Acessar do celular/4G uma vez para o Traefik disparar o ACME challenge. Hoje serve `TRAEFIK DEFAULT CERT`.

4. **Tela 3 (Setores) — decidir destino**
   Hoje placeholder cita PIB municipal IBGE + RAIS por CNAE setorial — nenhum dos dois está coletado. Investir em coleta nova ou substituir por outra análise (ex.: heatmap de composição transferências fed/est/mun por região).

5. **Tela 4 (Causal) — modelo OLS investimento → emprego**
   Scatter 14 regiões × meses, regressão simples β/α/R²/p, IC 95%. X = `siof_emp`, Y = `sal` (saldo CAGED). Variáveis de controle e especificação econométrica precisam de validação do Prof. Paulo.

6. **FilterBar funcional (período / indicador / recorte)**
   Implementar a barra de filtros do design original, com estado em URL (react-router params) que afete todas as telas.

7. **Auto-regenerar painel.json após coletas no Mac Mini**
   Hoje é manual: `scp` painel + `python3 build-data.py` + commit. Adicionar hook que comita o painel após `painel-ce` bem-sucedido (com proteção contra dados inválidos).

8. **Decisão sobre invest_privado residual** (bloqueada por Paulo)
   Definir fonte do "investimento total privado" (FBCF nacional × share PIB / JUCEC / PIA). Sem isso, a composição da Tela 1 não fecha.

9. **Plano de descontinuação `bruno-dashboard` (Streamlit)**
   Quando Telas 1+2 estiverem completas, swap DNS `bruno.ciciatech.cloud` para o frontend, parar app Streamlit no Coolify, remover `app.py` / `pages/` / `Dockerfile` raiz / `docker-compose.yml`. Manter `pipeline/` (backend ETL roda no Mac Mini).

10. **Mover `dashboard/prisma-regional/` para `archive/` ou deletar**
    JSX originais do design já foram portados para `frontend/src/` — pasta original ficou redundante.

11. **Adotar shadcn/ui no frontend**
    Hoje componentes são custom. Skill `claude_vps` recomenda shadcn/ui. `npx shadcn@latest add button table tooltip dialog` e refatorar onde fizer sentido (modais, dropdowns, tabelas grandes).

12. **Cache HTTP do `painel.json`**
    Hoje `cache-control: no-store` (835 KB todo deploy). Trocar para `painel.{hash}.json` versionado + cache longo + invalidação no build.

13. **Cleanup do repositório**
    `git status` mostra ~30 arquivos untracked (`analysis/`, `notebook/`, `status.md`, `docs/pdf/`, etc). Decidir o que entra no repo, o que vai pro `.gitignore`, o que arquiva.

14. **Criar `CLAUDE.md` inicial**
    Skill `/init` gera. Documenta stack (Python pipeline + frontend React), comandos comuns, convenções, links pra `.claude/rules/`.

15. **Testes mínimos**
    Pipeline: smoke test do coletor SICONFI (1 município/1 ano), validação de schema do painel. Frontend: snapshot test das 5 rotas. `pytest pipeline/tests/` + Vitest.

16. **ESTBAN BNB · adapter manual** (bloqueado)
    BCB removeu URL pública estável. Sem auto-download. Documentar processo de coleta manual em `dados_nordeste/raw/estban/README.md`.

17. **SEFAZ-CE cota-parte estadual · adapter manual** (bloqueado)
    Site bloqueia bots. Sem auto-download. Documentar processo manual em `dados_nordeste/raw/sefaz_ce/README.md`.

---

**Total**: 17 tasks (15 acionáveis + 2 bloqueadas por terceiros).
