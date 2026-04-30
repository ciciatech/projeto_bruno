# Tasks · Prisma Regional / Tese DESP-UFC

> Roadmap pra `ciciatech-task-pilot`. Formato `- [ ]` (pendente), `- [x]` (done), `- [!]` (bloqueada).
> Tasks marcadas `- [!]` exigem terceiros (Prof. Paulo, infra externa) ou ações no Mac Mini que o pilot remoto não tem acesso.

## Backlog

- [!] T01: Disparar coletor SICONFI invest_municipal no Mac Mini (~2h, 12k requests via SSH; manual)
- [x] T02: Migrar Tela 2 (Emprego) de placeholder para dados reais CAGED usando campos adm/des/sal/mov/sal_med do painel.json
- [!] T03: Forçar emissão TLS Let's Encrypt em prisma.bruno.ciciatech.cloud (depende de request via DNS público real)
- [!] T04: Tela 3 Setores bloqueada — PIB municipal IBGE e RAIS por CNAE não estão coletados, aguarda decisão de escopo
- [!] T05: Tela 4 Causal modelo OLS investimento → emprego bloqueada, aguarda especificação econométrica do Prof. Paulo
- [ ] T06: FilterBar funcional (período / indicador / recorte) com estado em URL via react-router — depende de T02
- [!] T07: Auto-regenerar painel.json após coletas no Mac Mini — exige hook local no Mac Mini
- [!] T08: Decisão sobre invest_privado residual bloqueada por Paulo (fonte do total privado)
- [x] T09: Documentar plano de descontinuação bruno-dashboard Streamlit em docs/plano-descontinuacao-streamlit.md
- [x] T10: Mover dashboard/prisma-regional/ para archive/ (assets do design já portados em frontend/src/)
- [ ] T11: Adotar shadcn/ui no frontend (button table tooltip dialog) e refatorar onde fizer sentido
- [x] T12: Cache HTTP do painel.json — versionar como painel.hash.json com cache longo e invalidação no build
- [!] T13: Cleanup do repositório (~30 arquivos untracked) — exige decisões manuais sobre cada arquivo
- [x] T14: Criar CLAUDE.md inicial documentando stack, comandos e convenções do projeto
- [ ] T15: Testes mínimos pytest pipeline/tests/ (smoke SICONFI, schema painel) e Vitest no frontend (snapshot rotas)
- [!] T16: ESTBAN BNB adapter manual bloqueado, BCB removeu URL pública estável
- [!] T17: SEFAZ-CE cota-parte estadual adapter manual bloqueado, site bloqueia bots

## Status (atualizado pelo task-pilot)

> Esta seção é atualizada automaticamente. Não edite manualmente.

- Última execução: —
- Tasks done: 0/17
- Tasks blocked: 0
