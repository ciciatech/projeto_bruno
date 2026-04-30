---
description: Analisa e corrige um issue específico, seguindo as convenções do projeto
argument-hint: <descrição-do-issue>
---

# /fix-issue $ARGUMENTS

Você recebeu o seguinte issue:

> $ARGUMENTS

## Plano

1. **Entenda**: identifique o que está descrito (bug, feature, tech-debt, refactor) e o módulo provável.
2. **Localize**: use `Grep`/`Glob` ou `Agent(subagent_type="Explore")` se a área não estiver óbvia. Verifique se há issue/PR relacionado.
3. **Reproduza**: rode o caso real (script, teste, dashboard) antes de mexer.
4. **Implemente** seguindo `.claude/rules/code-style.md`. Reuse helpers existentes; não introduza dependências novas sem justificativa.
5. **Valide**:
   - Python: `python3 -m pytest pipeline/tests/ -q` (se existir teste relevante) e import smoke (`python3 -c "import pipeline.run"`).
   - Streamlit: `streamlit run app.py` e checar a página afetada.
   - Frontend: `cd frontend && npm run build && npm run lint`.
6. **Commit** seguindo o padrão do projeto (`feat:`, `fix:`, `refactor:`...) com mensagem que explica o PORQUÊ.

Se o issue tocar deploy/infra, ler `.claude/rules/coolify-deploy.md` antes.
