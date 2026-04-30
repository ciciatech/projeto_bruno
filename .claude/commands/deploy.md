---
description: Deploy guiado para produção via Coolify (bruno-dashboard ou prisma-frontend)
---

# /deploy

Use para preparar e disparar deploy em produção. Não faz push direto — confirma com você antes.

## Passos

1. **Identifique o alvo**: `bruno-dashboard` (Streamlit) ou `prisma-frontend` (React).
2. **Validação local**:
   - Streamlit: `python3 -c "import pipeline.run"` + `python3 -m pytest pipeline/tests/ -q` se houver testes.
   - Frontend: `cd frontend && npm run build`.
3. **Verifique segredos**: `git diff --cached | grep -iE "(api[_-]?key|password|token|secret)"` — se algo aparecer, abortar.
4. **Liste commits que vão subir**:
   ```bash
   git fetch origin
   git log --oneline origin/main..HEAD
   ```
5. **Confirme com o usuário** antes de prosseguir.
6. **Deploy**: push para `main`. A GitHub Action `.github/workflows/deploy-coolify.yml` chama a API Coolify e dispara o redeploy.
7. **Acompanhe**:
   ```bash
   gh run list --branch main --limit 3
   ssh root@72.60.152.227 "docker ps --format '{{.Names}} {{.Status}}' | grep -E 'p4c0o8wk|prisma'"
   ```
8. **Validação pós-deploy**: abrir o domínio (`https://bruno.ciciatech.cloud` ou `https://prisma.bruno.ciciatech.cloud`) e conferir que carrega.

## Endpoints API (da rule `coolify-deploy.md`)

```bash
TOKEN="$COOLIFY_TOKEN"  # exporte localmente, nunca comite
BASE="https://painel.ciciacademy.com.br/api/v1"

# Forçar redeploy de uma app
curl -sS -X POST -H "Authorization: Bearer $TOKEN" "$BASE/applications/{UUID}/deploy"
```
