---
description: Validação automática pré-deploy (lint, build, secrets, commits)
auto: true
trigger: "antes de push para main ou quando o usuário mencionar deploy"
---

# Deploy Validation

Antes de qualquer push para `main`:

1. **Validações por stack**:
   - Python: `python3 -c "import pipeline.run"`; rodar `pytest pipeline/tests/ -q` se existir.
   - Frontend: `cd frontend && npm run build && npm run lint`.
2. **Conferir segredos staged**: `git diff --cached | grep -iE "(api[_-]?key|password|token|secret|bearer )"`. Se algo aparecer, abortar.
3. **Verificar `docker-compose.yml` e `Dockerfile`**: se foram alterados, validar localmente com `docker compose build` antes do push.
4. **Listar commits que vão subir**:
   ```bash
   git fetch origin
   git log --oneline origin/main..HEAD
   ```
5. **Pedir confirmação explícita** ao usuário antes de fazer push para `main`.
6. **Após push**:
   - `gh run list --branch main --limit 3` para ver a Action.
   - SSH para conferir container saudável: `ssh root@72.60.152.227 "docker ps | grep -E 'p4c0o8wk|prisma'"`.
   - Curl no health: `curl -sI https://bruno.ciciatech.cloud/_stcore/health` ou `https://prisma.bruno.ciciatech.cloud/healthz`.

Endpoints, UUIDs e SSH em `.claude/rules/coolify-deploy.md`.
