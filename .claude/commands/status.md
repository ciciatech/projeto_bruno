---
description: Verifica status das aplicações em produção via Coolify e SSH
---

# /status

Resumo rápido do estado de produção do projeto Bruno.

## O que verificar

1. **Containers no servidor** (via SSH `root@72.60.152.227`):
   ```bash
   ssh root@72.60.152.227 "docker ps --format '{{.Names}}\t{{.Status}}' | grep -E 'p4c0o8wk|prisma'"
   ```
2. **Health endpoints**:
   - Streamlit: `curl -sI https://bruno.ciciatech.cloud/_stcore/health`
   - Prisma: `curl -sI https://prisma.bruno.ciciatech.cloud/healthz`
3. **GitHub Action mais recente**:
   ```bash
   gh run list --branch main --limit 3
   ```
4. **Pipeline regional CE no Mac Mini** (se o usuário pedir):
   ```bash
   ssh mac-mini-de-cassio.local "tail -20 ~/.local/log/bruno-pipeline-coletas.log; echo ---; tail -20 ~/.local/log/bruno-pipeline-chain.log"
   ```

Reporte em formato curto: `<app> <status> <fqdn> <último deploy> <observação>`.
