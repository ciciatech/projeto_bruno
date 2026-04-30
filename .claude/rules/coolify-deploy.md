---
description: Configuração e regras de deploy via Coolify para o projeto Bruno
---

# Coolify · projeto Bruno

## Coolify

- **Painel**: https://painel.ciciacademy.com.br
- **Project**: `Projeto Bruno - Tese DESP/UFC` (uuid `zkkg4s0soock4sswg0ossk0g`)
- **Server**: localhost (Hostinger srv1068785, IP `72.60.152.227`)
- **Proxy**: Traefik 3.1.7
- **Token de API**: armazenado no `.mcp.json` e em `~/.coolify-tokens` (não comitar)

## Aplicações registradas

### bruno-dashboard (Streamlit, atual)
- **uuid**: `p4c0o8wkcgos8s0sscws8g8k`
- **fqdn**: https://bruno.ciciatech.cloud
- **build_pack**: `dockerfile` (raiz do repo)
- **base_directory**: `/`
- **branch**: `main`
- **repo**: `ciciatech/projeto_bruno.git`
- **porta**: 8501 (Streamlit)
- **deploy**: automático via GitHub Action `.github/workflows/deploy-coolify.yml` em push para `main`

### prisma-frontend (React/Vite, novo — em criação)
- **fqdn pretendido**: `https://prisma.bruno.ciciatech.cloud`
- **build_pack**: `dockerfile` (Dockerfile dentro de `frontend/`)
- **base_directory**: `/frontend`
- **branch**: `main`
- **porta**: 80 (nginx)
- **deploy**: automático via mesma GitHub Action ou webhook próprio

## Endpoints úteis da API Coolify

```bash
TOKEN="3|Oq2dlr3XaeysO89IdzrUm80O9B1yyD1tJzFHLO2O4db663ee"
BASE="https://painel.ciciacademy.com.br/api/v1"

# Listar applications
curl -sS -H "Authorization: Bearer $TOKEN" "$BASE/applications"

# Detalhes de uma application (vem nulo no GET singular; preferir filtrar a lista)
curl -sS -H "Authorization: Bearer $TOKEN" "$BASE/applications" | jq '.[] | select(.name=="bruno-dashboard")'

# Variáveis de ambiente
curl -sS -H "Authorization: Bearer $TOKEN" "$BASE/applications/{UUID}/envs"

# Disparar redeploy
curl -sS -X POST -H "Authorization: Bearer $TOKEN" "$BASE/applications/{UUID}/deploy"

# Status do servidor
curl -sS -H "Authorization: Bearer $TOKEN" "$BASE/servers"
```

## Acesso SSH ao servidor de produção

```bash
ssh root@72.60.152.227
# Chave: ~/.ssh/id_ed25519 (cassiopo7@gmail.com)

# Ver containers do projeto
ssh root@72.60.152.227 "docker ps --format '{{.Names}} {{.Status}}' | grep p4c0o8wk"

# Logs do bruno-dashboard
ssh root@72.60.152.227 "docker logs --tail 100 \$(docker ps --format '{{.Names}}' | grep p4c0o8wk)"

# Restart de container (Coolify trata, mas fallback útil)
ssh root@72.60.152.227 "docker restart \$(docker ps --format '{{.Names}}' | grep p4c0o8wk)"
```

## Regras de deploy

1. **Nunca editar config do app no Coolify**. Tudo passa pelo repositório.
2. **Branch produção**: `main`. Push direto em `main` aciona deploy.
3. **Branch desenvolvimento**: `dev`. PR/merge em `main` quando pronto.
4. **Auto-pull no Mac Mini**: o `scripts/git-auto-pull.sh` puxa `dev` no Mac Mini para coletas — não conflita com deploy de produção.
5. **Webhook GitHub→Coolify**: o webhook nativo está quebrado; o workaround é a GitHub Action que chama a API Coolify.
6. **Antes de push em `main`**: rodar lint/build localmente, conferir que `.env`/secrets não vazaram, listar commits que vão subir.
