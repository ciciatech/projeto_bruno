# Plano de descontinuação · `bruno-dashboard` (Streamlit)

> Documento vivo. Atualizado em 2026-04-30 enquanto migramos para o frontend
> React/Vite (`prisma-frontend`).

## Estado atual

| App | Stack | URL | Branch | Coolify UUID | Status |
|-----|-------|-----|--------|--------------|--------|
| `bruno-dashboard` | Streamlit 1.54 | https://bruno.ciciatech.cloud | main | `p4c0o8wkcgos8s0sscws8g8k` | 🟢 ativo |
| `prisma-frontend` | React 19 + Vite + TS + Tailwind | https://prisma.bruno.ciciatech.cloud | main (`base_directory=/frontend`) | `eomewrww9ecurlqvhb6vusml` | 🟢 ativo |

## Por que descontinuar

1. **Stack alinhada com o padrão CiciaTech**: o restante dos projetos usa
   React 19 + Vite + TypeScript + Tailwind v4 + shadcn/ui. Streamlit é um
   *outlier* que dificulta reuso de componentes, design system e CI/CD.
2. **Performance e densidade**: o desenho do Prisma Regional precisa de
   layout 1920×1080 com mapas custom e tipografia editorial. Streamlit força
   um chrome próprio (`st.set_page_config`, sidebar) que entra no caminho.
3. **Tipagem e testabilidade**: TS estrito + Vitest dão garantias que
   Streamlit não oferece.
4. **Single source of truth**: hoje a mesma análise vive em duas UIs. O
   pipeline de coletas é o mesmo; só a apresentação muda. Manter duas
   apresentações é dívida.

## Critérios para iniciar a descontinuação

A descontinuação **não começa** enquanto não houver paridade funcional. As
condições mínimas para mover `bruno.ciciatech.cloud` para o frontend são:

- [x] Tela 1 (Investimento) com dados reais 14 regiões CE
- [x] Tela 2 (Emprego) com dados reais CAGED
- [x] Tela 5 (Pipeline) listando fontes e status
- [ ] FilterBar funcional (período / indicador / recorte) — T06
- [ ] Tela 3 (Setores) com escopo definido — T04 ou substituição
- [ ] Tela 4 (Causal) com modelo OLS implementado — T05 (aguarda Paulo)
- [ ] Smoke tests Vitest cobrindo as 5 rotas — T15
- [ ] Validação final do Prof. Paulo

## Etapas (em ordem de execução)

### Etapa A · Paridade visual e de dados (em curso)

- T02 ✅, T06, T11 (shadcn/ui), T12 (cache hash). Continua até as 5 telas
  rodarem sem placeholders.

### Etapa B · Swap do domínio principal

1. Atualizar fqdn da app `prisma-frontend` no Coolify para incluir
   `bruno.ciciatech.cloud` (Traefik passa a resolver os dois para o nginx do
   frontend).
2. Atualizar a app `bruno-dashboard` para responder em algum subdomain
   transitório (ex: `bruno-streamlit.ciciatech.cloud`), garantindo que ainda
   esteja acessível durante 1–2 semanas como rollback.
3. Anunciar a mudança em `tasks.md` + commit semântico:
   `chore(infra): bruno.ciciatech.cloud passa a apontar para o frontend React`.

### Etapa C · Pausar `bruno-dashboard`

Após 7 dias sem regressão reportada:

```bash
COOLIFY_TOKEN=...   # do .env local, não comitar
curl -sS -X POST -H "Authorization: Bearer $COOLIFY_TOKEN" \
  "https://painel.ciciacademy.com.br/api/v1/applications/p4c0o8wkcgos8s0sscws8g8k/stop"
```

A app fica `stopped` no Coolify por 14 dias adicionais (rollback rápido se
algum stakeholder pedir uma vista que ainda não foi migrada).

### Etapa D · Remoção do código Streamlit

Quando as duas semanas adicionais passarem sem rollback:

```bash
git rm -r app.py pages/ Dockerfile docker-compose.yml
git rm -f .streamlit/config.toml
# manter dados_nordeste/, pipeline/, frontend/, scripts/
```

Atualizar `requirements.txt` removendo:

```
streamlit==1.54.0
```

Manter `pandas`, `plotly`, `pyarrow`, `requests`, `tqdm`, `py7zr`, `scipy`
(usados pelo pipeline ETL).

Commit final:

```
chore(streamlit): remover bruno-dashboard após migração para prisma-frontend
```

E excluir a app `bruno-dashboard` no Coolify via API:

```bash
curl -sS -X DELETE -H "Authorization: Bearer $COOLIFY_TOKEN" \
  "https://painel.ciciacademy.com.br/api/v1/applications/p4c0o8wkcgos8s0sscws8g8k"
```

## O que NUNCA descontinuar

- `pipeline/` (ETL, coletores, transformações). Fica como base do projeto.
  Roda no Mac Mini local + a qualquer momento via `python -m pipeline.run`.
- `dados_nordeste/processed/` (CSVs versionados). É o contrato com o
  frontend (ele lê via `frontend/scripts/build-data.py`).

## Riscos e mitigação

| Risco | Mitigação |
|-------|-----------|
| Stakeholder usa página específica do Streamlit que não foi migrada | Manter `bruno-dashboard` parado (não removido) por 14 dias adicionais. |
| Frontend depende de `painel.json` desatualizado | T07 (auto-regenerar painel.json) automatiza o passo. |
| Cobertura CAGED ainda parcial (9/14 regiões) | Tela 2 já sinaliza ausência de dado nas 5 regiões faltantes. Não bloqueia o swap. |
| Decisão pendente do Prof. Paulo (T04, T05, T08) | Tela 3/4 podem ficar como placeholders editoriais até decisão; não bloqueiam paridade do produto. |

## Cronograma proposto

- **Curto prazo**: Etapa A (continuar T06 / T11 / T12 / T15)
- **Marco de revisão**: quando Etapa A concluir, conversar com Prof. Paulo
  + Bruno antes de iniciar Etapa B
- **Após swap (Etapa B)**: 7 dias de soak + 14 dias com app pausada antes
  de remover código (Etapa D)
