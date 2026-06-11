# Archive · Prisma Regional Design (snapshot original)

Bundle de design entregue por [claude.ai/design](https://claude.ai/design) em
abr/2026, contendo as 5 telas hi-fi do dashboard Prisma Regional + o sistema
de design (paleta âmbar, tipografia Source Serif 4 / Inter / JetBrains Mono,
wireframes ASCII, padrões de componentes).

## Por que mover para `archive/`

Os componentes deste bundle foram portados para `frontend/src/` (React + TS +
Vite), com adaptações:

| Arquivo original | Equivalente no app real | Notas |
|------------------|--------------------------|-------|
| `tokens.css` | `frontend/src/lib/tokens.css` | Idêntico |
| `chrome.jsx` (AppHeader/FilterBar/Panel/KPI/...) | `frontend/src/components/Chrome.tsx`, `Panel.tsx`, `KPI.tsx`, `Sparkline.tsx` | Quebrado em arquivos por componente; tipado em TS |
| `charts.jsx` (ChoroplethBR) | `frontend/src/components/Choropleth14CE.tsx` | Adaptado de 27 UFs Brasil → 14 regiões SEPLAG/CE |
| `data.jsx` (dados sintéticos) | `frontend/scripts/build-data.py` + `frontend/public/data/painel.json` | Dados sintéticos substituídos pelo painel real |
| `screen01.jsx` (Investimento) | `frontend/src/screens/Investimento.tsx` | Adaptado para CE (SIOF + invest_federal + IBCR + FBCF) |
| `screen02.jsx` (Emprego) | `frontend/src/screens/Emprego.tsx` | Adaptado para dados CAGED reais |
| `screen03-05.jsx` | `frontend/src/screens/Setores/Causal/Pipeline.tsx` | Implementadas com dados reais (T04 Setores — pivot p/ Composição de Receitas; T05 Causal — OLS preliminar; Pipeline) |
| `Prisma_Regional_Deck.html` + `deck-stage.js` | — | Deck de apresentação 1920×1080; útil pra revisão de design |
| `Prisma_Regional_Canvas.html` | — | Design canvas com pan/zoom (todas as artboards lado a lado) |
| `design-canvas.jsx`, `tweaks-panel.jsx`, `spec.jsx` | — | Infra do design (não migrada; específica do prototipador) |

## O que ainda vale consultar aqui

- `screen03.jsx` (Setores) → referência do layout de stacked bars + heatmap LQ
  (T04 já implementada com pivot para Composição de Receitas).
- `screen04.jsx` (Causal) → referência do scatter com regressão + tabela de
  resultados econométricos (T05 já implementada com OLS univariado).
- `screen05.jsx` (Pipeline) → estrutura já portada em `frontend/src/screens/Pipeline.tsx`,
  mas o original tem detalhes de KPI banner que podem ser úteis.
- `Prisma_Regional_Deck.standalone.html` → versão self-contained do deck
  (útil pra apresentar offline ou exportar como PDF/PPTX).

## Como gerar o standalone novamente (se editar os JSX aqui)

```bash
python3 archive/prisma-regional-design/build_standalone.py
```

Saída em `archive/prisma-regional-design/Prisma_Regional_Deck.standalone.html`.

## Status do critério de deleção

O critério original ("deletar quando as 5 telas estiverem implementadas com
paridade visual") **foi atingido** — as 5 telas rodam com dados reais no
`frontend/src/screens/` (ver `tasks.md` T02/T04/T05 e `CLAUDE.md`).

**Decisão (jun/2026): manter o archive** como referência viva do design
original (o deck standalone segue útil para apresentações e para portar o
design a outras stacks). Reavaliar a remoção na Etapa D do plano de
descontinuação do Streamlit (`docs/plano-descontinuacao-streamlit.md`).
