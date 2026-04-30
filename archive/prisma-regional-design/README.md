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
| `screen03-05.jsx` | `frontend/src/screens/Setores/Causal/Pipeline.tsx` | Placeholders por enquanto; consultar este archive ao implementar |
| `Prisma_Regional_Deck.html` + `deck-stage.js` | — | Deck de apresentação 1920×1080; útil pra revisão de design |
| `Prisma_Regional_Canvas.html` | — | Design canvas com pan/zoom (todas as artboards lado a lado) |
| `design-canvas.jsx`, `tweaks-panel.jsx`, `spec.jsx` | — | Infra do design (não migrada; específica do prototipador) |

## O que ainda vale consultar aqui

- `screen03.jsx` (Setores) → ao implementar T04, copiar layout de stacked
  bars + heatmap LQ.
- `screen04.jsx` (Causal) → ao implementar T05, copiar scatter com regressão
  + tabela de resultados econométricos.
- `screen05.jsx` (Pipeline) → estrutura já portada em `frontend/src/screens/Pipeline.tsx`,
  mas o original tem detalhes de KPI banner que podem ser úteis.
- `Prisma_Regional_Deck.standalone.html` → versão self-contained do deck
  (útil pra apresentar offline ou exportar como PDF/PPTX).

## Como gerar o standalone novamente (se editar os JSX aqui)

```bash
python3 archive/prisma-regional-design/build_standalone.py
```

Saída em `archive/prisma-regional-design/Prisma_Regional_Deck.standalone.html`.

## Quando deletar este archive

Quando todas as 5 telas estiverem implementadas no `frontend/src/screens/`
com paridade visual, este diretório pode ser removido. Manter por enquanto
serve como referência viva do design original e da forma como ele se aplica
em outras stacks.
