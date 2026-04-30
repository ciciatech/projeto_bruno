---
description: Convenções de código do projeto Bruno (pipeline Python + Streamlit + frontend React)
globs: ["**/*.py", "**/*.tsx", "**/*.ts", "**/*.jsx", "**/*.js"]
---

# Code style · projeto Bruno

Stack heterogênea: pipeline ETL em Python 3.13 + dashboard Streamlit + frontend React (Vite/TS).

## Python (`pipeline/`, `pages/`, `app.py`)

- Type hints obrigatórios em funções públicas (ex.: `def coletar_ce() -> pd.DataFrame`).
- Imports em ordem: stdlib, terceiros, projeto. Sem star imports.
- Logging via `logger = logging.getLogger(__name__)`. Nunca `print()` em código de produção.
- HTTP requests externos passam por `pipeline.utils.safe_request` (retry/backoff).
- Persistência via `pipeline.utils.save_dataframe` — não escrever CSV/parquet manualmente.
- Mapeamento município→região via `pipeline.regioes_ce.agregar_para_regiao` (não recriar lógica).
- Imports do projeto sempre absolutos: `from pipeline.config import PROCESSED_DIR` (nunca relativo).

## Streamlit (`pages/`)

- Cada página começa com `st.set_page_config(page_title="...", layout="wide")`.
- Caminhos de dados resolvem por `pipeline.config.PROCESSED_DIR`, nunca hardcoded.
- Antes de ler CSV: checar `path.exists()` e mostrar erro útil com `st.error`/`st.info`.
- Plotly como gráfico padrão; evitar custom HTML salvo casos justificados.

## Frontend (`frontend/src/`)

- React 19 + TypeScript estrito (`tsconfig.app.json`). JSX antigo do design (`/public/deck.html`) é embed via iframe — não importar diretamente.
- Aliases: `@/` aponta para `frontend/src`. Use `import { X } from "@/components/..."`.
- TailwindCSS v4 via plugin `@tailwindcss/vite`. CSS vars do design ficam em `src/index.css`.
- Componentes shadcn/ui em `src/components/ui/` (instalar via `npx shadcn@latest add ...`).
- Hooks de API em `src/hooks/`, stores Zustand em `src/store/`, types em `src/types/`.

## Geral

- Sem comentários explicando O QUÊ — apenas o PORQUÊ não-óbvio.
- Sem mocks/fallbacks defensivos para inputs que vêm do nosso próprio código.
- PRs pequenos. Não misturar refator com feature.
- Português para domínio (variáveis: `regiao_codigo`, `salario_medio`); inglês para infra (Dockerfile, CI).
