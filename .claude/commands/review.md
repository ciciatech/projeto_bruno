---
description: Code review focado nas convenções do projeto Bruno
---

# /review

Faça code review das mudanças staged/unstaged ou de um PR. Foque em:

## Checklist

### Pipeline Python
- [ ] Type hints presentes em funções públicas?
- [ ] Reusou `safe_request`, `save_dataframe`, `agregar_para_regiao` em vez de reimplementar?
- [ ] Logging via `logger = logging.getLogger(__name__)`?
- [ ] Imports do projeto absolutos (`from pipeline.X`)?
- [ ] Schema do output bate com o consumidor (painel regional, dashboard)?
- [ ] Cobertura: 184 municípios CE × 14 regiões mantida?

### Streamlit (`pages/`)
- [ ] `st.set_page_config` no topo?
- [ ] Caminhos via `PROCESSED_DIR`?
- [ ] Tratamento gracioso de CSV ausente?

### Frontend (`frontend/`)
- [ ] TypeScript estrito (sem `any`)?
- [ ] Aliases `@/` em vez de path relativo profundo?
- [ ] Build passa (`npm run build`)?
- [ ] Lint passa (`npm run lint`)?

### Segurança
- [ ] Sem credenciais/tokens em código (`.env`, `Bearer ...`, `password=`)?
- [ ] Sem SQL crú concatenado com input?
- [ ] CORS correto se for endpoint público?

### Git/Coolify
- [ ] Commit message segue padrão (feat/fix/chore/refactor)?
- [ ] Não modifica `.github/workflows/` sem necessidade?
- [ ] Não muda `build_pack`/Dockerfile sem testar localmente primeiro?

Liste problemas como `severidade: arquivo:linha — descrição`.
