# QA UX/Fluidez/Performance · Prisma Regional CE

Cole o **prompt principal** abaixo numa sessão **Claude com browser tool** (Claude Chrome / Claude Desktop com Computer Use / outro agente que controle navegador real). Ele vai navegar pelas 5 telas, capturar screenshots, medir performance, testar interações e devolver um relatório estruturado.

> **Como me devolver**: cole as imagens (drag-and-drop) + o relatório final aqui no Claude Code. Eu analiso causa-raiz e abro PR com fixes.

---

## Prompt principal (copiar tudo abaixo da linha)

```
# QA Visual + UX + Performance · Prisma·Regional

Você é um avaliador de UX especialista em dashboards densos com vocação editorial
(referência: FT Visual & Data Journalism, Our World in Data, The Economist
Graphic Detail, Observable Plot). Sua missão é fazer uma **avaliação completa**
do app em https://prisma.bruno.ciciatech.cloud — não só "achar bugs" mas
**diagnosticar** o que faz a experiência fluir ou travar.

## CONTEXTO TÉCNICO

- **Stack**: React 19 + Vite + TypeScript + TailwindCSS v4 + react-router-dom (sem shadcn ainda)
- **Domínio**: análise econômica das 14 regiões SEPLAG/IPECE do Ceará. Painel mensal 2015-2025
- **5 rotas SPA**: `/investimento` (default), `/emprego`, `/setores`, `/causal`, `/pipeline`
- **Filtros globais**: período (1A/3A/5A/10A/Tudo) + recorte (Bruto/Per capita/% PIB) com estado em URL via `?periodo=5A&recorte=Bruto`
- **Tema**: claro (default) e escuro persistente em localStorage
- **Linguagem visual**: paleta âmbar/terra editorial, Source Serif 4 (manchetes), Inter (UI), JetBrains Mono (números tabulares)
- **Design canvas authored em 1920×1080** — comportamento em viewports menores é o ponto crítico

## PROTOCOLO DE AVALIAÇÃO

Execute os 6 blocos em ordem. Capture screenshots ao longo do caminho.

---

### BLOCO 1 · Setup e baseline (2 min)

1. Abra https://prisma.bruno.ciciatech.cloud em janela limpa (sem extensões interferindo)
2. Defina viewport **1440×900** (notebook 14" típico — o caso médio do uso real)
3. Aceite warning de cert se aparecer
4. Abra DevTools → Network → habilite "Disable cache" e "Preserve log"
5. Faça hard reload (Cmd+Shift+R)
6. **Capture screenshot 01-baseline-investimento.png** assim que a tela carregar

Anote no relatório:
- LCP (Largest Contentful Paint) do Performance tab
- FCP (First Contentful Paint)
- TTFB do request inicial
- Total de requests no Network
- Tamanho transferido (gzipped) e descompactado
- Há requests com status ≠ 200/304? Quais?

---

### BLOCO 2 · Navegação e fluidez (5 min)

Avalie a transição entre telas:

1. Clique em cada item do header (Investimento → Emprego → Setores → Causal → Pipeline) e meça quanto tempo o conteúdo principal leva pra atualizar (visualmente). Use o cronômetro do DevTools Performance se possível.
2. Use o **botão Back** do navegador entre as telas — funciona? URL atualiza corretamente?
3. **Deep link**: abra https://prisma.bruno.ciciatech.cloud/causal?periodo=3A em aba nova — a tela carrega já com filtro aplicado?
4. Pressione **Tab** repetidamente — a ordem de foco faz sentido? O foco fica visualmente claro?
5. Use **←/→** do teclado — funciona pra navegar entre telas? (não deve, é app SPA, não deck — confirmar que NÃO interfere acidentalmente)

**Capture screenshots** de cada tela após carregamento completo:
- 02-investimento.png · 03-emprego.png · 04-setores.png · 05-causal.png · 06-pipeline.png

Anote:
- Há "flash" de conteúdo (FOUT/FOUC)?
- Loading states aparecem? São consistentes entre telas?
- Há jank visual (jitter, layout shift) durante a transição?
- CLS (Cumulative Layout Shift) do Performance tab

---

### BLOCO 3 · Filtros e estado (3 min)

1. Em /investimento, clique cada botão de período (1A → 3A → 5A → 10A → Tudo) e observe:
   - O mapa atualiza? Os KPIs recalculam? Sparklines mudam?
   - URL reflete a mudança (`?periodo=5A`)?
   - Há transição animada ou é troca brusca?
   - Latência da atualização (instantânea, <500ms, >1s)?

2. Clique "Per capita" no Recorte. Observe:
   - Os valores mudam (devem ser dividos por população)?
   - Unidades nas labels mudam?
   - **Captura especial**: 07-investimento-per-capita.png

3. Clique "% PIB" — está disabled visualmente? Tooltip explicativo?

4. Clique no botão "↺ limpar" — volta tudo ao default?

5. Repita o teste de filtro em /emprego e /causal. Os filtros são aplicados consistentemente?

---

### BLOCO 4 · Interações nas telas (8 min)

#### /investimento
- Clique numa região do mapa coroplético (ex: Cariri tile 01) — o painel direito atualiza?
- Hover nos tiles do mapa — há tooltip ou feedback visual?
- A tabela "Snapshot · 14 regiões" é clicável? Sincroniza com o mapa?
- Os KPIs com sparkline têm dados consistentes (variação YoY, share, etc.)?
- A composição "Por modalidade" (4 esferas) tem barras visíveis para Estadual e Federal? Municipal e Privado mostram "pendente"?

#### /emprego
- Mapa divergente (cores frias = perda, quentes = ganho) faz sentido?
- Regiões sem dado CAGED ficam em cinza?
- Clicar numa região atualiza o painel direito com saldo, adm, des, sal_med?

#### /setores
- Stacked bars das 14 regiões está legível?
- Categorias (FPM, FUNDEB, ITR, royalties, outros, BF, BPC) têm cores distinguíveis?
- Legend embaixo é readable?
- Tabela de "Composição agregada CE" mostra share % correto?

#### /causal
- Scatter plot tem pontos visíveis?
- Linha de regressão laranja é desenhada?
- Banda de IC 95% (semitransparente) aparece?
- Tabela direita mostra β, α, R², σ, n com valores numéricos plausíveis?
- Hover destaca pontos da mesma região?

#### /pipeline
- Tabela das 11 fontes está completa?
- Badges coloridos (OK / RODANDO / PENDENTE / BLOQUEADO) destacam-se bem?

**Captures adicionais**: 08-investimento-regiao-clicada.png, 09-causal-hover.png

---

### BLOCO 5 · Tema escuro e persistência (3 min)

1. Clique "◐ escuro" no header. Captura: **10-investimento-dark.png**
2. Navegue para /emprego — tema persiste?
3. Recarregue (F5) — tema persiste após reload? (deve, via localStorage)
4. Cole `localStorage.getItem('prisma-theme')` no console — retorna `"dark"`?
5. Captura: **11-pipeline-dark.png**
6. No tema escuro, verifique:
   - Contraste de texto legível? (target WCAG AA = 4.5:1 para body, 3:1 para large)
   - Mapa coroplético tem tiles distinguíveis?
   - Sparklines visíveis?
   - Links/botões mantêm affordance?

---

### BLOCO 6 · Responsividade e edge cases (5 min)

1. **Viewport 1280×800** (laptop pequeno): redimensione e capture **12-investimento-1280.png**.
   - Conteúdo ainda cabe sem clipping?
   - Há scroll horizontal indesejado?
   - Mapa coroplético encolhe proporcionalmente?

2. **Viewport 1024×768** (tablet landscape): captura **13-investimento-1024.png**.
   - Layout colapsa graciosamente ou quebra?

3. **Viewport mobile 414×896**: captura **14-investimento-mobile.png**.
   - É usável? (não precisa ser perfeito, mas não pode mostrar conteúdo cortado/sobreposto)

4. **Console JavaScript**: rode estes comandos e cole o resultado:
   ```js
   document.fonts.ready.then(() => Array.from(document.fonts).map(f => `${f.family} (${f.status})`))
   ```
   ```js
   getComputedStyle(document.querySelector('h1')).fontFamily
   ```
   ```js
   getComputedStyle(document.documentElement).getPropertyValue('--seq-3').trim()
   ```

5. **Network throttling Slow 3G**: ative no DevTools, recarregue /investimento e meça de novo o LCP. Quantos segundos até ser usável?

---

## FORMATO DO RELATÓRIO

Devolva nesta estrutura (markdown):

```
# Avaliação Prisma Regional CE — UX/Fluidez/Performance

## Resumo executivo
[2-3 parágrafos]: o que funciona, o que precisa atenção urgente, percepção geral de polimento (1-10).

## Performance (números reais)
| Métrica | 1440×900 / Fast | Slow 3G |
|---------|-----------------|---------|
| FCP | __ ms | __ ms |
| LCP | __ ms | __ ms |
| CLS | __ | __ |
| INP médio | __ ms | __ ms |
| Bundle JS gzip | __ KB | |
| Bundle CSS gzip | __ KB | |
| painel.json | __ KB | |
| Total transferido | __ KB | |
| Requests | __ | |

## UX por tela (5 blocos)
Para CADA uma das 5 telas:
- ✅ O que funciona bem
- ⚠️ O que está confuso/quebrado
- 💡 Sugestões de melhoria

## Heurísticas de Nielsen (1-10 cada)
1. Visibilidade do status do sistema
2. Correspondência com mundo real
3. Controle e liberdade do usuário
4. Consistência e padrões
5. Prevenção de erros
6. Reconhecimento ao invés de memória
7. Flexibilidade e eficiência
8. Estética e design minimalista
9. Recuperação de erros
10. Ajuda e documentação

## Acessibilidade
- [ ] Contraste WCAG AA?
- [ ] Tab order lógico?
- [ ] Focus visível?
- [ ] Labels nos botões/links?
- [ ] Screen reader friendly?
- [ ] Keyboard-only navigable?

## Fluidez (motion design)
- Transições entre telas: ___
- Filtros aplicados: ___
- Hovers e feedback tátil: ___
- Loading states: ___

## Coerência editorial
- Paleta âmbar consistente? Onde escapa?
- Tipografia (serif manchetes, mono números)? Onde falha?
- Hierarquia visual clara?
- Tom acadêmico mantido?

## Top 5 problemas críticos
[severidade · descrição · causa provável · onde está · sugestão de fix]

## Top 5 oportunidades de polish
[descrição · ganho percebido · custo estimado]

## Screenshots anexados
Lista de 14 imagens com legendas curtas.
```

## OUTRAS DIRETRIZES

- Seja **específico**: ao invés de "o filtro está confuso", diga "o filtro Período não dá feedback de qual está ativo no estado URL inicial".
- Seja **comparativo**: cite referências (FT, OWiD, Bloomberg) quando útil.
- **Não corrija** — só diagnostique. O Cássio (Claude Code com Sonnet 4.6) cuida da implementação dos fixes.
- **Ignore o conteúdo de placeholder**: telas Setores/Causal são placeholders intencionais — avalie a UX do placeholder, não seu vazio.
- **Tempo total esperado**: 25-30 minutos.

ENTREGUE: relatório markdown + 14+ screenshots inline.
```

---

## Apêndice — usuários típicos para usar como persona

Sugiro o agente assumir, ao avaliar:

1. **Bruno (doutorando)** — quer ver os números do modelo, validar coleta, encontrar regiões anômalas. Ferramenta de pesquisa.
2. **Prof. Paulo (orientador)** — quer screenshots para apresentação, leitura editorial, comparação rápida. Ferramenta de comunicação.
3. **Outro pesquisador externo (par/banca)** — vai abrir uma vez, em laptop, sem nenhum contexto. Precisa entender em 30 segundos. Ferramenta de divulgação.

Pode incluir uma seção "Persona test: Bruno levou 12s pra encontrar X" no relatório.

---

## Como me devolver

1. Salve os 14+ screenshots numa pasta zipada (`prisma-qa-2026-04-30.zip`)
2. Cole o relatório markdown aqui no Claude Code
3. Anexe as imagens (drag-and-drop nesta conversa) — eu vou olhar uma a uma
4. Se houver bugs críticos urgentes, me avise antes (em texto livre) e eu já abro fix
