/* Painéis de especificação: paleta, tipografia, wireframes ASCII,
   componentes, justificativas. Renderizados como artboards no canvas. */

function SpecPaleta() {
  const swatch = (n, v, label) => (
    <div key={n} style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
      <div style={{ background: v, height: 56, border: '1px solid var(--border-soft)' }} />
      <div className="mono" style={{ fontSize: 10, color: 'var(--ink-3)' }}>{n}</div>
      <div className="mono" style={{ fontSize: 10, color: 'var(--ink-2)' }}>{label}</div>
    </div>
  );
  return (
    <div style={{ padding: 24, background: 'var(--bg-page)', height: '100%', overflow: 'auto' }}>
      <div className="mono" style={{ fontSize: 10, color: 'var(--ink-3)', textTransform: 'uppercase', letterSpacing: 0.8, fontWeight: 600 }}>00 · Sistema</div>
      <h1 className="serif" style={{ margin: '4px 0 6px', fontSize: 28, fontWeight: 600, letterSpacing: -0.5 }}>Paleta de cores</h1>
      <p style={{ margin: '0 0 24px', fontSize: 13, color: 'var(--ink-3)', maxWidth: 620, lineHeight: 1.55 }}>
        Sistema âmbar/terra com divergente teal—âmbar. Inspiração: cor da terra brasileira, sépia de impressão, paletas editoriais (FT/OWiD). Evita primárias saturadas e neon.
        Todos os tons foram escolhidos para passar AA em texto pequeno sobre fundo claro/escuro.
      </p>

      <h2 className="serif" style={{ fontSize: 17, fontWeight: 600, margin: '0 0 12px' }}>Sequencial · 5 stops</h2>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: 8, marginBottom: 24 }}>
        {[
          ['seq-1','#fef3c7','quintil 1 (mín.)'],
          ['seq-2','#fcd34d','quintil 2'],
          ['seq-3','#d97706','quintil 3 · default accent'],
          ['seq-4','#92400e','quintil 4'],
          ['seq-5','#451a03','quintil 5 (máx.)'],
        ].map((s) => swatch(s[0], s[1], s[2]))}
      </div>
      <div style={{ fontSize: 11, color: 'var(--ink-3)', marginBottom: 24 }}>
        <strong style={{ color: 'var(--ink-1)' }}>Uso:</strong> mapas coropléticos com valores monotônicos (investimento, PIB, exportações, registros).
      </div>

      <h2 className="serif" style={{ fontSize: 17, fontWeight: 600, margin: '0 0 12px' }}>Divergente · 7 stops</h2>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(7, 1fr)', gap: 8, marginBottom: 24 }}>
        {[
          ['div-neg-3','#134e4a','−10%'],
          ['div-neg-2','#5c9c97','−5%'],
          ['div-neg-1','#b8d6d3','−1%'],
          ['div-zero','#f5f1e6','0'],
          ['div-pos-1','#fcd9a8','+1%'],
          ['div-pos-2','#d97706','+5%'],
          ['div-pos-3','#92400e','+10%'],
        ].map((s) => swatch(s[0], s[1], s[2]))}
      </div>
      <div style={{ fontSize: 11, color: 'var(--ink-3)', marginBottom: 24 }}>
        <strong style={{ color: 'var(--ink-1)' }}>Uso:</strong> variação YoY, choques, balança comercial — qualquer indicador com zero como referência.
      </div>

      <h2 className="serif" style={{ fontSize: 17, fontWeight: 600, margin: '0 0 12px' }}>Categórico setorial · 6</h2>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(6, 1fr)', gap: 8, marginBottom: 24 }}>
        {[
          ['cat-1','#92400e','Indústria'],
          ['cat-2','#1f6f6a','Serviços'],
          ['cat-3','#c5811f','Agropecuária'],
          ['cat-4','#4a5d3a','Construção'],
          ['cat-5','#6b3a5c','Comércio'],
          ['cat-6','#2a4060','Adm. Pública'],
        ].map((s) => swatch(s[0], s[1], s[2]))}
      </div>

      <h2 className="serif" style={{ fontSize: 17, fontWeight: 600, margin: '24px 0 12px' }}>Neutros · warm gray</h2>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(7, 1fr)', gap: 8 }}>
        {[
          ['ink-1','#1c160e','texto principal'],
          ['ink-2','#3d362a','texto secundário'],
          ['ink-3','#6b6354','meta / eixo'],
          ['ink-4','#8f8775','desabilitado'],
          ['border','#e6e0d3','linha suave'],
          ['surface','#ffffff','painel'],
          ['page','#f7f4ee','página'],
        ].map((s) => swatch(s[0], s[1], s[2]))}
      </div>

      <h2 className="serif" style={{ fontSize: 17, fontWeight: 600, margin: '32px 0 8px' }}>Justificativa</h2>
      <ul style={{ fontSize: 12, color: 'var(--ink-2)', lineHeight: 1.65, paddingLeft: 18, maxWidth: 720 }}>
        <li>Âmbar (terra/argila) substitui o azul-default de dashboards corporativos. Mantém leitura como dado quantitativo sem evocar SaaS.</li>
        <li>Teal escuro como contraponto frio para o lado negativo do divergente. Funciona em dark mode (invertido para teal claro).</li>
        <li>Categóricos limitados a 6: forçar agregação em vez de proliferar séries.</li>
        <li>Saturação dos brancos &lt; 0,02 (oklch). Fundo bege evita brilho excessivo em sessões longas.</li>
      </ul>
    </div>
  );
}

function SpecTipografia() {
  const Sample = ({ size, family, label, weight = 400, children }) => (
    <div style={{ display: 'grid', gridTemplateColumns: '120px 80px 1fr', gap: 16, alignItems: 'baseline', borderBottom: '1px solid var(--border-soft)', padding: '12px 0' }}>
      <div className="mono" style={{ fontSize: 10, color: 'var(--ink-3)' }}>{label}</div>
      <div className="mono" style={{ fontSize: 10, color: 'var(--ink-3)' }}>{size}px · {weight}</div>
      <div style={{ fontFamily: family, fontSize: size, fontWeight: weight, color: 'var(--ink-1)', lineHeight: 1.2 }}>{children}</div>
    </div>
  );
  return (
    <div style={{ padding: 24, background: 'var(--bg-page)', height: '100%', overflow: 'auto' }}>
      <div className="mono" style={{ fontSize: 10, color: 'var(--ink-3)', textTransform: 'uppercase', letterSpacing: 0.8, fontWeight: 600 }}>00 · Sistema</div>
      <h1 className="serif" style={{ margin: '4px 0 6px', fontSize: 28, fontWeight: 600, letterSpacing: -0.5 }}>Tipografia</h1>
      <p style={{ margin: '0 0 16px', fontSize: 13, color: 'var(--ink-3)', maxWidth: 620, lineHeight: 1.55 }}>
        <strong style={{ color: 'var(--ink-1)' }}>Source Serif 4</strong> (manchetes editoriais) +
        <strong style={{ color: 'var(--ink-1)' }}> Inter</strong> (UI/corpo) +
        <strong style={{ color: 'var(--ink-1)' }}> JetBrains Mono</strong> (números, código, eixos).
        Todas free, com excelente cobertura para acentuação portuguesa (ç, ã, ê, ó, etc.) e numerais tabulares.
      </p>

      <Sample size={40} family="var(--font-serif)" weight={600} label="display">Investimento privado por unidade federativa</Sample>
      <Sample size={28} family="var(--font-serif)" weight={600} label="h1">Composição setorial das economias regionais</Sample>
      <Sample size={20} family="var(--font-serif)" weight={600} label="h2">Concentração persistente</Sample>
      <Sample size={15} family="var(--font-sans)" weight={600} label="h3">Top 10 municípios · 2024</Sample>
      <Sample size={13} family="var(--font-sans)" label="body">Cada ponto representa uma unidade federativa observada em 2024. O raio é proporcional à população.</Sample>
      <Sample size={12} family="var(--font-sans)" label="small">Fonte: BNDES + Investe SP + Comex Stat — n = 5.570 municípios</Sample>
      <Sample size={10.5} family="var(--font-sans)" weight={600} label="eyebrow">DIAGRAMA DE DISPERSÃO</Sample>
      <Sample size={26} family="var(--font-mono)" weight={600} label="numeral xl">R$ 142.847.300.000</Sample>
      <Sample size={13} family="var(--font-mono)" label="numeral inline">+12,4% · n=27 · p=0,0028 · R²=0,34</Sample>
      <Sample size={11} family="var(--font-mono)" label="código">ipea_regional.api.v2/investimento?uf=SP&period=2024</Sample>

      <h2 className="serif" style={{ fontSize: 17, fontWeight: 600, margin: '32px 0 8px' }}>Justificativa</h2>
      <ul style={{ fontSize: 12, color: 'var(--ink-2)', lineHeight: 1.65, paddingLeft: 18, maxWidth: 720 }}>
        <li><strong>Source Serif 4</strong> em manchetes evoca trabalho acadêmico/jornalismo de dados (FT, NYT, OWiD) sem cair em Times/Georgia genéricos.</li>
        <li><strong>Inter</strong> para UI/corpo: alta legibilidade em densidade alta, suporta numerais tabulares, oito pesos.</li>
        <li><strong>JetBrains Mono</strong> para todos os números e códigos: ligaduras desativadas (font-feature `liga 0`), tabular nums forçados via CSS — alinhamento de R$ em colunas.</li>
        <li>Acentuação testada em todos os caracteres do português brasileiro, incluindo ç maiúsculo e dígrafos.</li>
        <li>Display ≥ 24px em 1920×1080 mantém leitura confortável a 1m em apresentações.</li>
      </ul>
    </div>
  );
}

function SpecWireframes() {
  const ascii = `
┌─ TELA 01 · INVESTIMENTO ────────────────────────────────────────────┐
│ HEADER: Prisma·Regional   Inv  Emp  Set  Causal  Pipe  ◐ ↗  user@ │
│ FILTERS: [Período][Geo][Indicador][Setor][Recorte]  +filtro [Aplicar]│
├─────────────────────────────────────────────────────────────────────┤
│ # Investimento privado anunciado por UF · 2024 · R$ 745 bi          │
├──────────┬──────────────────────────────────────────────┬───────────┤
│ KPI: tot │  ▓▓▓▓▓ MAPA COROPLÉTICO                      │ TOP 10    │
│ R$ 745bi │  ░░▓▓▓ (escala sequencial âmbar)             │ municípios│
│ ▲ +12,4% │  ░░░▓▓ tile UF + valor mono inline           │ tabela    │
│          │  ░░░░▓ hover → tooltip · click → fix         │ densa     │
│ KPI:cob  │  ░░░░░                                       │ + spark   │
│ 27/27    │                                              ├───────────┤
│          │  legenda gradiente · selecionado: SP         │ Por modal:│
│ KPI:SE%  │  por região breakdown                        │ Greenfld  │
│ 40,5%    │  última atualização                          │ Expansão  │
│ ▼ −1,8%  │                                              │ M&A...    │
│          │                                              │           │
│ TOP 6 UF │                                              │           │
│ HBars    │                                              │           │
│ + nota   │                                              │           │
│ editorial│                                              │           │
└──────────┴──────────────────────────────────────────────┴───────────┘

┌─ TELA 02 · EMPREGO ─────────────────────────────────────────────────┐
│ HEADER + FILTERS (mesmos)                                           │
│ # Dinâmica do emprego formal · 2020T1 → 2025T2                      │
├──────────────────────────┬──────────────────────────────────────────┤
│  ▓▓▓ MAPA YoY (diverg.)  │ LINHAS · estoque por região (5 séries)   │
│  ░░░ teal ← 0 → âmbar    │ — Sudeste (espessa)                      │
│  legenda · top↑ · top↓   │ — Sul                                    │
│  + nota Matopiba         │ — Nordeste                               │
│                          │ — Centro-Oeste                           │
│                          │ — Norte                                  │
│                          ├──────────────────────────────────────────┤
│                          │ TABELA densa · UF | Reg | Estq | YoY |  │
│                          │ Desemp | spark 8T (cor pelo YoY)         │
│                          │ scroll virtual · 27 linhas               │
└──────────────────────────┴──────────────────────────────────────────┘

┌─ TELA 03 · SETORES ─────────────────────────────────────────────────┐
│ # Composição setorial das economias regionais · 2024                │
├──────────────────────────────────────────┬──────────────────────────┤
│ STACKED BARS · 5 regiões                 │ HBARS · top crescimento  │
│ ▓▓▓▓░░░░  Norte                          │ Agro·MT      +18,4%     │
│ ▓▓▓▓▓░░░  Nordeste                       │ Indústr·SC   +12,7%     │
│ ▓▓░░▓▓▓░  C-Oeste                        │ Servs·DF     +8,1%      │
│ ▓▓▓▓▓░░░  Sudeste                        │ ...                      │
│ ▓▓▓▓▓░░░  Sul                            │ Cnstr·RR     −8,1%      │
│ legenda 6 setores                        │ + nota furtado-revisited │
├──────────────────────────────────────────┤                          │
│ HEATMAP · LQ setor × UF (15 UFs × 6 set) │                          │
│ valores divergentes em 6 buckets         │                          │
│ leitura: > 1,0 = especialização          │                          │
└──────────────────────────────────────────┴──────────────────────────┘

┌─ TELA 04 · RELAÇÃO CAUSAL ──────────────────────────────────────────┐
│ # Investimento → emprego: existe causalidade regional?              │
├──────────────────────────────────────────┬──────────────────────────┤
│ SCATTER 27 UFs                           │ TABELA OLS               │
│ x: investimento R$ bi                    │ β   0,0124   (0,0041)*** │
│ y: Δ emprego YoY %                       │ α   2,11     (0,42)***   │
│ raio: pop · cor: região (5)              │ R²  0,34   R²-aj 0,31    │
│ linha tracejada OLS + IC 95%             │ p   0,0028   F(1,25)     │
│ destaque ⇧+click p/ comparar             │ n   27       UFs         │
│ legenda regiões + linha tracejada        ├──────────────────────────┤
│ rodapé: equação ŷ = α + β·x + ε         │ ESPECIFICAÇÃO            │
│                                          │ Defasagem 4T             │
│                                          │ FE regional · IV Bartik  │
│                                          │ Pesos pop · 2020-24      │
│                                          │ + nota: R² modesto       │
└──────────────────────────────────────────┴──────────────────────────┘

┌─ TELA 05 · PIPELINE ────────────────────────────────────────────────┐
│ # Pipeline de dados — coletores e qualidade                         │
│ KPIs banner: Fontes 12/12 · Cob 93,1% · Reg 81,3mi · Lat 4h12 · 4 al│
├──────────────────────────────────────────┬──────────────────────────┤
│ TABELA fontes (12)                       │ ALERTAS                  │
│ ● fonte | cadência | cobertura | recs    │ ┃ ERRO  SUDAM/SUDENE     │
│ ● CAGED | Mensal   | ▓▓▓▓▓ 100%| 1.2M    │   parser quebrado · 29d  │
│ ● RAIS  | Anual    | ▓▓▓▓▓ 100%| 54.8M   │ ┃ AVISO ANEEL            │
│ ● PNAD-C| Trim.    | ▓▓▓▓▓ 100%| 847k    │   schema novo · 11d      │
│ ⚠ Investe SP · 24h sem dados             │ ┃ AVISO Investe SP       │
│ ✕ SUDAM · parser quebrado                │ ┃ INFO  PIM-PF revisão   │
│ + 8 outras                               │                          │
└──────────────────────────────────────────┴──────────────────────────┘
`;
  return (
    <div style={{ padding: 24, background: 'var(--bg-page)', height: '100%', overflow: 'auto' }}>
      <div className="mono" style={{ fontSize: 10, color: 'var(--ink-3)', textTransform: 'uppercase', letterSpacing: 0.8, fontWeight: 600 }}>00 · Layout</div>
      <h1 className="serif" style={{ margin: '4px 0 6px', fontSize: 28, fontWeight: 600, letterSpacing: -0.5 }}>Wireframes — 5 telas</h1>
      <p style={{ margin: '0 0 16px', fontSize: 13, color: 'var(--ink-3)', maxWidth: 720, lineHeight: 1.55 }}>
        Estrutura comum: header + filter bar persistente + área principal em grid. Header e filtros são compartilhados; a área principal varia.
        Densidade alta com hierarquia tipográfica clara (manchete em serifa, eyebrow em uppercase tracked).
      </p>
      <pre className="mono" style={{
        fontSize: 11, lineHeight: 1.45, color: 'var(--ink-1)',
        background: 'var(--bg-surface)',
        border: '1px solid var(--border-soft)',
        padding: 16,
        margin: 0,
        whiteSpace: 'pre',
        overflow: 'auto',
      }}>{ascii}</pre>
    </div>
  );
}

function SpecComponentes() {
  return (
    <div style={{ padding: 24, background: 'var(--bg-page)', height: '100%', overflow: 'auto' }}>
      <div className="mono" style={{ fontSize: 10, color: 'var(--ink-3)', textTransform: 'uppercase', letterSpacing: 0.8, fontWeight: 600 }}>00 · Componentes</div>
      <h1 className="serif" style={{ margin: '4px 0 6px', fontSize: 28, fontWeight: 600, letterSpacing: -0.5 }}>Padrões de componentes</h1>
      <p style={{ margin: '0 0 24px', fontSize: 13, color: 'var(--ink-3)', maxWidth: 620, lineHeight: 1.55 }}>
        Patterns para filtros, tooltips, leitura de tabelas grandes — reutilizados em todas as 5 telas.
      </p>

      <SpecBlock title="Filtros · barra de segmento">
        <div style={{ background: 'var(--bg-surface)', padding: 16, border: '1px solid var(--border-soft)' }}>
          <FilterBar values={{ periodo: '5A', geo: 'UF', ind: 'Anunciado', setor: 'Todos', rec: 'Bruto' }} setValues={() => {}} />
        </div>
        <ul style={{ fontSize: 12, color: 'var(--ink-2)', lineHeight: 1.6, paddingLeft: 18, marginTop: 12 }}>
          <li>Single-select por dimensão · 5 dimensões core (Período, Geo, Indicador, Setor, Recorte)</li>
          <li>Selecionado: fundo preto com texto invertido — alto contraste, fácil identificar estado atual</li>
          <li>Botão "+ adicionar filtro" abre seletor de dimensões secundárias (CNAE, porte, IED, fonte)</li>
          <li>Botão "Aplicar" só destaca quando há mudança pendente</li>
        </ul>
      </SpecBlock>

      <SpecBlock title="Tooltip de mapa">
        <div style={{ background: 'var(--bg-surface)', padding: 24, border: '1px solid var(--border-soft)', position: 'relative', height: 160 }}>
          <div style={{
            display: 'inline-block',
            background: 'var(--bg-surface)',
            border: '1px solid var(--border-strong)',
            boxShadow: 'var(--shadow-pop)',
            padding: '10px 12px',
            minWidth: 200,
          }}>
            <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', gap: 8, marginBottom: 4 }}>
              <span className="serif" style={{ fontSize: 14, fontWeight: 600 }}>São Paulo</span>
              <span className="mono" style={{ fontSize: 10, color: 'var(--ink-3)', textTransform: 'uppercase', letterSpacing: 0.6 }}>SP · SE</span>
            </div>
            <div className="mono" style={{ fontSize: 18, fontWeight: 600 }}>R$ 142,8 bi</div>
            <div style={{ fontSize: 11, color: 'var(--ink-3)', marginTop: 4 }}>investimento anunciado · 2024</div>
            <div style={{ fontSize: 11, color: 'var(--ink-3)', marginTop: 6, lineHeight: 1.5 }}>
              Clique para fixar · ⇧+clique para comparar
            </div>
          </div>
        </div>
        <ul style={{ fontSize: 12, color: 'var(--ink-2)', lineHeight: 1.6, paddingLeft: 18, marginTop: 12 }}>
          <li>Header: nome + UF/região em mono uppercase</li>
          <li>Valor primário: numeral grande em mono — comparação fácil entre UFs no hover sequencial</li>
          <li>Atalhos exibidos: ⇧+clique para comparar (multi-seleção sem modal)</li>
        </ul>
      </SpecBlock>

      <SpecBlock title="Tabela densa">
        <div style={{ background: 'var(--bg-surface)', border: '1px solid var(--border-soft)' }}>
          <DenseTable
            columns={[
              { key: 'rank', label: '#', mono: true, muted: true, render: (r) => String(r.rank).padStart(2,'0') },
              { key: 'm', label: 'Município' },
              { key: 'uf', label: 'UF', mono: true, muted: true },
              { key: 'v', label: 'R$ mi', align: 'right', mono: true, render: (r) => fmtNum(r.v) },
              { key: 'spark', label: '5A', render: () => <Sparkline data={[2,3,4,5,7,9,12,14,18,22]} width={48} height={16} /> },
            ]}
            rows={TOP_MUNI.slice(0, 5).map((r, i) => ({ ...r, rank: i + 1 }))}
          />
        </div>
        <ul style={{ fontSize: 12, color: 'var(--ink-2)', lineHeight: 1.6, paddingLeft: 18, marginTop: 12 }}>
          <li>Linhas de 28px — densidade ~50 linhas/tela em altura padrão</li>
          <li>Header sticky · ordenação clicando no rótulo · setas indicam direção</li>
          <li>Numerais em mono com tabular-nums forçado — colunas alinham mesmo em escalas mistas</li>
          <li>Sparkline inline na última coluna substitui necessidade de tooltip de série</li>
          <li>Footer mostra "5 de 5.570" + link "ver todos →" para drill</li>
        </ul>
      </SpecBlock>

      <SpecBlock title="KPI compacto">
        <div style={{ background: 'var(--bg-surface)', padding: 16, border: '1px solid var(--border-soft)', display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 24 }}>
          <KPI label="Total anunciado" value="R$ 745" unit="bi" delta={12.4} sparkData={[412,438,481,523,571,612,665,710,745]} />
          <KPI label="Sudeste · share" value="40,5" unit="%" delta={-1.8} sparkData={[44.2,43.8,43.1,42.7,42.0,41.6,41.2,40.8,40.5]} sparkColor="var(--cat-2)" />
          <KPI label="UFs cobertas" value="27" unit="/ 27" sub="cobertura completa" />
        </div>
      </SpecBlock>
    </div>
  );
}

function SpecBlock({ title, children }) {
  return (
    <div style={{ marginBottom: 32 }}>
      <h2 className="serif" style={{ fontSize: 17, fontWeight: 600, margin: '0 0 12px' }}>{title}</h2>
      {children}
    </div>
  );
}

function SpecJustificativa() {
  const Q = ({ q, a }) => (
    <div style={{ marginBottom: 22, paddingBottom: 22, borderBottom: '1px solid var(--border-soft)' }}>
      <div className="mono" style={{ fontSize: 10, color: 'var(--ink-3)', textTransform: 'uppercase', letterSpacing: 0.8, fontWeight: 600, marginBottom: 6 }}>decisão</div>
      <h3 className="serif" style={{ fontSize: 17, fontWeight: 600, margin: '0 0 8px', letterSpacing: -0.2 }}>{q}</h3>
      <p style={{ margin: 0, fontSize: 13, color: 'var(--ink-2)', lineHeight: 1.6 }}>{a}</p>
    </div>
  );
  return (
    <div style={{ padding: 24, background: 'var(--bg-page)', height: '100%', overflow: 'auto' }}>
      <div className="mono" style={{ fontSize: 10, color: 'var(--ink-3)', textTransform: 'uppercase', letterSpacing: 0.8, fontWeight: 600 }}>00 · Racional</div>
      <h1 className="serif" style={{ margin: '4px 0 24px', fontSize: 28, fontWeight: 600, letterSpacing: -0.5 }}>Justificativa de design</h1>

      <Q q="Por que mapas em tile-grid e não geográficos?"
         a="Para um overview comparativo de 27 UFs, a geografia real distorce a leitura — RR ocupa mais pixels que SP mas pesa muito menos no fenômeno. Tile-grid dá área igual a cada UF e força comparação por intensidade, não por tamanho. A forma macroregional (N/NE/CO/SE/S) é preservada o suficiente para o reconhecimento. Para drill municipal, plano é trocar por TopoJSON real com simplificação por escala." />

      <Q q="Por que serifa em manchetes em um produto técnico?"
         a="O brief pede 'acadêmico sério mas moderno' e 'não corporate dashboard genérico'. Serifa em manchetes (Source Serif 4) traz o registro de papers/jornalismo de dados (FT, OWiD) sem ficar nostálgico. UI e dados densos seguem em sans (Inter) — combinação clássica de editorial digital." />

      <Q q="Por que numerais sempre em mono?"
         a="Tabular-nums em todos os números (R$ 142.847.300, +12,4%, p=0,0028) garante alinhamento decimal vertical em tabelas e horizontal em KPIs sequenciais. Em pesquisa econômica isso é não-negociável: ler 5 R$/UF empilhados é trivial em mono e impossível em proporcional. Custo: estética ligeiramente mais 'técnica' — exatamente o tom desejado." />

      <Q q="Por que filter bar superior em vez de sidebar?"
         a="Mapas são protagonistas. Sidebar de 240-280px rouba ~15% da largura. Top bar perde uma faixa de 56px de altura — em telas largas (1440+), trade-off claramente favorável ao mapa. Filtros raramente passam de 5 dimensões; quando precisar de mais, painel '+ adicionar filtro' abre overlay sem ocupar espaço permanente." />

      <Q q="Por que paleta âmbar/terra em vez de azul?"
         a="Azul é o default de SaaS analytics — exatamente o que o brief pede para evitar. Âmbar/terra evoca cartografia histórica, papel envelhecido, sépia editorial. Mantém legibilidade quantitativa (ordem clara light→dark) sem importar o vocabulário visual de PowerBI/Tableau. Acentos em laranja queimado (#d97706) destacam o que importa." />

      <Q q="Por que dark mode é necessário (não opcional)?"
         a="Brief explicita 'sessões longas de análise'. Tela clara após 2-3h causa fadiga e altera percepção de cor (drift do cinza). Dark mode com fundo levemente brunido (#14110b, não preto puro) e tons sequenciais invertidos preserva a hierarquia da escala. Toggle persistente no header." />

      <Q q="Por que mostrar regressão e não só correlação?"
         a="Tela 4 é onde o trabalho deixa de ser descritivo e vira inferência. Mostrar β, erro-padrão, R², p-valor e n no mesmo painel deixa o leitor decidir confiança. Anotações editoriais (em serifa itálica) avisam contra over-interpretação — o produto ensina o leitor a usar os dados criticamente, em vez de só apresentá-los." />

      <Q q="O que NÃO foi feito (escopo desta entrega)">
         <span /></Q>
      <ul style={{ fontSize: 12.5, color: 'var(--ink-2)', lineHeight: 1.65, paddingLeft: 18 }}>
        <li>Mapas reais com TopoJSON (município/mesorregião) — usei tile-grid simbólico</li>
        <li>Interatividade real: brushing/linking entre painéis, drill UF→meso→município</li>
        <li>Exportação real (SVG/PNG/CSV); botões existem mas não atuam</li>
        <li>Versão móvel — densidade alta exige &gt; 1280px</li>
        <li>Filtros de Bartik shock e IV são placeholders; integração econométrica real fica para spec separado</li>
      </ul>
    </div>
  );
}

Object.assign(window, { SpecPaleta, SpecTipografia, SpecWireframes, SpecComponentes, SpecJustificativa });
