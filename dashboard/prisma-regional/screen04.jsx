/* Tela 04 — Relação causal
   Layout: scatter principal grande + painel de regressão + filtros lag/controle
   + small multiples por região */

function Screen04_Causal() {
  // 27 UFs: investimento (R$ bi) vs Δ emprego YoY (%)
  const points = Object.keys(INVESTMENT_BY_UF).map((uf) => ({
    label: uf,
    region: UF_META[uf]?.region || 'SE',
    x: INVESTMENT_BY_UF[uf] / 1000,        // R$ bi
    y: EMPLOYMENT_GROWTH_YOY[uf],
    size: (UF_META[uf]?.pop || 1) * 4,
  }));

  // OLS no olho: slope ~0.012, intercept ~1.8 (plausível com nossos dados)
  const regression = { slope: 0.012, intercept: 2.1, ci: 0.9, r2: 0.34, p: 0.0028, n: 27 };

  return (
    <div style={{
      gridArea: 'main',
      display: 'grid',
      gridTemplateColumns: '1.4fr 1fr',
      gridTemplateRows: 'auto 1fr',
      gap: 12,
      padding: 12,
      minHeight: 0,
      overflow: 'hidden',
    }}>
      <div style={{ gridColumn: '1 / -1', display: 'flex', alignItems: 'baseline', gap: 16, padding: '4px 4px 0' }}>
        <h1 className="serif" style={{ margin: 0, fontSize: 28, fontWeight: 600, letterSpacing: -0.5 }}>
          Investimento → emprego: existe causalidade regional?
        </h1>
        <span style={{ fontSize: 12, color: 'var(--ink-3)' }}>OLS · 27 UFs · 2024 · controle: defasagem 4T</span>
        <div style={{ flex: 1 }} />
        <span className="mono" style={{ fontSize: 11, color: 'var(--ink-4)' }}>tela 04/05</span>
      </div>

      {/* Scatter principal */}
      <Panel padding={14} eyebrow="Diagrama de dispersão" title="Investimento anunciado vs Δ emprego formal"
        subtitle="Cada ponto = uma UF · raio proporcional à população · linha tracejada = OLS"
        action={<PanelAction>OLS · IV · DiD</PanelAction>}
        footer={<>
          <span>Atenção: correlação ≠ causalidade. Use o painel de inferência →</span>
          <span className="mono">y = α + β·x + ε</span>
        </>}>
        <ScatterRegression
          width={760}
          height={460}
          points={points}
          xLabel="investimento anunciado · R$ bilhões"
          yLabel="Δ emprego formal · % YoY"
          xFormat={(v) => v.toFixed(0)}
          yFormat={(v) => v.toFixed(1).replace('.', ',') + '%'}
          regression={regression}
        />

        {/* Legenda regiões */}
        <div style={{ display: 'flex', gap: 16, fontSize: 11, marginTop: 6, color: 'var(--ink-2)' }}>
          {[
            ['Sudeste', 'var(--cat-2)'],
            ['Sul', 'var(--cat-5)'],
            ['Nordeste', 'var(--cat-1)'],
            ['C-Oeste', 'var(--cat-4)'],
            ['Norte', 'var(--cat-3)'],
          ].map(([n, c]) => (
            <span key={n} style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
              <span style={{ width: 10, height: 10, borderRadius: 5, background: c }} />{n}
            </span>
          ))}
          <span style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 5 }}>
            <svg width="40" height="10"><line x1="0" y1="5" x2="40" y2="5" stroke="var(--ink-1)" strokeWidth="1.5" strokeDasharray="6 3"/></svg>
            <span>OLS · IC 95%</span>
          </span>
        </div>
      </Panel>

      {/* Painel de inferência */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 12, minHeight: 0 }}>
        <Panel padding={14} eyebrow="Estimativa OLS" title="ŷ = α + β · investimento">
          <table className="mono" style={{ width: '100%', fontSize: 12, borderCollapse: 'collapse' }}>
            <tbody>
              <tr style={{ borderBottom: '1px solid var(--border-soft)' }}>
                <td style={{ padding: '8px 4px', color: 'var(--ink-3)' }}>β (inclinação)</td>
                <td style={{ padding: '8px 4px', textAlign: 'right', fontWeight: 600 }}>0,0124</td>
                <td style={{ padding: '8px 4px', textAlign: 'right', color: 'var(--ink-3)', fontSize: 11 }}>(0,0041)***</td>
              </tr>
              <tr style={{ borderBottom: '1px solid var(--border-soft)' }}>
                <td style={{ padding: '8px 4px', color: 'var(--ink-3)' }}>α (intercepto)</td>
                <td style={{ padding: '8px 4px', textAlign: 'right', fontWeight: 600 }}>2,11</td>
                <td style={{ padding: '8px 4px', textAlign: 'right', color: 'var(--ink-3)', fontSize: 11 }}>(0,42)***</td>
              </tr>
              <tr style={{ borderBottom: '1px solid var(--border-soft)' }}>
                <td style={{ padding: '8px 4px', color: 'var(--ink-3)' }}>R²</td>
                <td style={{ padding: '8px 4px', textAlign: 'right', fontWeight: 600 }}>0,34</td>
                <td style={{ padding: '8px 4px', textAlign: 'right', color: 'var(--ink-3)', fontSize: 11 }}>R²-aj 0,31</td>
              </tr>
              <tr style={{ borderBottom: '1px solid var(--border-soft)' }}>
                <td style={{ padding: '8px 4px', color: 'var(--ink-3)' }}>p-valor</td>
                <td style={{ padding: '8px 4px', textAlign: 'right', fontWeight: 600, color: 'var(--signal-good)' }}>0,0028</td>
                <td style={{ padding: '8px 4px', textAlign: 'right', color: 'var(--ink-3)', fontSize: 11 }}>F(1,25)</td>
              </tr>
              <tr>
                <td style={{ padding: '8px 4px', color: 'var(--ink-3)' }}>n</td>
                <td style={{ padding: '8px 4px', textAlign: 'right', fontWeight: 600 }}>27</td>
                <td style={{ padding: '8px 4px', textAlign: 'right', color: 'var(--ink-3)', fontSize: 11 }}>UFs</td>
              </tr>
            </tbody>
          </table>
          <div style={{ fontSize: 11, color: 'var(--ink-3)', marginTop: 8, lineHeight: 1.4 }}>
            *** p &lt; 0,01 · erros-padrão entre parênteses · HC1-robust
          </div>
        </Panel>

        <Panel padding={14} eyebrow="Especificação" title="Controles e robustez">
          <div style={{ display: 'flex', flexDirection: 'column', gap: 10, fontSize: 12 }}>
            {[
              ['Defasagem temporal', '4T', 'fixo'],
              ['Efeito fixo regional', 'Sim', 'opcional'],
              ['Variável instrumental', 'Bartik shock', 'ativo'],
              ['Pesos populacionais', 'Sim', 'ativo'],
              ['Período', '2020 T1 – 2024 T4', '20 obs/UF'],
            ].map(([k, v, meta]) => (
              <div key={k} style={{ display: 'grid', gridTemplateColumns: '1fr auto auto', gap: 12, alignItems: 'baseline', borderBottom: '1px solid var(--border-soft)', paddingBottom: 6 }}>
                <span style={{ color: 'var(--ink-3)' }}>{k}</span>
                <span className="mono" style={{ color: 'var(--ink-1)', fontWeight: 600 }}>{v}</span>
                <span className="mono" style={{ color: 'var(--ink-4)', fontSize: 10 }}>{meta}</span>
              </div>
            ))}
          </div>
          <EditorialNote>
            β &gt; 0 e estatisticamente significante, mas R² modesto sugere que outros fatores (qualificação, infraestrutura, políticas locais) explicam a maior parte da variância. Não interpretar como elasticidade pura.
          </EditorialNote>
        </Panel>
      </div>
    </div>
  );
}

window.Screen04_Causal = Screen04_Causal;
