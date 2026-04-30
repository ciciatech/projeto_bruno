/* Tela 03 — Setores
   Layout: stacked bar composição × região + treemap-like grid setor × UF
   + ranking de crescimento setorial. */

function Screen03_Setores() {
  return (
    <div style={{
      gridArea: 'main',
      display: 'grid',
      gridTemplateColumns: '1.4fr 1fr',
      gridTemplateRows: 'auto 1fr 1fr',
      gap: 12,
      padding: 12,
      minHeight: 0,
      overflow: 'hidden',
    }}>
      <div style={{ gridColumn: '1 / -1', display: 'flex', alignItems: 'baseline', gap: 16, padding: '4px 4px 0' }}>
        <h1 className="serif" style={{ margin: 0, fontSize: 28, fontWeight: 600, letterSpacing: -0.5 }}>
          Composição setorial das economias regionais
        </h1>
        <span style={{ fontSize: 12, color: 'var(--ink-3)' }}>% do VAB regional · 2024 · IBGE Contas Regionais</span>
        <div style={{ flex: 1 }} />
        <span className="mono" style={{ fontSize: 11, color: 'var(--ink-4)' }}>tela 03/05</span>
      </div>

      {/* Stacked bars */}
      <Panel padding={14} eyebrow="Composição" title="Participação setorial · 5 regiões"
        subtitle="Cada barra soma 100% do VAB regional"
        action={<PanelAction>% · R$ · diff</PanelAction>}>
        <StackedBars
          width={760}
          height={300}
          data={SECTOR_BY_REGION}
          keys={['industria','servicos','agro','construcao','comercio','admpublica']}
          colors={['var(--cat-1)','var(--cat-2)','var(--cat-3)','var(--cat-4)','var(--cat-5)','var(--cat-6)']}
        />
        <div style={{ display: 'flex', gap: 14, flexWrap: 'wrap', marginTop: 10, fontSize: 11 }}>
          {SECTORS.map((s) => (
            <span key={s.key} style={{ display: 'flex', alignItems: 'center', gap: 5, color: 'var(--ink-2)' }}>
              <span style={{ width: 10, height: 10, background: s.color }} />{s.name}
            </span>
          ))}
        </div>
      </Panel>

      {/* Ranking crescimento setorial */}
      <Panel padding={14} eyebrow="Crescimento real" title="VAB setorial · YoY 2024"
        style={{ gridRow: 'span 2' }}>
        <HBar
          data={[
            { label: 'Agro · MT',          value:  18.4, color: 'var(--cat-3)' },
            { label: 'Indústria · SC',     value:  12.7, color: 'var(--cat-1)' },
            { label: 'Serviços · DF',      value:   8.1, color: 'var(--cat-2)' },
            { label: 'Construção · GO',    value:   7.4, color: 'var(--cat-4)' },
            { label: 'Comércio · CE',      value:   6.2, color: 'var(--cat-5)' },
            { label: 'Indústria · SP',     value:   3.8, color: 'var(--cat-1)' },
            { label: 'Serviços · RJ',      value:   2.1, color: 'var(--cat-2)' },
            { label: 'Agro · RS',          value:  -2.4, color: 'var(--div-neg-2)' },
            { label: 'Indústria · AM',     value:  -3.6, color: 'var(--div-neg-2)' },
            { label: 'Construção · RR',    value:  -8.1, color: 'var(--div-neg-2)' },
          ]}
          width={340}
          height={420}
          maxBars={10}
          valueFormat={(v) => fmtPct(v, 1)}
        />
        <EditorialNote>
          Heterogeneidade setorial dentro de cada região é maior que a heterogeneidade entre regiões — argumento clássico de Furtado revisitado.
        </EditorialNote>
      </Panel>

      {/* Heatmap setor × UF */}
      <Panel padding={14} eyebrow="Calor setor × UF" title="Especialização (LQ — quociente locacional)"
        subtitle="Valores > 1,0 indicam especialização relativa">
        <SectorUFHeatmap />
      </Panel>
    </div>
  );
}

function SectorUFHeatmap() {
  // Dados sintéticos: LQ por UF × setor
  const ufs = ['SP','RJ','MG','RS','PR','SC','BA','GO','PE','CE','MT','MS','PA','AM','MA'];
  const sectors = ['Indústria','Serviços','Agro','Constr.','Comércio','Adm. Pub.'];
  // valores plausíveis
  const lq = {
    SP: [1.4, 1.3, 0.2, 0.9, 1.1, 0.6],
    RJ: [0.9, 1.5, 0.1, 0.8, 0.9, 1.4],
    MG: [1.2, 0.9, 1.1, 1.0, 1.0, 0.7],
    RS: [1.4, 0.9, 1.4, 0.7, 1.0, 0.5],
    PR: [1.3, 0.9, 1.5, 0.7, 1.0, 0.4],
    SC: [1.6, 0.9, 1.2, 0.6, 0.9, 0.4],
    BA: [1.1, 1.0, 1.0, 0.9, 0.9, 1.1],
    GO: [0.8, 0.8, 2.4, 1.1, 0.8, 1.2],
    PE: [0.8, 1.1, 0.7, 1.0, 1.1, 1.2],
    CE: [0.7, 1.2, 0.7, 1.2, 1.1, 1.1],
    MT: [0.6, 0.7, 3.2, 1.0, 0.8, 0.9],
    MS: [0.7, 0.7, 2.8, 1.0, 0.9, 1.0],
    PA: [1.4, 0.8, 1.0, 1.1, 0.9, 1.0],
    AM: [1.7, 0.8, 0.7, 0.8, 0.9, 1.1],
    MA: [0.6, 0.9, 1.4, 1.4, 0.9, 1.5],
  };

  const colorOf = (v) => {
    if (v < 0.7) return 'var(--div-neg-2)';
    if (v < 0.9) return 'var(--div-neg-1)';
    if (v < 1.1) return 'var(--div-zero)';
    if (v < 1.4) return 'var(--div-pos-1)';
    if (v < 2.0) return 'var(--div-pos-2)';
    return 'var(--div-pos-3)';
  };

  return (
    <div style={{ overflow: 'auto', maxHeight: '100%' }}>
      <table style={{ borderCollapse: 'collapse', fontFamily: 'var(--font-mono)', fontSize: 11 }}>
        <thead>
          <tr>
            <th style={{ padding: '4px 8px', borderBottom: '1px solid var(--rule)' }}></th>
            {sectors.map((s) => (
              <th key={s} style={{ padding: '4px 8px', borderBottom: '1px solid var(--rule)', fontSize: 10, color: 'var(--ink-3)', textTransform: 'uppercase', letterSpacing: 0.6, fontWeight: 600, fontFamily: 'var(--font-sans)', textAlign: 'center', minWidth: 64 }}>{s}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {ufs.map((uf) => (
            <tr key={uf}>
              <td style={{ padding: '0 8px', fontWeight: 600, color: 'var(--ink-2)', borderBottom: '1px solid var(--border-soft)' }}>{uf}</td>
              {lq[uf].map((v, i) => (
                <td key={i} style={{ padding: 0, borderBottom: '1px solid var(--border-soft)' }}>
                  <div style={{ background: colorOf(v), height: 22, display: 'flex', alignItems: 'center', justifyContent: 'center', color: v > 1.5 ? 'var(--bg-page)' : 'var(--ink-1)', fontVariantNumeric: 'tabular-nums', fontWeight: v > 1.5 ? 600 : 500 }}>
                    {v.toFixed(1).replace('.', ',')}
                  </div>
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

window.Screen03_Setores = Screen03_Setores;
