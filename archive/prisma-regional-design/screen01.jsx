/* Tela 01 — Investimento (HERO)
   Layout: mapa protagonista + KPIs verticais + tabela top municípios + composição. */

function Screen01_Investimento({ filters, setFilters }) {
  const [hover, setHover] = React.useState(null);
  const [selected, setSelected] = React.useState('SP');

  // KPIs
  const total2024 = Object.values(INVESTMENT_BY_UF).reduce((a,b) => a+b, 0);
  const ufCount = Object.keys(INVESTMENT_BY_UF).length;

  // top concentração: 5 maiores UFs
  const topUFs = Object.entries(INVESTMENT_BY_UF)
    .sort((a,b) => b[1]-a[1])
    .slice(0, 6)
    .map(([uf, v]) => ({ label: uf, value: v }));

  return (
    <div style={{
      gridArea: 'main',
      display: 'grid',
      gridTemplateColumns: '320px 1fr 360px',
      gridTemplateRows: 'auto 1fr',
      gap: 12,
      padding: 12,
      minHeight: 0,
      overflow: 'hidden',
    }}>
      {/* SECTION HEADER (full row) */}
      <div style={{ gridColumn: '1 / -1', display: 'flex', alignItems: 'baseline', gap: 16, padding: '4px 4px 0' }}>
        <h1 className="serif" style={{ margin: 0, fontSize: 28, fontWeight: 600, letterSpacing: -0.5 }}>
          Investimento privado anunciado por unidade federativa
        </h1>
        <span style={{ fontSize: 12, color: 'var(--ink-3)' }}>Brasil · jan 2024 – dez 2024 · {fmtBRL(total2024 * 1e6)} agregado</span>
        <div style={{ flex: 1 }} />
        <span className="mono" style={{ fontSize: 11, color: 'var(--ink-4)' }}>tela 01/05</span>
      </div>

      {/* COLUNA ESQUERDA — KPIs e leitura editorial */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 12, minHeight: 0 }}>
        <Panel padding={14} eyebrow="Síntese" title="Concentração persistente">
          <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
            <KPI label="Total anunciado" value="R$ 745" unit="bi" delta={12.4}
                 sparkData={[412,438,481,523,571,612,665,710,745]} />
            <KPI label="UFs cobertas" value="27" unit="/ 27"
                 sub="cobertura completa · 5.4k municípios" />
            <KPI label="Sudeste · share" value="40,5" unit="%" delta={-1.8}
                 sparkData={[44.2,43.8,43.1,42.7,42.0,41.6,41.2,40.8,40.5]}
                 sparkColor="var(--cat-2)" />
            <KPI label="Nordeste · share" value="14,5" unit="%" delta={2.1}
                 sparkData={[11.0,11.6,12.2,12.8,13.4,13.7,14.0,14.3,14.5]}
                 sparkColor="var(--cat-1)" />
          </div>
        </Panel>

        <Panel padding={14} eyebrow="Top concentração" title="6 UFs > 60% do total">
          <HBar data={topUFs} width={290} height={170} valueFormat={(v) => fmtBRL(v*1e6)} color="var(--seq-3)" maxBars={6} />
          <EditorialNote>
            SP sozinho responde por 19,2% do investimento anunciado em 2024 — concentração típica do período pós-2017, mas em queda lenta.
          </EditorialNote>
        </Panel>
      </div>

      {/* MAPA PROTAGONISTA */}
      <Panel padding={16} eyebrow="Mapa coroplético" title="R$ por UF · 2024" subtitle="Clique em uma UF para fixar como ponto de referência. Hover para detalhe."
        action={<PanelAction>SVG · PNG · CSV</PanelAction>}
        footer={<>
          <span>Escala sequencial · 5 quintis · valores brutos em R$ milhões</span>
          <span className="mono">fonte: BNDES + Investe SP + Comex · n=5.570 munic.</span>
        </>}
      >
        <div style={{ display: 'flex', gap: 24, alignItems: 'flex-start', height: '100%' }}>
          <div style={{ flex: 1, minWidth: 0, display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
            <ChoroplethBR
              values={INVESTMENT_BY_UF}
              scale="seq"
              width={520}
              height={620}
              tile={62}
              gap={3}
              onHover={setHover}
              selected={selected}
            />
          </div>

          <aside style={{ width: 180, display: 'flex', flexDirection: 'column', gap: 18, paddingTop: 8 }}>
            <MapLegend scale="seq" min={1400} max={142800} label="R$ milhões" format={fmtCompact} />

            <div>
              <div style={{ fontSize: 10, color: 'var(--ink-3)', textTransform: 'uppercase', letterSpacing: 0.8, fontWeight: 600, marginBottom: 6 }}>Selecionado</div>
              <div className="serif" style={{ fontSize: 18, fontWeight: 600, color: 'var(--ink-1)', letterSpacing: -0.3 }}>São Paulo</div>
              <div className="mono" style={{ fontSize: 22, fontWeight: 600, color: 'var(--ink-1)', marginTop: 2 }}>R$ 142,8 bi</div>
              <div className="mono" style={{ fontSize: 11, color: 'var(--signal-good)', fontWeight: 600 }}>▲ +14,2% vs 2023</div>
              <div style={{ fontSize: 11, color: 'var(--ink-3)', marginTop: 6, lineHeight: 1.5 }}>
                19,2% do total nacional · ranking #1 desde 1995. 645 municípios cobertos.
              </div>
            </div>

            <div>
              <div style={{ fontSize: 10, color: 'var(--ink-3)', textTransform: 'uppercase', letterSpacing: 0.8, fontWeight: 600, marginBottom: 6 }}>Por região</div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 4, fontSize: 11 }}>
                {[
                  ['Sudeste',     302100, 'var(--cat-2)'],
                  ['Sul',         112200, 'var(--cat-5)'],
                  ['Nordeste',    108200, 'var(--cat-1)'],
                  ['Centro-Oeste', 73300, 'var(--cat-4)'],
                  ['Norte',        47800, 'var(--cat-3)'],
                ].map(([n, v, c]) => (
                  <div key={n} style={{ display: 'grid', gridTemplateColumns: '8px 1fr auto', alignItems: 'center', gap: 8 }}>
                    <span style={{ width: 8, height: 8, background: c }} />
                    <span style={{ color: 'var(--ink-2)' }}>{n}</span>
                    <span className="mono" style={{ color: 'var(--ink-1)', fontWeight: 600 }}>{fmtBRL(v*1e6)}</span>
                  </div>
                ))}
              </div>
            </div>

            <div style={{ borderTop: '1px solid var(--border-soft)', paddingTop: 10 }}>
              <div className="mono" style={{ fontSize: 10, color: 'var(--ink-4)', lineHeight: 1.5 }}>
                Última atualização<br/>
                <span style={{ color: 'var(--ink-2)', fontSize: 11 }}>2026-04-28 · 03:14 BRT</span>
              </div>
            </div>
          </aside>
        </div>

        {hover && (
          <Tooltip data={{
            ...hover,
            children: (
              <>
                <div className="mono" style={{ fontSize: 16, fontWeight: 600, color: 'var(--ink-1)', marginTop: 2 }}>
                  {fmtBRL((hover.value || 0) * 1e6)}
                </div>
                <div style={{ fontSize: 11, color: 'var(--ink-3)', marginTop: 6, lineHeight: 1.5 }}>
                  Investimento anunciado · 2024<br/>
                  Clique para fixar · ⇧ + clique para comparar
                </div>
              </>
            )
          }} />
        )}
      </Panel>

      {/* COLUNA DIREITA — top municípios + composição */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 12, minHeight: 0, overflow: 'hidden' }}>
        <Panel padding={0} eyebrow="Drill-down" title="Top 10 municípios"
          action={<PanelAction>↕</PanelAction>}>
          <DenseTable
            columns={[
              { key: 'rank',  label: '#',   align: 'right', mono: true, muted: true,
                render: (r) => String(r.rank).padStart(2,'0') },
              { key: 'm',     label: 'Município' },
              { key: 'uf',    label: 'UF', mono: true, muted: true },
              { key: 'v',     label: 'R$ mi', align: 'right', mono: true,
                render: (r) => fmtNum(r.v) },
              { key: 'spark', label: '5A',
                render: () => <Sparkline data={[2,3,4,5,7,9,12,14,18,22]} width={48} height={16} /> },
            ]}
            rows={TOP_MUNI.map((r, i) => ({ ...r, rank: i+1 }))}
            footer={<><span>10 de 5.570</span><span className="mono">ver todos →</span></>}
          />
        </Panel>

        <Panel padding={14} eyebrow="Decomposição" title="Por modalidade · 2024">
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {[
              ['Greenfield',           312, 'var(--seq-3)'],
              ['Expansão',             198, 'var(--seq-4)'],
              ['M&A doméstico',        134, 'var(--cat-2)'],
              ['IED — controle',        78, 'var(--cat-1)'],
              ['Concessão pública',     23, 'var(--cat-4)'],
            ].map(([n, v, c]) => (
              <div key={n} style={{ display: 'grid', gridTemplateColumns: '110px 1fr 64px', alignItems: 'center', gap: 8, fontSize: 12 }}>
                <span style={{ color: 'var(--ink-2)' }}>{n}</span>
                <div style={{ background: 'var(--bg-sunken)', height: 14, position: 'relative' }}>
                  <div style={{ background: c, height: '100%', width: `${(v/312)*100}%` }} />
                </div>
                <span className="mono" style={{ textAlign: 'right', color: 'var(--ink-1)', fontWeight: 600 }}>{fmtBRL(v*1e9)}</span>
              </div>
            ))}
          </div>
        </Panel>
      </div>
    </div>
  );
}

window.Screen01_Investimento = Screen01_Investimento;
