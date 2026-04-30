/* Tela 02 — Dinâmica do emprego
   Layout: mapa de variação YoY (divergente) + linhas regionais sobrepostas
   + decomposição setorial (small multiples) + tabela com sparklines */

function Screen02_Emprego({ filters, setFilters }) {
  const [hover, setHover] = React.useState(null);

  return (
    <div style={{
      gridArea: 'main',
      display: 'grid',
      gridTemplateColumns: '1.1fr 1fr',
      gridTemplateRows: 'auto 1fr 1fr',
      gap: 12,
      padding: 12,
      minHeight: 0,
      overflow: 'hidden',
    }}>
      <div style={{ gridColumn: '1 / -1', display: 'flex', alignItems: 'baseline', gap: 16, padding: '4px 4px 0' }}>
        <h1 className="serif" style={{ margin: 0, fontSize: 28, fontWeight: 600, letterSpacing: -0.5 }}>
          Dinâmica do emprego formal
        </h1>
        <span style={{ fontSize: 12, color: 'var(--ink-3)' }}>CAGED + RAIS + PNAD-C · 2020 T1 → 2025 T2</span>
        <div style={{ flex: 1 }} />
        <span className="mono" style={{ fontSize: 11, color: 'var(--ink-4)' }}>tela 02/05</span>
      </div>

      {/* Mapa variação YoY (divergente) */}
      <Panel padding={14} eyebrow="Variação YoY · 2024" title="Crescimento do estoque de emprego formal"
             subtitle="Escala divergente · zero = sem variação"
             style={{ gridRow: 'span 2' }}
             footer={<>
               <span>Norte (RR, AP, AC) único bloco com retração</span>
               <span className="mono">CAGED · estoque líquido</span>
             </>}>
        <div style={{ display: 'flex', gap: 16, alignItems: 'flex-start', height: '100%' }}>
          <div style={{ flex: 1, minWidth: 0, display: 'flex', justifyContent: 'center' }}>
            <ChoroplethBR
              values={EMPLOYMENT_GROWTH_YOY}
              scale="div"
              width={460}
              height={540}
              tile={56}
              gap={3}
              onHover={setHover}
            />
          </div>
          <aside style={{ width: 160, display: 'flex', flexDirection: 'column', gap: 18, paddingTop: 8 }}>
            <MapLegend scale="div" label="Variação YoY" />

            <div>
              <div style={{ fontSize: 10, color: 'var(--ink-3)', textTransform: 'uppercase', letterSpacing: 0.8, fontWeight: 600, marginBottom: 6 }}>Top crescimento</div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 5, fontSize: 12 }}>
                {[['MA',7.2],['PI',6.8],['MT',6.4],['TO',5.4],['MS',5.1]].map(([uf,v]) => (
                  <div key={uf} style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span className="mono" style={{ color: 'var(--ink-2)' }}>{uf}</span>
                    <span className="mono" style={{ color: 'var(--signal-good)', fontWeight: 600 }}>{fmtPct(v)}</span>
                  </div>
                ))}
              </div>
            </div>
            <div>
              <div style={{ fontSize: 10, color: 'var(--ink-3)', textTransform: 'uppercase', letterSpacing: 0.8, fontWeight: 600, marginBottom: 6 }}>Retração</div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 5, fontSize: 12 }}>
                {[['RR',-1.2],['AP',-0.8],['AC',0.4],['DF',0.9]].map(([uf,v]) => (
                  <div key={uf} style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span className="mono" style={{ color: 'var(--ink-2)' }}>{uf}</span>
                    <span className="mono" style={{ color: v < 0 ? 'var(--signal-bad)' : 'var(--ink-3)', fontWeight: 600 }}>{fmtPct(v)}</span>
                  </div>
                ))}
              </div>
            </div>
            <EditorialNote>
              Matopiba — eixo BA/MA/PI/TO — concentra 4 dos 5 maiores ganhos relativos, espelhando a fronteira agrícola.
            </EditorialNote>
          </aside>
        </div>
        {hover && <Tooltip data={{ ...hover, children: (
          <>
            <div className="mono" style={{ fontSize: 18, fontWeight: 600, color: hover.value >= 0 ? 'var(--signal-good)' : 'var(--signal-bad)' }}>
              {fmtPct(hover.value || 0)}
            </div>
            <div style={{ fontSize: 11, color: 'var(--ink-3)', marginTop: 4 }}>variação YoY · estoque CAGED</div>
          </>
        ) }} />}
      </Panel>

      {/* Séries regionais empilhadas */}
      <Panel padding={14} eyebrow="Séries trimestrais" title="Estoque de emprego por região"
        subtitle="Milhões de vínculos formais · escala absoluta"
        action={<PanelAction>R$ · % · log</PanelAction>}>
        <div style={{ width: '100%', overflow: 'hidden' }}>
        <LineChart
          width={620}
          height={260}
          xTicks={[{ x: 0, label: '1T20' }, { x: 4, label: '1T21' }, { x: 8, label: '1T22' }, { x: 12, label: '1T23' }, { x: 16, label: '1T24' }, { x: 20, label: '1T25' }]}
          yLabel="milhões de vínculos"
          yFormat={(v) => v.toFixed(1)}
          series={[
            { name: 'Sudeste',      color: 'var(--cat-2)', points: EMPLOYMENT_TIMESERIES.SE.map((y,i) => ({ x: i, y })) },
            { name: 'Nordeste',     color: 'var(--cat-1)', points: EMPLOYMENT_TIMESERIES.NE.map((y,i) => ({ x: i, y })) },
            { name: 'Sul',          color: 'var(--cat-5)', points: EMPLOYMENT_TIMESERIES.S.map ((y,i) => ({ x: i, y })) },
            { name: 'C-Oeste',      color: 'var(--cat-4)', points: EMPLOYMENT_TIMESERIES.CO.map((y,i) => ({ x: i, y })) },
            { name: 'Norte',        color: 'var(--cat-3)', points: EMPLOYMENT_TIMESERIES.N.map ((y,i) => ({ x: i, y })) },
          ]}
        />
        </div>
      </Panel>

      {/* Decomposição: tabela densa com sparkline */}
      <Panel padding={0} eyebrow="Quadro completo" title="Indicadores por UF"
        action={<PanelAction>↧ CSV</PanelAction>}>
        <DenseTable
          columns={[
            { key: 'uf',    label: 'UF', mono: true },
            { key: 'reg',   label: 'Reg.', mono: true, muted: true },
            { key: 'estoque', label: 'Estoque', align: 'right', mono: true,
              render: (r) => fmtNum(r.estoque) + ' k' },
            { key: 'yoy',   label: 'YoY', align: 'right', mono: true,
              render: (r) => <span style={{ color: r.yoy >= 0 ? 'var(--signal-good)' : 'var(--signal-bad)', fontWeight: 600 }}>{fmtPct(r.yoy)}</span> },
            { key: 'desemprego', label: 'Desemp.', align: 'right', mono: true,
              render: (r) => r.desemprego.toFixed(1).replace('.', ',') + '%' },
            { key: 'spark', label: '8T', align: 'left',
              render: (r) => <Sparkline data={r.spark} width={56} height={16} color={r.yoy >= 0 ? 'var(--signal-good)' : 'var(--signal-bad)'} /> },
          ]}
          rows={Object.entries(EMPLOYMENT_GROWTH_YOY).slice(0, 14).map(([uf, yoy]) => ({
            uf, reg: UF_META[uf]?.region, yoy,
            estoque: Math.round((UF_META[uf]?.pop || 1) * 380),
            desemprego: UNEMPLOYMENT[uf],
            spark: Array.from({ length: 8 }, (_, i) => 100 + i * (yoy/2) + (Math.sin(i*1.3+uf.charCodeAt(0)) * 2)),
          }))}
          footer={<><span>Mostrando 14 de 27 UFs</span><span className="mono">↓ rolar para mais</span></>}
        />
      </Panel>
    </div>
  );
}

window.Screen02_Emprego = Screen02_Emprego;
