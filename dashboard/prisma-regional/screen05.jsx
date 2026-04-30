/* Tela 05 — Pipeline de dados
   Layout: tabela de fontes (status, cobertura, última atualização)
   + alertas + timeline de coletas + indicadores de qualidade. */

function Screen05_Pipeline() {
  return (
    <div style={{
      gridArea: 'main',
      display: 'grid',
      gridTemplateColumns: '1.4fr 1fr',
      gridTemplateRows: 'auto auto 1fr',
      gap: 12,
      padding: 12,
      minHeight: 0,
      overflow: 'hidden',
    }}>
      <div style={{ gridColumn: '1 / -1', display: 'flex', alignItems: 'baseline', gap: 16, padding: '4px 4px 0' }}>
        <h1 className="serif" style={{ margin: 0, fontSize: 28, fontWeight: 600, letterSpacing: -0.5 }}>
          Pipeline de dados — coletores e qualidade
        </h1>
        <span style={{ fontSize: 12, color: 'var(--ink-3)' }}>12 fontes ativas · 81,3 milhões de registros · monitoramento contínuo</span>
        <div style={{ flex: 1 }} />
        <span className="mono" style={{ fontSize: 11, color: 'var(--ink-4)' }}>tela 05/05</span>
      </div>

      {/* Banner KPIs */}
      <div style={{ gridColumn: '1 / -1', display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: 12 }}>
        {[
          { label: 'Fontes ativas',  value: '12', sub: '/ 12 esperadas',  delta: null,   tone: 'good' },
          { label: 'Cobertura média', value: '93,1', unit: '%',          delta: -0.4,    tone: 'warn' },
          { label: 'Registros (total)', value: '81,3', unit: 'mi',       delta: 4.2,     tone: 'good' },
          { label: 'Latência média',  value: '4h12', unit: '',           delta: -8.0,    tone: 'good' },
          { label: 'Alertas abertos', value: '4',                        delta: null,    tone: 'warn' },
        ].map((k, i) => (
          <Panel key={i} padding={14}>
            <KPI label={k.label} value={k.value} unit={k.unit} delta={k.delta}
                 sub={k.sub} />
          </Panel>
        ))}
      </div>

      {/* Tabela fontes */}
      <Panel padding={0} eyebrow="Fontes de dados" title="Status por coletor"
        action={<PanelAction>↧ JSON · YAML</PanelAction>}>
        <DenseTable
          columns={[
            { key: 'status', label: '', align: 'center',
              render: (r) => {
                const dot = { ok: 'var(--signal-good)', warn: 'var(--signal-warn)', bad: 'var(--signal-bad)' }[r.status];
                return <span style={{ display: 'inline-block', width: 8, height: 8, borderRadius: 4, background: dot }} />;
              }
            },
            { key: 'fonte', label: 'Fonte' },
            { key: 'cadencia', label: 'Cadência', mono: true, muted: true },
            { key: 'cobertura', label: 'Cobertura', align: 'right', mono: true,
              render: (r) => (
                <div style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
                  <div style={{ width: 60, height: 6, background: 'var(--bg-sunken)' }}>
                    <div style={{
                      width: `${r.cobertura}%`, height: '100%',
                      background: r.cobertura > 90 ? 'var(--signal-good)' : r.cobertura > 70 ? 'var(--signal-warn)' : 'var(--signal-bad)',
                    }} />
                  </div>
                  <span style={{ minWidth: 32, textAlign: 'right' }}>{r.cobertura}%</span>
                </div>
              )
            },
            { key: 'records', label: 'Registros', align: 'right', mono: true },
            { key: 'lastUpdate', label: 'Última atualização', mono: true, muted: true },
            { key: 'delta', label: 'Tendência', muted: true },
          ]}
          rows={PIPELINE}
          footer={<><span>12 fontes · 1 com erro · 2 com alerta</span><span className="mono">↻ recarregar</span></>}
        />
      </Panel>

      {/* Alertas */}
      <Panel padding={14} eyebrow="Alertas ativos" title="Qualidade dos dados"
        style={{ gridRow: 'span 1' }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          {ALERTS.map((a, i) => {
            const tone = a.sev;
            return (
              <div key={i} style={{
                display: 'grid',
                gridTemplateColumns: '6px 1fr auto',
                gap: 10,
                paddingBottom: 10,
                borderBottom: i < ALERTS.length - 1 ? '1px solid var(--border-soft)' : 'none',
              }}>
                <div style={{ background: { bad: 'var(--signal-bad)', warn: 'var(--signal-warn)', info: 'var(--signal-info)' }[tone] }} />
                <div>
                  <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
                    <Tag tone={tone}>{tone === 'bad' ? 'erro' : tone === 'warn' ? 'aviso' : 'info'}</Tag>
                    <span className="serif" style={{ fontSize: 13, fontWeight: 600 }}>{a.src}</span>
                  </div>
                  <div style={{ fontSize: 12, color: 'var(--ink-2)', marginTop: 3, lineHeight: 1.4 }}>{a.msg}</div>
                </div>
                <span className="mono" style={{ fontSize: 11, color: 'var(--ink-3)' }}>{a.age}</span>
              </div>
            );
          })}
        </div>
      </Panel>
    </div>
  );
}

window.Screen05_Pipeline = Screen05_Pipeline;
