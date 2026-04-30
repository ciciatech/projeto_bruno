/* Chrome compartilhada: header, filter bar, painéis, badges,
   tabela densa, tooltip, breadcrumbs.
   ----------------------------------------------------------- */

function AppHeader({ screen, theme, setTheme }) {
  return (
    <header style={{
      gridArea: 'header',
      display: 'flex', alignItems: 'center',
      borderBottom: '1px solid var(--border-soft)',
      background: 'var(--bg-surface)',
      padding: '0 24px',
      height: 56,
      gap: 24,
    }}>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: 10 }}>
        <span className="serif" style={{ fontSize: 22, fontWeight: 600, letterSpacing: -0.4, color: 'var(--ink-1)' }}>
          Prisma<span style={{ color: 'var(--seq-3)' }}>·</span>Regional
        </span>
        <span style={{ fontSize: 11, color: 'var(--ink-3)', textTransform: 'uppercase', letterSpacing: 0.8 }}>
          v0.4 · IPEA
        </span>
      </div>

      <nav style={{ display: 'flex', gap: 2, marginLeft: 8 }}>
        {['Investimento','Emprego','Setores','Causal','Pipeline'].map((n, i) => {
          const active = screen === i + 1;
          return (
            <a key={n} href="#" style={{
              padding: '8px 12px',
              fontSize: 13,
              fontWeight: active ? 600 : 500,
              color: active ? 'var(--ink-1)' : 'var(--ink-3)',
              borderBottom: active ? '2px solid var(--seq-3)' : '2px solid transparent',
              textDecoration: 'none',
              marginBottom: -1,
            }}>
              <span className="mono" style={{ fontSize: 10, color: 'var(--ink-4)', marginRight: 6 }}>{String(i+1).padStart(2,'0')}</span>
              {n}
            </a>
          );
        })}
      </nav>

      <div style={{ flex: 1 }} />

      <div style={{ display: 'flex', alignItems: 'center', gap: 16, fontSize: 12, color: 'var(--ink-3)' }}>
        <span style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <span style={{ width: 6, height: 6, borderRadius: 3, background: 'var(--signal-good)' }} />
          <span>Pipeline saudável · atualizado há 2 min</span>
        </span>
        <span style={{ borderLeft: '1px solid var(--border-soft)', height: 16 }} />
        <button onClick={() => setTheme(theme === 'light' ? 'dark' : 'light')}
          style={{ ...btn(), padding: '4px 8px', fontSize: 12 }}>
          {theme === 'light' ? '◐ escuro' : '◑ claro'}
        </button>
        <button style={{ ...btn(), padding: '4px 10px' }}>↗ Exportar</button>
        <span className="mono" style={{ color: 'var(--ink-4)' }}>m.silva@ipea.gov.br</span>
      </div>
    </header>
  );
}

function btn() {
  return {
    fontFamily: 'var(--font-sans)',
    fontSize: 12,
    color: 'var(--ink-2)',
    background: 'var(--bg-surface)',
    border: '1px solid var(--border-strong)',
    borderRadius: 'var(--radius-sm)',
    padding: '5px 10px',
    cursor: 'pointer',
    lineHeight: 1.2,
  };
}

function FilterBar({ values, setValues, extra }) {
  const v = values || {};
  const Field = ({ label, options, k }) => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
      <span style={{ fontSize: 10, color: 'var(--ink-3)', textTransform: 'uppercase', letterSpacing: 0.8, fontWeight: 600 }}>{label}</span>
      <div style={{ display: 'flex', gap: 0, border: '1px solid var(--border-strong)' }}>
        {options.map((o, i) => {
          const sel = v[k] === o;
          return (
            <button key={o} onClick={() => setValues && setValues({ ...v, [k]: o })}
              style={{
                background: sel ? 'var(--ink-1)' : 'transparent',
                color: sel ? 'var(--bg-surface)' : 'var(--ink-2)',
                border: 'none',
                borderLeft: i ? '1px solid var(--border-strong)' : 'none',
                padding: '4px 10px',
                fontSize: 12,
                fontFamily: 'var(--font-sans)',
                fontWeight: sel ? 600 : 500,
                cursor: 'pointer',
                whiteSpace: 'nowrap',
              }}>{o}</button>
          );
        })}
      </div>
    </div>
  );

  return (
    <div style={{
      gridArea: 'filters',
      display: 'flex', gap: 24, alignItems: 'flex-end',
      padding: '12px 24px',
      borderBottom: '1px solid var(--border-soft)',
      background: 'var(--bg-surface)',
    }}>
      <Field label="Período" k="periodo" options={['1A','3A','5A','10A','Custom']} />
      <Field label="Geografia" k="geo" options={['UF','Mesorregião','Município']} />
      <Field label="Indicador" k="ind" options={['Anunciado','Realizado','Comprometido']} />
      <Field label="Setor" k="setor" options={['Todos','Indústria','Serviços','Agro','Constr.','Comércio']} />
      <Field label="Recorte" k="rec" options={['Bruto','Per capita','% PIB']} />

      <div style={{ flex: 1 }} />
      {extra}
      <button style={{ ...btn(), padding: '6px 12px', fontSize: 12 }}>＋ adicionar filtro</button>
      <button style={{ ...btn(), background: 'var(--ink-1)', color: 'var(--bg-surface)', borderColor: 'var(--ink-1)', padding: '6px 14px', fontWeight: 600 }}>
        Aplicar
      </button>
    </div>
  );
}

function Panel({ title, eyebrow, subtitle, action, children, footer, style, padding = 16 }) {
  return (
    <section style={{
      background: 'var(--bg-surface)',
      border: '1px solid var(--border-soft)',
      display: 'flex', flexDirection: 'column',
      minWidth: 0, minHeight: 0,
      ...style,
    }}>
      {(title || eyebrow) && (
        <header style={{
          padding: `${padding - 4}px ${padding}px ${padding - 8}px`,
          display: 'flex', alignItems: 'flex-start', gap: 12,
          borderBottom: '1px solid var(--border-soft)',
        }}>
          <div style={{ flex: 1, minWidth: 0 }}>
            {eyebrow && (
              <div style={{ fontSize: 10, color: 'var(--ink-3)', textTransform: 'uppercase', letterSpacing: 0.8, fontWeight: 600, marginBottom: 4 }}>
                {eyebrow}
              </div>
            )}
            {title && (
              <h2 className="serif" style={{ margin: 0, fontSize: 17, fontWeight: 600, color: 'var(--ink-1)', letterSpacing: -0.2, lineHeight: 1.2 }}>
                {title}
              </h2>
            )}
            {subtitle && (
              <div style={{ fontSize: 12, color: 'var(--ink-3)', marginTop: 4, lineHeight: 1.4 }}>{subtitle}</div>
            )}
          </div>
          {action && <div style={{ flexShrink: 0 }}>{action}</div>}
        </header>
      )}
      <div style={{ padding, flex: 1, minWidth: 0, minHeight: 0, overflow: 'hidden' }}>{children}</div>
      {footer && (
        <footer style={{ padding: `8px ${padding}px`, borderTop: '1px solid var(--border-soft)',
                         fontSize: 11, color: 'var(--ink-3)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          {footer}
        </footer>
      )}
    </section>
  );
}

function PanelAction({ children }) {
  return (
    <div style={{ display: 'flex', gap: 4 }}>
      {React.Children.map(children, (c, i) => (
        <button key={i} style={{ ...btn(), padding: '3px 8px', fontSize: 11 }}>{c}</button>
      ))}
    </div>
  );
}

function KPI({ label, value, unit, delta, sparkData, sparkColor, sub }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 4, padding: '8px 0' }}>
      <div style={{ fontSize: 10.5, color: 'var(--ink-3)', textTransform: 'uppercase', letterSpacing: 0.8, fontWeight: 600 }}>{label}</div>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
        <span className="mono serif" style={{ fontSize: 26, fontWeight: 600, color: 'var(--ink-1)', letterSpacing: -0.5, lineHeight: 1 }}>
          {value}
        </span>
        {unit && <span className="mono" style={{ fontSize: 12, color: 'var(--ink-3)' }}>{unit}</span>}
        {delta != null && (
          <span className="mono" style={{
            fontSize: 12,
            color: delta >= 0 ? 'var(--signal-good)' : 'var(--signal-bad)',
            fontWeight: 600,
          }}>
            {delta >= 0 ? '▲' : '▼'} {fmtPct(delta)}
          </span>
        )}
      </div>
      {sparkData && (
        <div style={{ marginTop: 2 }}>
          <Sparkline data={sparkData} color={sparkColor || 'var(--seq-3)'} width={120} height={20} />
        </div>
      )}
      {sub && <div style={{ fontSize: 11, color: 'var(--ink-3)' }}>{sub}</div>}
    </div>
  );
}

function Tag({ children, tone = 'neutral' }) {
  const tones = {
    neutral: { bg: 'var(--bg-sunken)', fg: 'var(--ink-2)' },
    good:    { bg: 'rgba(31,111,106,0.12)', fg: 'var(--signal-good)' },
    warn:    { bg: 'rgba(180,100,20,0.12)', fg: 'var(--signal-warn)' },
    bad:     { bg: 'rgba(140,42,28,0.12)', fg: 'var(--signal-bad)' },
    info:    { bg: 'rgba(42,64,96,0.10)', fg: 'var(--signal-info)' },
  };
  const t = tones[tone];
  return (
    <span className="mono" style={{
      background: t.bg, color: t.fg,
      fontSize: 10.5, fontWeight: 600,
      padding: '2px 6px',
      borderRadius: 'var(--radius-sm)',
      textTransform: 'uppercase', letterSpacing: 0.5,
    }}>{children}</span>
  );
}

function Tooltip({ data }) {
  if (!data) return null;
  return (
    <div style={{
      position: 'fixed',
      left: data.x + 14, top: data.y + 14,
      background: 'var(--bg-surface)',
      border: '1px solid var(--border-strong)',
      boxShadow: 'var(--shadow-pop)',
      padding: '10px 12px',
      pointerEvents: 'none',
      minWidth: 200,
      zIndex: 100,
    }}>
      <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', gap: 8, marginBottom: 4 }}>
        <span className="serif" style={{ fontSize: 14, fontWeight: 600, color: 'var(--ink-1)' }}>{data.name}</span>
        <span className="mono" style={{ fontSize: 10, color: 'var(--ink-3)', textTransform: 'uppercase', letterSpacing: 0.6 }}>{data.uf} · {data.region}</span>
      </div>
      {data.children}
      {data.value != null && !data.children && (
        <>
          <div className="mono" style={{ fontSize: 18, fontWeight: 600, color: 'var(--ink-1)' }}>
            {fmtBRL(data.value * 1e6)}
          </div>
          <div style={{ fontSize: 11, color: 'var(--ink-3)', marginTop: 4 }}>investimento anunciado · 2024</div>
        </>
      )}
    </div>
  );
}

/* Tabela densa para "leitura de tabelas grandes" */
function DenseTable({ columns, rows, sortKey, onSort, footer }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0 }}>
      <div style={{ overflow: 'auto', flex: 1 }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
          <thead>
            <tr style={{ position: 'sticky', top: 0, background: 'var(--bg-surface)', zIndex: 1 }}>
              {columns.map((c, i) => (
                <th key={i} onClick={() => onSort && onSort(c.key)}
                  style={{
                    textAlign: c.align || 'left',
                    padding: '6px 10px',
                    fontSize: 10, fontWeight: 600, color: 'var(--ink-3)',
                    textTransform: 'uppercase', letterSpacing: 0.6,
                    borderBottom: '1px solid var(--rule)',
                    cursor: onSort ? 'pointer' : 'default',
                    whiteSpace: 'nowrap',
                  }}>
                  {c.label}
                  {sortKey === c.key && <span style={{ marginLeft: 4 }}>↓</span>}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i} style={{ borderBottom: '1px solid var(--border-soft)' }}>
                {columns.map((c, j) => (
                  <td key={j} className={c.mono ? 'mono' : ''}
                    style={{
                      padding: '6px 10px',
                      textAlign: c.align || 'left',
                      color: c.muted ? 'var(--ink-3)' : 'var(--ink-1)',
                      fontVariantNumeric: c.mono ? 'tabular-nums' : 'normal',
                      whiteSpace: 'nowrap',
                    }}>
                    {c.render ? c.render(r) : r[c.key]}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {footer && (
        <div style={{ padding: '6px 10px', fontSize: 11, color: 'var(--ink-3)', borderTop: '1px solid var(--border-soft)', display: 'flex', justifyContent: 'space-between' }}>
          {footer}
        </div>
      )}
    </div>
  );
}

/* Anotação editorial — citação ou nota de rodapé inline */
function EditorialNote({ children }) {
  return (
    <div style={{
      borderLeft: '2px solid var(--seq-3)',
      paddingLeft: 10,
      fontSize: 12,
      color: 'var(--ink-2)',
      lineHeight: 1.5,
      fontStyle: 'italic',
      fontFamily: 'var(--font-serif)',
    }}>{children}</div>
  );
}

Object.assign(window, { AppHeader, FilterBar, Panel, PanelAction, KPI, Tag, Tooltip, DenseTable, EditorialNote, btn });
