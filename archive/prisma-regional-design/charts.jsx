/* Chart primitives — Prisma Regional
   ---------------------------------------------------
   Editorial-grade charts: sparse axes, no chart-junk,
   Tufte-ish data-ink. SVG only, no deps.
   --------------------------------------------------- */

const fmtBRL = (v) => {
  if (Math.abs(v) >= 1e9) return 'R$ ' + (v / 1e9).toFixed(1).replace('.', ',') + ' bi';
  if (Math.abs(v) >= 1e6) return 'R$ ' + (v / 1e6).toFixed(0) + ' mi';
  if (Math.abs(v) >= 1e3) return 'R$ ' + (v / 1e3).toFixed(0) + ' mil';
  return 'R$ ' + v.toFixed(0);
};
const fmtNum = (v) => v.toLocaleString('pt-BR');
const fmtPct = (v, d = 1) => (v >= 0 ? '+' : '') + v.toFixed(d).replace('.', ',') + '%';
const fmtCompact = (v) => {
  if (Math.abs(v) >= 1e9) return (v / 1e9).toFixed(1).replace('.', ',') + ' bi';
  if (Math.abs(v) >= 1e6) return (v / 1e6).toFixed(0) + ' mi';
  if (Math.abs(v) >= 1e3) return (v / 1e3).toFixed(0) + 'k';
  return v.toFixed(0);
};

/* ── ChoroplethBR ────────────────────────────────
   Mapa simbólico estilizado dos 27 UFs (não geograficamente preciso,
   mas reconhecível e suficiente para hi-fi mockup). Cada UF é um
   tile arranjado em layout que aproxima a forma do Brasil.
   Para produção, trocar por TopoJSON real. */

const UF_LAYOUT = [
  // [code, col, row, fullName, region]
  ['RR', 2, 0, 'Roraima',         'N'],
  ['AP', 4, 0, 'Amapá',           'N'],
  ['AM', 1, 1, 'Amazonas',        'N'],
  ['PA', 3, 1, 'Pará',            'N'],
  ['MA', 4, 1, 'Maranhão',        'NE'],
  ['CE', 5, 1, 'Ceará',           'NE'],
  ['RN', 6, 1, 'Rio Grande do Norte', 'NE'],
  ['AC', 0, 2, 'Acre',            'N'],
  ['RO', 1, 2, 'Rondônia',        'N'],
  ['TO', 3, 2, 'Tocantins',       'N'],
  ['PI', 4, 2, 'Piauí',           'NE'],
  ['PB', 6, 2, 'Paraíba',         'NE'],
  ['PE', 5, 2, 'Pernambuco',      'NE'],
  ['AL', 6, 3, 'Alagoas',         'NE'],
  ['SE', 5, 3, 'Sergipe',         'NE'],
  ['BA', 4, 3, 'Bahia',           'NE'],
  ['MT', 2, 3, 'Mato Grosso',     'CO'],
  ['DF', 3, 3, 'Distrito Federal','CO'],
  ['GO', 3, 4, 'Goiás',           'CO'],
  ['MS', 2, 4, 'Mato Grosso do Sul','CO'],
  ['MG', 4, 4, 'Minas Gerais',    'SE'],
  ['ES', 5, 4, 'Espírito Santo',  'SE'],
  ['SP', 3, 5, 'São Paulo',       'SE'],
  ['RJ', 4, 5, 'Rio de Janeiro',  'SE'],
  ['PR', 3, 6, 'Paraná',          'S'],
  ['SC', 3, 7, 'Santa Catarina',  'S'],
  ['RS', 2, 8, 'Rio Grande do Sul','S'],
];

function ChoroplethBR({ values, scale = 'seq', width = 520, height = 620, onHover, selected, tile = 56, gap = 4, showLabels = true }) {
  // values: { UF: number }
  const vs = Object.values(values).filter((v) => v != null);
  const min = Math.min(...vs), max = Math.max(...vs);
  const range = max - min || 1;
  const buckets = scale === 'seq'
    ? ['var(--seq-1)','var(--seq-2)','var(--seq-3)','var(--seq-4)','var(--seq-5)']
    : ['var(--div-neg-3)','var(--div-neg-2)','var(--div-neg-1)','var(--div-zero)','var(--div-pos-1)','var(--div-pos-2)','var(--div-pos-3)'];

  const colorOf = (v) => {
    if (v == null) return 'var(--bg-sunken)';
    if (scale === 'div') {
      // -10 .. +10 mapped to 7 buckets centered at 0
      const t = Math.max(-1, Math.min(1, v / 10));
      const i = Math.round((t + 1) * 3);
      return buckets[i];
    }
    const t = (v - min) / range;
    return buckets[Math.min(buckets.length - 1, Math.floor(t * buckets.length))];
  };

  const cols = 7, rows = 9;
  const W = cols * (tile + gap);
  const H = rows * (tile + gap);

  return (
    <svg viewBox={`0 0 ${W} ${H}`} width={width} height={height} style={{ display: 'block' }}>
      {UF_LAYOUT.map(([uf, c, r, name, region]) => {
        const x = c * (tile + gap);
        const y = r * (tile + gap);
        const v = values[uf];
        const fill = colorOf(v);
        const isSel = selected === uf;
        return (
          <g key={uf}
             onMouseEnter={onHover ? (e) => onHover({ uf, name, region, value: v, x: e.clientX, y: e.clientY }) : undefined}
             onMouseLeave={onHover ? () => onHover(null) : undefined}
             style={{ cursor: 'pointer' }}>
            <rect x={x} y={y} width={tile} height={tile} fill={fill}
                  stroke={isSel ? 'var(--rule)' : 'var(--bg-page)'}
                  strokeWidth={isSel ? 2 : 1}
                  rx="1" />
            {showLabels && (
              <>
                <text x={x + tile / 2} y={y + tile / 2 - 2}
                      textAnchor="middle" dominantBaseline="middle"
                      style={{
                        fontFamily: 'var(--font-sans)',
                        fontSize: 11, fontWeight: 600,
                        fill: (scale === 'seq' && v > min + range * 0.55) ? 'var(--bg-page)' : 'var(--ink-1)',
                        pointerEvents: 'none',
                      }}>{uf}</text>
                {v != null && (
                  <text x={x + tile / 2} y={y + tile / 2 + 11}
                        textAnchor="middle" dominantBaseline="middle"
                        style={{
                          fontFamily: 'var(--font-mono)',
                          fontSize: 9, fontVariantNumeric: 'tabular-nums',
                          fill: (scale === 'seq' && v > min + range * 0.55) ? 'var(--bg-page)' : 'var(--ink-2)',
                          pointerEvents: 'none',
                          opacity: 0.85,
                        }}>{scale === 'div' ? fmtPct(v, 1) : fmtCompact(v)}</text>
                )}
              </>
            )}
          </g>
        );
      })}
    </svg>
  );
}

/* ── Legenda de mapa (gradiente discreto) ─────── */
function MapLegend({ scale = 'seq', min, max, label, format = fmtCompact }) {
  const buckets = scale === 'seq'
    ? ['var(--seq-1)','var(--seq-2)','var(--seq-3)','var(--seq-4)','var(--seq-5)']
    : ['var(--div-neg-3)','var(--div-neg-2)','var(--div-neg-1)','var(--div-zero)','var(--div-pos-1)','var(--div-pos-2)','var(--div-pos-3)'];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
      <div style={{ fontSize: 11, color: 'var(--ink-3)', textTransform: 'uppercase', letterSpacing: 0.6, fontWeight: 600 }}>{label}</div>
      <div style={{ display: 'flex', gap: 0 }}>
        {buckets.map((c, i) => (
          <div key={i} style={{ width: 28, height: 12, background: c, borderTop: '1px solid var(--border-soft)', borderBottom: '1px solid var(--border-soft)',
            ...(i === 0 ? { borderLeft: '1px solid var(--border-soft)' } : {}),
            ...(i === buckets.length - 1 ? { borderRight: '1px solid var(--border-soft)' } : {})
          }} />
        ))}
      </div>
      <div className="mono" style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, color: 'var(--ink-2)', width: buckets.length * 28 }}>
        <span>{scale === 'div' ? '−10%' : format(min)}</span>
        <span>{scale === 'div' ? '0' : ''}</span>
        <span>{scale === 'div' ? '+10%' : format(max)}</span>
      </div>
    </div>
  );
}

/* ── Linhas (séries temporais) ────────────────── */
function LineChart({ series, width = 720, height = 260, padding = { t: 20, r: 24, b: 32, l: 56 }, yLabel, xTicks, yFormat = fmtCompact, highlight, dashed }) {
  // series: [{ name, color, points: [{x, y}] }]
  const all = series.flatMap((s) => s.points);
  const xs = all.map((p) => p.x), ys = all.map((p) => p.y);
  const xMin = Math.min(...xs), xMax = Math.max(...xs);
  const yMin = Math.min(0, Math.min(...ys));
  const yMax = Math.max(...ys) * 1.05;

  const W = width - padding.l - padding.r;
  const H = height - padding.t - padding.b;
  const sx = (x) => padding.l + ((x - xMin) / (xMax - xMin || 1)) * W;
  const sy = (y) => padding.t + (1 - (y - yMin) / (yMax - yMin || 1)) * H;

  const yTicks = 4;
  const yStep = (yMax - yMin) / yTicks;

  return (
    <svg width={width} height={height} style={{ display: 'block', overflow: 'visible' }}>
      {/* gridlines horizontais */}
      {Array.from({ length: yTicks + 1 }).map((_, i) => {
        const y = sy(yMin + i * yStep);
        return (
          <g key={i}>
            <line x1={padding.l} x2={width - padding.r} y1={y} y2={y}
                  stroke="var(--border-soft)" strokeWidth="0.75" />
            <text x={padding.l - 8} y={y + 3} textAnchor="end"
                  style={{ fontFamily: 'var(--font-mono)', fontSize: 11, fill: 'var(--ink-3)', fontVariantNumeric: 'tabular-nums' }}>
              {yFormat(yMin + i * yStep)}
            </text>
          </g>
        );
      })}
      {/* eixo x ticks */}
      {(xTicks || []).map((t, i) => {
        const xv = typeof t === 'object' ? t.x : t;
        const lbl = typeof t === 'object' ? t.label : t;
        return (
          <text key={i} x={sx(xv)} y={height - padding.b + 16} textAnchor="middle"
                style={{ fontFamily: 'var(--font-mono)', fontSize: 11, fill: 'var(--ink-3)' }}>{lbl}</text>
        );
      })}
      {/* baseline */}
      <line x1={padding.l} x2={width - padding.r} y1={sy(0)} y2={sy(0)} stroke="var(--rule)" strokeWidth="1" />
      {/* series */}
      {series.map((s, i) => {
        const d = s.points.map((p, j) => `${j ? 'L' : 'M'}${sx(p.x)},${sy(p.y)}`).join(' ');
        return (
          <g key={i}>
            <path d={d} fill="none" stroke={s.color || 'var(--ink-1)'}
                  strokeWidth={s.weight || (highlight && highlight === s.name ? 2.5 : 1.5)}
                  strokeLinejoin="round" strokeLinecap="round"
                  strokeDasharray={(dashed && dashed.includes(s.name)) ? '4 3' : undefined}
                  opacity={highlight && highlight !== s.name ? 0.35 : 1}/>
            {/* end label */}
            {s.points.length > 0 && (
              <text x={sx(s.points[s.points.length - 1].x) + 5}
                    y={sy(s.points[s.points.length - 1].y) + 3}
                    style={{ fontFamily: 'var(--font-sans)', fontSize: 11, fill: s.color || 'var(--ink-1)', fontWeight: 600 }}>
                {s.name}
              </text>
            )}
          </g>
        );
      })}
      {yLabel && (
        <text x={padding.l - 44} y={padding.t - 6}
              style={{ fontFamily: 'var(--font-sans)', fontSize: 10.5, fill: 'var(--ink-3)', textTransform: 'uppercase', letterSpacing: 0.6, fontWeight: 600 }}>{yLabel}</text>
      )}
    </svg>
  );
}

/* ── Barras horizontais ─────────────────────── */
function HBar({ data, width = 380, height = 220, valueFormat = fmtCompact, color = 'var(--seq-3)', maxBars = 10 }) {
  const items = data.slice(0, maxBars);
  const max = Math.max(...items.map((d) => Math.abs(d.value)));
  const labelW = 86;
  const barW = width - labelW - 70;
  const rowH = (height - 8) / items.length;

  return (
    <svg width={width} height={height} style={{ display: 'block' }}>
      {items.map((d, i) => {
        const w = (Math.abs(d.value) / max) * barW;
        const y = i * rowH + 4;
        const isNeg = d.value < 0;
        const fill = d.color || (isNeg ? 'var(--div-neg-2)' : color);
        return (
          <g key={d.label}>
            <text x={labelW - 8} y={y + rowH / 2 + 3} textAnchor="end"
                  style={{ fontFamily: 'var(--font-sans)', fontSize: 12, fill: 'var(--ink-2)' }}>
              {d.label}
            </text>
            <rect x={labelW} y={y + 4} width={w} height={rowH - 8} fill={fill} />
            <text x={labelW + w + 6} y={y + rowH / 2 + 3}
                  style={{ fontFamily: 'var(--font-mono)', fontSize: 11, fill: 'var(--ink-1)', fontVariantNumeric: 'tabular-nums', fontWeight: 600 }}>
              {valueFormat(d.value)}
            </text>
          </g>
        );
      })}
    </svg>
  );
}

/* ── Stacked bars (composição) ───────────────── */
function StackedBars({ data, keys, colors, width = 720, height = 240, padding = { t: 16, r: 12, b: 36, l: 56 } }) {
  // data: [{ x: 'label', a: 10, b: 20 }, ...]
  const W = width - padding.l - padding.r;
  const H = height - padding.t - padding.b;
  const max = Math.max(...data.map((d) => keys.reduce((s, k) => s + d[k], 0)));
  const bw = W / data.length * 0.7;
  const sx = (i) => padding.l + (i + 0.5) * (W / data.length) - bw / 2;
  const sy = (v) => padding.t + (1 - v / max) * H;

  return (
    <svg width={width} height={height} style={{ display: 'block', overflow: 'visible' }}>
      {[0, 0.25, 0.5, 0.75, 1].map((t, i) => {
        const y = padding.t + (1 - t) * H;
        return (
          <g key={i}>
            <line x1={padding.l} x2={width - padding.r} y1={y} y2={y} stroke="var(--border-soft)" strokeWidth="0.75" />
            <text x={padding.l - 8} y={y + 3} textAnchor="end"
                  style={{ fontFamily: 'var(--font-mono)', fontSize: 11, fill: 'var(--ink-3)', fontVariantNumeric: 'tabular-nums' }}>
              {fmtCompact(t * max)}
            </text>
          </g>
        );
      })}
      {data.map((d, i) => {
        let acc = 0;
        return (
          <g key={i}>
            {keys.map((k, j) => {
              const v = d[k];
              const y0 = sy(acc + v), y1 = sy(acc);
              acc += v;
              return <rect key={k} x={sx(i)} width={bw} y={y0} height={y1 - y0} fill={colors[j]} />;
            })}
            <text x={sx(i) + bw / 2} y={height - padding.b + 14} textAnchor="middle"
                  style={{ fontFamily: 'var(--font-mono)', fontSize: 11, fill: 'var(--ink-3)' }}>{d.x}</text>
          </g>
        );
      })}
    </svg>
  );
}

/* ── Scatter + linha de regressão ─────────────── */
function ScatterRegression({ points, width = 720, height = 420, padding = { t: 28, r: 28, b: 48, l: 64 }, xLabel, yLabel, xFormat = fmtCompact, yFormat = (v) => v.toFixed(1), highlight, regression }) {
  // points: [{ x, y, label, region, size? }]
  const xs = points.map((p) => p.x), ys = points.map((p) => p.y);
  const xMin = Math.min(...xs), xMax = Math.max(...xs);
  const yMin = Math.min(...ys), yMax = Math.max(...ys);
  const padX = (xMax - xMin) * 0.05, padY = (yMax - yMin) * 0.08;
  const xLo = xMin - padX, xHi = xMax + padX;
  const yLo = yMin - padY, yHi = yMax + padY;

  const W = width - padding.l - padding.r;
  const H = height - padding.t - padding.b;
  const sx = (x) => padding.l + ((x - xLo) / (xHi - xLo)) * W;
  const sy = (y) => padding.t + (1 - (y - yLo) / (yHi - yLo)) * H;

  const regionColor = {
    N:  'var(--cat-3)',
    NE: 'var(--cat-1)',
    CO: 'var(--cat-4)',
    SE: 'var(--cat-2)',
    S:  'var(--cat-5)',
  };

  return (
    <svg width={width} height={height} style={{ display: 'block', overflow: 'visible' }}>
      {/* grid */}
      {[0, 0.25, 0.5, 0.75, 1].map((t, i) => {
        const x = padding.l + t * W;
        const y = padding.t + (1 - t) * H;
        return (
          <g key={i}>
            <line x1={padding.l} x2={width - padding.r} y1={y} y2={y} stroke="var(--border-soft)" strokeWidth="0.5" />
            <line y1={padding.t} y2={height - padding.b} x1={x} x2={x} stroke="var(--border-soft)" strokeWidth="0.5" />
            <text x={x} y={height - padding.b + 16} textAnchor="middle" style={{ fontFamily: 'var(--font-mono)', fontSize: 11, fill: 'var(--ink-3)' }}>
              {xFormat(xLo + t * (xHi - xLo))}
            </text>
            <text x={padding.l - 8} y={y + 3} textAnchor="end" style={{ fontFamily: 'var(--font-mono)', fontSize: 11, fill: 'var(--ink-3)' }}>
              {yFormat(yLo + t * (yHi - yLo))}
            </text>
          </g>
        );
      })}
      {/* axis labels */}
      <text x={padding.l + W / 2} y={height - 6} textAnchor="middle"
            style={{ fontFamily: 'var(--font-sans)', fontSize: 11, fill: 'var(--ink-3)', textTransform: 'uppercase', letterSpacing: 0.6, fontWeight: 600 }}>{xLabel}</text>
      <text transform={`rotate(-90 ${14} ${padding.t + H / 2})`} x={14} y={padding.t + H / 2} textAnchor="middle"
            style={{ fontFamily: 'var(--font-sans)', fontSize: 11, fill: 'var(--ink-3)', textTransform: 'uppercase', letterSpacing: 0.6, fontWeight: 600 }}>{yLabel}</text>

      {/* regression line */}
      {regression && (
        <>
          <line x1={sx(xLo)} y1={sy(regression.intercept + regression.slope * xLo)}
                x2={sx(xHi)} y2={sy(regression.intercept + regression.slope * xHi)}
                stroke="var(--ink-1)" strokeWidth="1.5" strokeDasharray="6 3" />
          {regression.ci && (
            <path d={`M${sx(xLo)},${sy(regression.intercept + regression.slope * xLo - regression.ci)}
                       L${sx(xHi)},${sy(regression.intercept + regression.slope * xHi - regression.ci)}
                       L${sx(xHi)},${sy(regression.intercept + regression.slope * xHi + regression.ci)}
                       L${sx(xLo)},${sy(regression.intercept + regression.slope * xLo + regression.ci)} Z`}
                  fill="var(--ink-1)" opacity="0.06" />
          )}
        </>
      )}

      {/* points */}
      {points.map((p, i) => {
        const r = p.size ? Math.sqrt(p.size) * 1.2 + 3 : 4;
        const sel = highlight && highlight === p.label;
        return (
          <g key={i}>
            <circle cx={sx(p.x)} cy={sy(p.y)} r={r}
                    fill={regionColor[p.region] || 'var(--ink-3)'}
                    stroke={sel ? 'var(--rule)' : 'var(--bg-surface)'}
                    strokeWidth={sel ? 1.8 : 1}
                    fillOpacity={sel ? 1 : 0.78} />
            {(sel || (p.size && p.size > 50)) && (
              <text x={sx(p.x) + r + 4} y={sy(p.y) + 3}
                    style={{ fontFamily: 'var(--font-sans)', fontSize: 11, fill: 'var(--ink-1)', fontWeight: 600 }}>{p.label}</text>
            )}
          </g>
        );
      })}
    </svg>
  );
}

/* ── Sparkline (inline, sem eixos) ─────────────── */
function Sparkline({ data, width = 80, height = 22, color = 'var(--seq-3)', showDot = true }) {
  if (!data || !data.length) return null;
  const min = Math.min(...data), max = Math.max(...data);
  const range = max - min || 1;
  const sx = (i) => (i / (data.length - 1)) * width;
  const sy = (v) => height - ((v - min) / range) * (height - 4) - 2;
  const d = data.map((v, i) => `${i ? 'L' : 'M'}${sx(i)},${sy(v)}`).join(' ');
  return (
    <svg width={width} height={height} style={{ display: 'inline-block', verticalAlign: 'middle' }}>
      <path d={d} fill="none" stroke={color} strokeWidth="1.2" strokeLinejoin="round" />
      {showDot && <circle cx={sx(data.length - 1)} cy={sy(data[data.length - 1])} r="2" fill={color} />}
    </svg>
  );
}

/* ── Bullet (KPI compacto com referência) ────── */
function Bullet({ value, target, range = [0, 100], width = 140, height = 22, format = fmtCompact, color = 'var(--seq-3)' }) {
  const [lo, hi] = range;
  const sx = (v) => ((v - lo) / (hi - lo)) * (width - 2) + 1;
  return (
    <svg width={width} height={height}>
      <rect x="0" y="6" width={width} height={height - 12} fill="var(--bg-sunken)" />
      <rect x="0" y="6" width={sx(value)} height={height - 12} fill={color} />
      <line x1={sx(target)} x2={sx(target)} y1="2" y2={height - 2} stroke="var(--rule)" strokeWidth="1.5" />
    </svg>
  );
}

Object.assign(window, {
  fmtBRL, fmtNum, fmtPct, fmtCompact,
  ChoroplethBR, MapLegend,
  LineChart, HBar, StackedBars, ScatterRegression, Sparkline, Bullet,
  UF_LAYOUT,
});
