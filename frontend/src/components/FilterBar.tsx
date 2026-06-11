import {
  DEFAULT_FILTROS,
  PERIODOS,
  RECORTES,
  useFiltros,
  type Periodo,
} from "../lib/filtros";

/** Tooltips explicando o que cada recorte de período significa. */
const HINT_PERIODO: Record<Periodo, string> = {
  "1A": "ano mais recente da série",
  "3A": "últimos 3 anos",
  "5A": "últimos 5 anos",
  "10A": "últimos 10 anos",
  Tudo: "série completa disponível",
};

export function FilterBar() {
  const [filtros, setFiltros] = useFiltros();

  return (
    <div
      style={{
        display: "flex",
        gap: 24,
        alignItems: "flex-end",
        padding: "10px 24px",
        borderBottom: "1px solid var(--border-soft)",
        background: "var(--bg-surface)",
        gridArea: "filters",
      }}
    >
      <Field
        label="Período"
        value={filtros.periodo}
        options={PERIODOS}
        hints={HINT_PERIODO}
        onChange={(p) => setFiltros({ periodo: p })}
      />
      <Field
        label="Recorte"
        value={filtros.recorte}
        options={RECORTES}
        onChange={(r) => setFiltros({ recorte: r })}
      />
      <div style={{ flex: 1 }} />
      <span
        className="mono"
        aria-live="polite"
        style={{ fontSize: 11, color: "var(--ink-3)", paddingBottom: 5 }}
      >
        período: <strong style={{ color: "var(--ink-1)" }}>{filtros.periodo}</strong>
        {" · "}
        {HINT_PERIODO[filtros.periodo]}
      </span>
      <button
        onClick={() => setFiltros(DEFAULT_FILTROS)}
        aria-label="Limpar filtros e voltar ao padrão"
        style={{
          fontSize: 11,
          color: "var(--ink-3)",
          background: "transparent",
          border: "none",
          cursor: "pointer",
          padding: "4px 8px",
        }}
        title="Limpar filtros"
      >
        ↺ limpar
      </button>
    </div>
  );
}

type FieldProps<T extends string> = {
  label: string;
  value: T;
  options: { key: T; label: string; disabled?: boolean }[];
  hints?: Partial<Record<T, string>>;
  onChange: (v: T) => void;
};

function Field<T extends string>({ label, value, options, hints, onChange }: FieldProps<T>) {
  return (
    <div className="flex flex-col" style={{ gap: 3 }}>
      <span
        style={{
          fontSize: 10,
          color: "var(--ink-3)",
          textTransform: "uppercase",
          letterSpacing: 0.8,
          fontWeight: 600,
        }}
      >
        {label}
      </span>
      <div
        role="group"
        aria-label={`Filtro de ${label.toLowerCase()}`}
        style={{ display: "flex", border: "1px solid var(--border-strong)" }}
      >
        {options.map((o, i) => {
          const sel = value === o.key;
          return (
            <button
              key={o.key}
              onClick={() => !o.disabled && onChange(o.key)}
              disabled={o.disabled}
              aria-pressed={sel}
              title={o.disabled ? "Em breve" : hints?.[o.key]}
              style={{
                background: sel ? "var(--ink-1)" : "transparent",
                color: o.disabled
                  ? "var(--ink-mute)"
                  : sel
                  ? "var(--bg-surface)"
                  : "var(--ink-2)",
                border: "none",
                borderLeft: i ? "1px solid var(--border-strong)" : "none",
                padding: "5px 12px",
                fontSize: 12,
                fontFamily: "var(--font-sans)",
                fontWeight: sel ? 600 : 500,
                cursor: o.disabled ? "not-allowed" : "pointer",
                whiteSpace: "nowrap",
                opacity: o.disabled ? 0.5 : 1,
              }}
            >
              {o.label}
            </button>
          );
        })}
      </div>
    </div>
  );
}
