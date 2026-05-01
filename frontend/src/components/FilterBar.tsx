import { useSearchParams } from "react-router-dom";

export type Periodo = "1A" | "3A" | "5A" | "10A" | "Tudo";
export type Recorte = "Bruto" | "Per capita" | "% PIB";

const PERIODOS: { key: Periodo; label: string }[] = [
  { key: "1A", label: "1A" },
  { key: "3A", label: "3A" },
  { key: "5A", label: "5A" },
  { key: "10A", label: "10A" },
  { key: "Tudo", label: "Tudo" },
];

const RECORTES: { key: Recorte; label: string; disabled?: boolean }[] = [
  { key: "Bruto", label: "Bruto" },
  { key: "Per capita", label: "Per capita" },
  { key: "% PIB", label: "% PIB", disabled: true },
];

export type Filtros = {
  periodo: Periodo;
  recorte: Recorte;
};

export const DEFAULT_FILTROS: Filtros = {
  periodo: "Tudo",
  recorte: "Bruto",
};

export function useFiltros(): [Filtros, (f: Partial<Filtros>) => void] {
  const [params, setParams] = useSearchParams();
  const filtros: Filtros = {
    periodo: (params.get("periodo") as Periodo) || DEFAULT_FILTROS.periodo,
    recorte: (params.get("recorte") as Recorte) || DEFAULT_FILTROS.recorte,
  };
  const setFiltros = (patch: Partial<Filtros>) => {
    const next = new URLSearchParams(params);
    Object.entries(patch).forEach(([k, v]) => {
      if (v == null || v === DEFAULT_FILTROS[k as keyof Filtros]) {
        next.delete(k);
      } else {
        next.set(k, String(v));
      }
    });
    setParams(next, { replace: true });
  };
  return [filtros, setFiltros];
}

/**
 * Aplica o recorte de período sobre a lista de anos disponíveis no painel.
 * Retorna o ano-início (inclusivo) e ano-fim (inclusivo).
 */
export function aplicarPeriodo(
  todosAnos: number[],
  periodo: Periodo,
): { inicio: number; fim: number } {
  if (todosAnos.length === 0) return { inicio: 0, fim: 0 };
  const ordenados = [...todosAnos].sort((a, b) => a - b);
  const fim = ordenados[ordenados.length - 1];
  const inicio = ordenados[0];
  if (periodo === "Tudo") return { inicio, fim };
  const n = parseInt(periodo, 10) - 1;
  return { inicio: Math.max(inicio, fim - n), fim };
}

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
        onChange={(p) => setFiltros({ periodo: p })}
      />
      <Field
        label="Recorte"
        value={filtros.recorte}
        options={RECORTES}
        onChange={(r) => setFiltros({ recorte: r })}
      />
      <div style={{ flex: 1 }} />
      <button
        onClick={() => setFiltros(DEFAULT_FILTROS)}
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
  onChange: (v: T) => void;
};

function Field<T extends string>({ label, value, options, onChange }: FieldProps<T>) {
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
      <div style={{ display: "flex", border: "1px solid var(--border-strong)" }}>
        {options.map((o, i) => {
          const sel = value === o.key;
          return (
            <button
              key={o.key}
              onClick={() => !o.disabled && onChange(o.key)}
              disabled={o.disabled}
              title={o.disabled ? "Em breve" : undefined}
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
