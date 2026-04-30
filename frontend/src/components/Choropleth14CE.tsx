import { useMemo } from "react";
import { REGIOES_CE, GRID_COLS, GRID_ROWS } from "../lib/regioes";
import { fmtCompact } from "../lib/format";

type Props = {
  /** Mapa { regiao_codigo: valor } */
  values: Record<string, number | null | undefined>;
  scale?: "seq" | "div";
  width?: number;
  height?: number;
  tile?: number;
  gap?: number;
  selected?: string;
  onSelect?: (codigo: string) => void;
  onHover?: (info: { codigo: string; nome: string; value: number | null; x: number; y: number } | null) => void;
};

const SEQ = [
  "var(--seq-1)",
  "var(--seq-2)",
  "var(--seq-3)",
  "var(--seq-4)",
  "var(--seq-5)",
];

const DIV = [
  "var(--div-neg-3)",
  "var(--div-neg-2)",
  "var(--div-neg-1)",
  "var(--div-zero)",
  "var(--div-pos-1)",
  "var(--div-pos-2)",
  "var(--div-pos-3)",
];

export function Choropleth14CE({
  values,
  scale = "seq",
  width = 520,
  height = 480,
  tile = 90,
  gap = 6,
  selected,
  onSelect,
  onHover,
}: Props) {
  const { min, max } = useMemo(() => {
    const nums = Object.values(values).filter((v): v is number => typeof v === "number");
    if (nums.length === 0) return { min: 0, max: 1 };
    return { min: Math.min(...nums), max: Math.max(...nums) };
  }, [values]);

  const range = max - min || 1;

  const colorOf = (v: number | null | undefined) => {
    if (v == null || Number.isNaN(v)) return "var(--bg-sunken)";
    if (scale === "div") {
      const t = Math.max(-1, Math.min(1, v / 10));
      const i = Math.round((t + 1) * 3);
      return DIV[i];
    }
    const t = (v - min) / range;
    return SEQ[Math.min(SEQ.length - 1, Math.floor(t * SEQ.length))];
  };

  const W = GRID_COLS * (tile + gap);
  const H = GRID_ROWS * (tile + gap);

  return (
    <svg
      viewBox={`0 0 ${W} ${H}`}
      width={width}
      height={height}
      style={{ display: "block" }}
    >
      {REGIOES_CE.map((r) => {
        const x = r.col * (tile + gap);
        const y = r.row * (tile + gap);
        const v = values[r.codigo];
        const fill = colorOf(v);
        const isSel = selected === r.codigo;
        const dark = scale === "seq" && typeof v === "number" && v > min + range * 0.55;
        return (
          <g
            key={r.codigo}
            onClick={() => onSelect?.(r.codigo)}
            onMouseEnter={(e) =>
              onHover?.({ codigo: r.codigo, nome: r.nome, value: v ?? null, x: e.clientX, y: e.clientY })
            }
            onMouseLeave={() => onHover?.(null)}
            style={{ cursor: onSelect ? "pointer" : "default" }}
          >
            <rect
              x={x}
              y={y}
              width={tile}
              height={tile}
              fill={fill}
              stroke={isSel ? "var(--rule)" : "var(--bg-page)"}
              strokeWidth={isSel ? 2 : 1}
              rx={1}
            />
            <text
              x={x + tile / 2}
              y={y + tile / 2 - 4}
              textAnchor="middle"
              dominantBaseline="middle"
              style={{
                fontFamily: "var(--font-sans)",
                fontSize: 12,
                fontWeight: 600,
                fill: dark ? "var(--bg-page)" : "var(--ink-1)",
                pointerEvents: "none",
              }}
            >
              {r.codigo}
            </text>
            <text
              x={x + tile / 2}
              y={y + tile / 2 + 9}
              textAnchor="middle"
              dominantBaseline="middle"
              style={{
                fontFamily: "var(--font-sans)",
                fontSize: 8.5,
                fill: dark ? "var(--bg-page)" : "var(--ink-2)",
                pointerEvents: "none",
                opacity: 0.85,
              }}
            >
              {r.nome.split(" / ")[0].slice(0, 18)}
            </text>
            {typeof v === "number" && (
              <text
                x={x + tile / 2}
                y={y + tile - 8}
                textAnchor="middle"
                dominantBaseline="middle"
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: 10,
                  fontVariantNumeric: "tabular-nums",
                  fill: dark ? "var(--bg-page)" : "var(--ink-2)",
                  pointerEvents: "none",
                  opacity: 0.9,
                }}
              >
                {fmtCompact(v)}
              </text>
            )}
          </g>
        );
      })}
    </svg>
  );
}
