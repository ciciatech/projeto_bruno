import { fmtCompact } from "../lib/format";

const SEQ_STOPS = [
  "var(--seq-1)",
  "var(--seq-2)",
  "var(--seq-3)",
  "var(--seq-4)",
  "var(--seq-5)",
];

const DIV_STOPS = [
  "var(--div-neg-3)",
  "var(--div-neg-2)",
  "var(--div-neg-1)",
  "var(--div-zero)",
  "var(--div-pos-1)",
  "var(--div-pos-2)",
  "var(--div-pos-3)",
];

type Props = {
  scale?: "seq" | "div";
  min: number;
  max: number;
  label?: string;
  format?: (v: number) => string;
};

/**
 * Legenda de cores para o Choropleth14CE — porta de MapLegend do design
 * original. Tira o "está confuso, qual cor é mais?" sem explicação.
 */
export function MapLegend({ scale = "seq", min, max, label = "valores", format = fmtCompact }: Props) {
  const stops = scale === "seq" ? SEQ_STOPS : DIV_STOPS;
  const isNeg = scale === "div" && min < 0;
  const lo = isNeg ? min : 0;
  const hi = max;

  return (
    <div>
      <div
        style={{
          fontSize: 10,
          color: "var(--ink-3)",
          textTransform: "uppercase",
          letterSpacing: 0.8,
          fontWeight: 600,
          marginBottom: 6,
        }}
      >
        Escala
      </div>
      <div
        style={{
          display: "flex",
          height: 10,
          marginBottom: 4,
          border: "1px solid var(--border-soft)",
        }}
      >
        {stops.map((c, i) => {
          // Faixa de valores coberta por cada bin (escala linear lo→hi):
          // legível no hover em vez do antigo "bin N".
          const passo = (hi - lo) / stops.length;
          const de = lo + i * passo;
          const ate = lo + (i + 1) * passo;
          return (
            <div
              key={i}
              style={{ flex: 1, background: c }}
              title={`${format(de)} – ${format(ate)}`}
            />
          );
        })}
      </div>
      <div
        className="mono"
        style={{
          display: "flex",
          justifyContent: "space-between",
          fontSize: 9.5,
          color: "var(--ink-3)",
          marginBottom: 2,
        }}
      >
        <span>{format(lo)}</span>
        <span>{format(lo + (hi - lo) / 2)}</span>
        <span>{format(hi)}</span>
      </div>
      <div
        style={{
          fontSize: 10,
          color: "var(--ink-4)",
          fontStyle: "italic",
        }}
      >
        {label}
      </div>
    </div>
  );
}
