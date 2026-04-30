import type { ReactNode } from "react";
import { Sparkline } from "./Sparkline";
import { fmtPct } from "../lib/format";

type Props = {
  label: string;
  value: string;
  unit?: string;
  delta?: number;
  sparkData?: number[];
  sparkColor?: string;
  sub?: ReactNode;
};

export function KPI({ label, value, unit, delta, sparkData, sparkColor, sub }: Props) {
  return (
    <div className="flex flex-col gap-1 py-2">
      <div
        style={{
          fontSize: 10.5,
          color: "var(--ink-3)",
          textTransform: "uppercase",
          letterSpacing: 0.8,
          fontWeight: 600,
        }}
      >
        {label}
      </div>
      <div className="flex items-baseline gap-2">
        <span
          className="mono serif"
          style={{
            fontSize: 26,
            fontWeight: 600,
            color: "var(--ink-1)",
            letterSpacing: -0.5,
            lineHeight: 1,
          }}
        >
          {value}
        </span>
        {unit && (
          <span className="mono" style={{ fontSize: 12, color: "var(--ink-3)" }}>
            {unit}
          </span>
        )}
        {delta != null && (
          <span
            className="mono"
            style={{
              fontSize: 12,
              color: delta >= 0 ? "var(--signal-good)" : "var(--signal-bad)",
              fontWeight: 600,
            }}
          >
            {delta >= 0 ? "▲" : "▼"} {fmtPct(delta)}
          </span>
        )}
      </div>
      {sparkData && (
        <div className="mt-0.5">
          <Sparkline data={sparkData} color={sparkColor || "var(--seq-3)"} width={120} height={20} />
        </div>
      )}
      {sub && (
        <div style={{ fontSize: 11, color: "var(--ink-3)" }}>{sub}</div>
      )}
    </div>
  );
}
