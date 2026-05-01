import type { ReactNode } from "react";

export type HoverInfo = {
  codigo: string;
  nome: string;
  value: number | null;
  x: number;
  y: number;
};

type Props = {
  hover: HoverInfo | null;
  children?: (info: HoverInfo) => ReactNode;
};

/**
 * Tooltip flutuante para o Choropleth — porta do componente Tooltip do
 * design original. Posição via clientX/clientY do mouse; render em
 * position:fixed para ficar em cima do SVG sem clipar.
 */
export function MapTooltip({ hover, children }: Props) {
  if (!hover) return null;
  return (
    <div
      style={{
        position: "fixed",
        left: hover.x + 14,
        top: hover.y + 14,
        background: "var(--bg-surface)",
        border: "1px solid var(--border-strong)",
        boxShadow: "var(--shadow-pop)",
        padding: "10px 12px",
        pointerEvents: "none",
        minWidth: 200,
        zIndex: 100,
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "baseline",
          justifyContent: "space-between",
          gap: 8,
          marginBottom: 4,
        }}
      >
        <span
          className="serif"
          style={{ fontSize: 14, fontWeight: 600, color: "var(--ink-1)" }}
        >
          {hover.nome}
        </span>
        <span
          className="mono"
          style={{
            fontSize: 10,
            color: "var(--ink-3)",
            textTransform: "uppercase",
            letterSpacing: 0.6,
          }}
        >
          região {hover.codigo}
        </span>
      </div>
      {children ? children(hover) : null}
    </div>
  );
}
