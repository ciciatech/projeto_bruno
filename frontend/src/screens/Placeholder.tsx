import type { ReactNode } from "react";
import { Panel, EditorialNote } from "../components/Panel";

type Props = {
  numero: 2 | 3 | 4 | 5;
  titulo: string;
  resumo: string;
  bullets: string[];
  fontes: string;
  visual?: ReactNode;
};

export default function Placeholder({ numero, titulo, resumo, bullets, fontes, visual }: Props) {
  return (
    <div
      style={{
        gridArea: "main",
        display: "grid",
        gridTemplateColumns: "1fr 1fr",
        gap: 12,
        padding: 12,
        gridTemplateRows: "auto 1fr",
      }}
    >
      <div
        style={{
          gridColumn: "1 / -1",
          display: "flex",
          alignItems: "baseline",
          gap: 16,
          padding: "4px 4px 0",
        }}
      >
        <h1
          className="serif"
          style={{ margin: 0, fontSize: 28, fontWeight: 600, letterSpacing: -0.5 }}
        >
          {titulo}
        </h1>
        <span style={{ fontSize: 12, color: "var(--ink-3)" }}>{resumo}</span>
        <div style={{ flex: 1 }} />
        <span className="mono" style={{ fontSize: 11, color: "var(--ink-4)" }}>
          tela 0{numero}/05 · em construção
        </span>
      </div>

      <Panel padding={20} eyebrow="Roadmap" title="O que entra nesta tela">
        <ul
          style={{
            margin: 0,
            paddingLeft: 18,
            color: "var(--ink-2)",
            fontSize: 13,
            lineHeight: 1.7,
          }}
        >
          {bullets.map((b) => (
            <li key={b}>{b}</li>
          ))}
        </ul>
        <div className="mt-4">
          <EditorialNote>
            Estrutura visual e layout já estão padronizados. Os dados são
            ligados conforme as coletas no Mac Mini avançam — pipeline e
            front-end ficam desacoplados via <span className="mono">painel.json</span>.
          </EditorialNote>
        </div>
      </Panel>

      <Panel padding={20} eyebrow="Dependências" title="Coletas que alimentam esta tela">
        <div style={{ fontSize: 12, color: "var(--ink-2)", lineHeight: 1.7 }}>
          {fontes}
        </div>
        {visual && <div className="mt-4">{visual}</div>}
      </Panel>
    </div>
  );
}
