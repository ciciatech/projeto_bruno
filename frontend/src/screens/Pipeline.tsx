import { useEffect, useState } from "react";
import { Panel } from "../components/Panel";
import { carregarPainel } from "../lib/painel";

type Fonte = {
  fonte: string;
  cobertura: number;
  cadencia: string;
  status: "ok" | "warn" | "bad" | "info";
  delta: string;
};

const FONTES_BASE: Fonte[] = [
  { fonte: "BACEN SGS · IBCR-CE",                  cobertura: 100, cadencia: "Mensal",    status: "ok",   delta: "série SGS 25380" },
  { fonte: "STN · Transferências constitucionais", cobertura: 100, cadencia: "Mensal",    status: "ok",   delta: "FPM/FUNDEB/ITR/royalties" },
  { fonte: "SIOF-CE · Obras e instalações",        cobertura: 100, cadencia: "Anual",     status: "ok",   delta: "14 regiões × ano" },
  { fonte: "IpeaData FBCF · proxy invest. total",  cobertura: 100, cadencia: "Mensal",    status: "ok",   delta: "× share PIB CE/BR" },
  { fonte: "Bolsa Família · Portal Transp.",       cobertura: 100, cadencia: "Mensal",    status: "ok",   delta: "Onda 2 ✓ (4min33s)" },
  { fonte: "BPC · Portal Transp.",                 cobertura: 100, cadencia: "Mensal",    status: "ok",   delta: "Onda 2 ✓" },
  { fonte: "Invest. federal · RREO",               cobertura: 100, cadencia: "Bimestral", status: "ok",   delta: "Onda 2 ✓ · 132 obs" },
  { fonte: "CAGED · MTE (mensal municipal)",       cobertura:  10, cadencia: "Mensal",    status: "warn", delta: "18/184 munic. cobertos" },
  { fonte: "Invest. municipal · SICONFI RREO",     cobertura:   0, cadencia: "Bimestral", status: "info", delta: "código pronto · executar manual" },
  { fonte: "ESTBAN BNB",                           cobertura:   0, cadencia: "Mensal",    status: "bad",  delta: "BCB removeu URL pública" },
  { fonte: "SEFAZ-CE · cota-parte ICMS/IPVA (via SICONFI Anexo 03)", cobertura: 0, cadencia: "Mensal", status: "info", delta: "código pronto · executar manual" },
];

const TONE: Record<Fonte["status"], { bg: string; fg: string; label: string }> = {
  ok:   { bg: "rgba(31,111,106,0.12)",  fg: "var(--signal-good)", label: "OK" },
  warn: { bg: "rgba(180,100,20,0.12)",  fg: "var(--signal-warn)", label: "RODANDO" },
  bad:  { bg: "rgba(140,42,28,0.12)",   fg: "var(--signal-bad)",  label: "BLOQUEADO" },
  info: { bg: "rgba(42,64,96,0.10)",    fg: "var(--signal-info)", label: "PENDENTE" },
};

export default function Pipeline() {
  const [meta, setMeta] = useState<{
    atualizado_em: string | null;
    linhas: number;
    regioes: number;
  } | null>(null);

  useEffect(() => {
    carregarPainel().then((p) =>
      setMeta({
        atualizado_em: p.meta.atualizado_em,
        linhas: p.meta.linhas,
        regioes: p.meta.regioes.length,
      }),
    );
  }, []);

  return (
    <div
      style={{
        gridArea: "main",
        display: "grid",
        gridTemplateColumns: "320px 1fr",
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
          Pipeline e qualidade de dados
        </h1>
        <span style={{ fontSize: 12, color: "var(--ink-3)" }}>
          {meta ? `${meta.linhas.toLocaleString("pt-BR")} linhas · ${meta.regioes} regiões` : "carregando…"}
        </span>
        <div style={{ flex: 1 }} />
        <span className="mono" style={{ fontSize: 11, color: "var(--ink-4)" }}>
          tela 05/05
        </span>
      </div>

      <div className="flex flex-col gap-3">
        <Panel padding={14} eyebrow="Saúde" title="Painel atualizado">
          <div className="flex flex-col gap-2 text-sm" style={{ color: "var(--ink-2)" }}>
            <div>
              <span className="mono" style={{ color: "var(--ink-3)" }}>fonte:</span>{" "}
              <span className="mono">painel_regional_ce_mensal.csv</span>
            </div>
            <div>
              <span className="mono" style={{ color: "var(--ink-3)" }}>build:</span>{" "}
              <span className="mono">{meta?.atualizado_em ?? "—"}</span>
            </div>
            <div>
              <span className="mono" style={{ color: "var(--ink-3)" }}>cobertura:</span>{" "}
              <span className="mono">14 regiões × {meta ? Math.round(meta.linhas / 14) : "—"} meses</span>
            </div>
          </div>
        </Panel>

        <Panel padding={14} eyebrow="Decisões pendentes" title="Aguardando Prof. Paulo">
          <ul
            style={{
              margin: 0,
              paddingLeft: 16,
              fontSize: 12,
              color: "var(--ink-2)",
              lineHeight: 1.7,
            }}
          >
            <li>Fonte do "investimento total privado" para residual: FBCF nacional × share PIB ✓ (proxy)</li>
            <li>Ano-base do residual: <strong>2024/2025</strong> ✓</li>
            <li>CAGED municipal (~24h FTP): <strong>vale o custo</strong> ✓</li>
            <li>Investimento municipal: <strong>SICONFI</strong> ✓</li>
          </ul>
        </Panel>
      </div>

      <Panel padding={0} eyebrow="Coletas" title={`${FONTES_BASE.length} fontes do painel regional CE`}>
        <div className="overflow-auto">
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead>
              <tr style={{ position: "sticky", top: 0, background: "var(--bg-surface)" }}>
                {["Fonte", "Cadência", "Cobertura", "Status", "Δ"].map((h, i) => (
                  <th
                    key={h}
                    style={{
                      padding: "8px 12px",
                      textAlign: i === 2 ? "right" : "left",
                      fontSize: 10,
                      color: "var(--ink-3)",
                      textTransform: "uppercase",
                      letterSpacing: 0.6,
                      fontWeight: 600,
                      borderBottom: "1px solid var(--rule)",
                      whiteSpace: "nowrap",
                    }}
                  >
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {FONTES_BASE.map((f) => {
                const t = TONE[f.status];
                return (
                  <tr key={f.fonte} style={{ borderBottom: "1px solid var(--border-soft)" }}>
                    <td style={{ padding: "8px 12px", color: "var(--ink-1)" }}>{f.fonte}</td>
                    <td style={{ padding: "8px 12px", color: "var(--ink-2)" }}>{f.cadencia}</td>
                    <td className="mono" style={{ padding: "8px 12px", textAlign: "right", color: "var(--ink-1)", fontWeight: 600 }}>
                      {f.cobertura}%
                    </td>
                    <td style={{ padding: "8px 12px" }}>
                      <span
                        className="mono"
                        style={{
                          background: t.bg,
                          color: t.fg,
                          fontSize: 10.5,
                          fontWeight: 600,
                          padding: "2px 6px",
                          borderRadius: "var(--radius-sm)",
                          textTransform: "uppercase",
                          letterSpacing: 0.5,
                        }}
                      >
                        {t.label}
                      </span>
                    </td>
                    <td style={{ padding: "8px 12px", color: "var(--ink-2)", fontSize: 11 }}>
                      {f.delta}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </Panel>
    </div>
  );
}
