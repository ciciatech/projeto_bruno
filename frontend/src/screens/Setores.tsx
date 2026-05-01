import { useEffect, useMemo, useState } from "react";
import { Panel, EditorialNote } from "../components/Panel";
import { useFiltros } from "../components/FilterBar";
import { fmtCompact } from "../lib/format";
import { carregarPainel, type Painel } from "../lib/painel";
import { REGIOES_CE } from "../lib/regioes";

/**
 * Tela 3 — Composição de receitas públicas regionais.
 *
 * Pivot do escopo original (PIB municipal × CNAE) para o que de fato
 * temos no painel hoje: composição da receita pública municipal+federal+
 * social por região. Útil para mostrar heterogeneidade fiscal e dependência
 * de transferências, e como controle no modelo causal.
 */
export default function Setores() {
  const [painel, setPainel] = useState<Painel | null>(null);
  const [erro, setErro] = useState<string | null>(null);
  const [filtros] = useFiltros();

  useEffect(() => {
    carregarPainel().then(setPainel).catch((e) => setErro(String(e)));
  }, []);

  const dados = useMemo(() => (painel ? computar(painel, filtros.periodo) : null), [painel, filtros.periodo]);

  if (erro) return <ErrorBox msg={erro} />;
  if (!painel || !dados) return <Loading />;

  return (
    <div
      style={{
        gridArea: "main",
        display: "grid",
        gridTemplateColumns: "1fr 360px",
        gridTemplateRows: "auto 1fr",
        gap: 12,
        padding: 12,
        minHeight: 0,
        overflow: "hidden",
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
          Composição de receitas públicas regionais
        </h1>
        <span style={{ fontSize: 12, color: "var(--ink-3)" }}>
          {dados.periodoLabel} · {fmtCompact(dados.totalGeral)} R$ agregado
        </span>
        <div style={{ flex: 1 }} />
        <span className="mono" style={{ fontSize: 11, color: "var(--ink-4)" }}>
          tela 03/05
        </span>
      </div>

      <Panel
        padding={20}
        eyebrow="Stacked bars"
        title="Receitas por região (acumulado no período)"
        subtitle="Cada barra somada mostra o total recebido por região; cores indicam a fonte."
        footer={
          <>
            <span>Categorias: 7 fontes federais + sociais (BF, BPC) + transferências</span>
            <span className="mono">unidade: R$ — fonte: STN, MDS, Portal Transp.</span>
          </>
        }
      >
        <StackedBars dados={dados} />
      </Panel>

      <div className="flex flex-col gap-3 min-h-0 overflow-hidden">
        <Panel padding={14} eyebrow="Categorias" title="Composição agregada CE">
          <table style={{ width: "100%", fontSize: 12, borderCollapse: "collapse" }}>
            <tbody>
              {dados.categorias.map((c) => (
                <tr key={c.key} style={{ borderBottom: "1px solid var(--border-soft)" }}>
                  <td style={{ padding: "6px 0", verticalAlign: "middle" }}>
                    <span
                      style={{
                        display: "inline-block",
                        width: 10,
                        height: 10,
                        background: c.color,
                        marginRight: 8,
                        verticalAlign: "middle",
                      }}
                    />
                    <span style={{ color: "var(--ink-1)", fontWeight: 500 }}>{c.label}</span>
                  </td>
                  <td
                    className="mono"
                    style={{
                      padding: "6px 0",
                      textAlign: "right",
                      color: "var(--ink-1)",
                      fontWeight: 600,
                    }}
                  >
                    {fmtCompact(c.total)}
                  </td>
                  <td
                    className="mono"
                    style={{
                      padding: "6px 0 6px 12px",
                      textAlign: "right",
                      color: "var(--ink-3)",
                      width: 50,
                    }}
                  >
                    {((c.total / dados.totalGeral) * 100).toFixed(1)}%
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </Panel>

        <Panel padding={14} eyebrow="Heterogeneidade" title="Vocação fiscal regional">
          <p style={{ fontSize: 12, color: "var(--ink-2)", lineHeight: 1.55, margin: 0 }}>
            Regiões metropolitanas concentram FPM (cota fixa por habitante) e
            FUNDEB (educação básica). Sertão tem maior peso relativo de
            transferências sociais (BF e BPC) — refletindo desigualdade
            estrutural na arrecadação própria municipal.
          </p>
          <div className="mt-3">
            <EditorialNote>
              Esta tela substitui o escopo original "Setores produtivos"
              (PIB×CNAE) — não temos PIB municipal coletado e o IBGE só
              publica até 2022. A composição de receitas é a alternativa
              factível com dados disponíveis e mais útil como controle do
              modelo causal.
            </EditorialNote>
          </div>
        </Panel>
      </div>
    </div>
  );
}

const CATEGORIAS = [
  { key: "tf_fpm",       label: "FPM",                color: "var(--seq-3)" },
  { key: "tf_fundeb",    label: "FUNDEB",             color: "var(--seq-4)" },
  { key: "tf_royalties", label: "Royalties",          color: "var(--cat-4)" },
  { key: "tf_outros",    label: "Outras transf.",     color: "var(--cat-3)" },
  { key: "tf_itr",       label: "ITR",                color: "var(--cat-5)" },
  { key: "bf_v",         label: "Bolsa Família",      color: "var(--cat-1)" },
  { key: "bpc_v",        label: "BPC",                color: "var(--cat-2)" },
] as const;

type Categoria = (typeof CATEGORIAS)[number];

type DadosSetores = ReturnType<typeof computar>;

function StackedBars({ dados }: { dados: NonNullable<DadosSetores> }) {
  const W = 760;
  const H = 460;
  const PAD = { top: 16, right: 16, bottom: 70, left: 60 };
  const innerW = W - PAD.left - PAD.right;
  const innerH = H - PAD.top - PAD.bottom;
  const n = dados.regioes.length;
  const barW = (innerW / n) * 0.75;
  const gap = (innerW / n) * 0.25;

  return (
    <svg width="100%" height={H} viewBox={`0 0 ${W} ${H}`} style={{ display: "block" }}>
      {/* eixo Y */}
      <line
        x1={PAD.left}
        x2={PAD.left}
        y1={PAD.top}
        y2={PAD.top + innerH}
        stroke="var(--ink-3)"
      />
      <line
        x1={PAD.left}
        x2={PAD.left + innerW}
        y1={PAD.top + innerH}
        y2={PAD.top + innerH}
        stroke="var(--ink-3)"
      />

      {/* ticks Y */}
      {[0, 0.25, 0.5, 0.75, 1].map((t) => {
        const v = dados.maxRegiao * t;
        return (
          <g key={t}>
            <line
              x1={PAD.left - 4}
              x2={PAD.left}
              y1={PAD.top + innerH * (1 - t)}
              y2={PAD.top + innerH * (1 - t)}
              stroke="var(--ink-3)"
            />
            <text
              x={PAD.left - 8}
              y={PAD.top + innerH * (1 - t) + 3}
              textAnchor="end"
              className="mono"
              style={{ fontSize: 10, fill: "var(--ink-3)" }}
            >
              {fmtCompact(v)}
            </text>
            {t > 0 && (
              <line
                x1={PAD.left}
                x2={PAD.left + innerW}
                y1={PAD.top + innerH * (1 - t)}
                y2={PAD.top + innerH * (1 - t)}
                stroke="var(--ink-mute)"
                strokeWidth={0.4}
                strokeDasharray="2 3"
              />
            )}
          </g>
        );
      })}

      {/* barras */}
      {dados.regioes.map((r, i) => {
        const x = PAD.left + i * (barW + gap) + gap / 2;
        let acc = 0;
        return (
          <g key={r.codigo}>
            {CATEGORIAS.map((c) => {
              const v = r.por_cat[c.key] ?? 0;
              if (v <= 0) return null;
              const h = (v / dados.maxRegiao) * innerH;
              const y = PAD.top + innerH - acc - h;
              acc += h;
              return (
                <rect
                  key={c.key}
                  x={x}
                  y={y}
                  width={barW}
                  height={h}
                  fill={c.color}
                />
              );
            })}
            <text
              x={x + barW / 2}
              y={PAD.top + innerH + 14}
              textAnchor="middle"
              className="mono"
              style={{ fontSize: 9, fill: "var(--ink-2)" }}
            >
              {r.codigo}
            </text>
            <text
              x={x + barW / 2}
              y={PAD.top + innerH + 26}
              textAnchor="middle"
              style={{ fontSize: 8, fill: "var(--ink-3)" }}
            >
              {r.nome.split(" ").slice(0, 2).join(" ").slice(0, 14)}
            </text>
          </g>
        );
      })}

      {/* legend */}
      {CATEGORIAS.map((c, i) => {
        const x = PAD.left + (i % 4) * 170;
        const y = H - 14 + Math.floor(i / 4) * 12;
        return (
          <g key={c.key}>
            <rect x={x} y={y - 8} width={10} height={10} fill={c.color} />
            <text x={x + 14} y={y + 1} style={{ fontSize: 10, fill: "var(--ink-2)" }}>
              {c.label}
            </text>
          </g>
        );
      })}
    </svg>
  );
}

function Loading() {
  return (
    <div
      className="flex items-center justify-center"
      style={{ gridArea: "main", color: "var(--ink-3)", fontSize: 14 }}
    >
      Carregando composição de receitas…
    </div>
  );
}

function ErrorBox({ msg }: { msg: string }) {
  return (
    <div
      className="flex items-center justify-center"
      style={{ gridArea: "main", padding: 48 }}
    >
      <div style={{ maxWidth: 520, padding: 32, background: "var(--bg-surface)", border: "1px solid var(--signal-bad)" }}>
        <p style={{ color: "var(--signal-bad)", fontSize: 13 }}>{msg}</p>
      </div>
    </div>
  );
}

function computar(painel: Painel, periodo: string) {
  const todosAnos = Array.from(new Set(painel.rows.map((r) => r.y))).sort();
  const fim = todosAnos[todosAnos.length - 1] ?? 0;
  const inicio = periodo === "Tudo"
    ? todosAnos[0] ?? 0
    : Math.max(todosAnos[0] ?? 0, fim - (parseInt(periodo, 10) - 1));

  // soma por (regiao, categoria)
  const acc = new Map<string, Record<string, number>>();
  for (const row of painel.rows) {
    if (row.y < inicio || row.y > fim) continue;
    const cur = acc.get(row.r) ?? {};
    for (const c of CATEGORIAS) {
      const v = row[c.key];
      if (typeof v === "number") cur[c.key] = (cur[c.key] ?? 0) + v;
    }
    acc.set(row.r, cur);
  }

  const regioes = REGIOES_CE.map((r) => ({
    codigo: r.codigo,
    nome: r.nome,
    por_cat: acc.get(r.codigo) ?? {},
    total: Object.values(acc.get(r.codigo) ?? {}).reduce((a, b) => a + b, 0),
  })).sort((a, b) => b.total - a.total);

  const maxRegiao = Math.max(...regioes.map((r) => r.total), 1);

  // totais por categoria
  const categorias: (Categoria & { total: number })[] = CATEGORIAS.map((c) => {
    const total = regioes.reduce((s, r) => s + (r.por_cat[c.key] ?? 0), 0);
    return { ...c, total };
  })
    .filter((c) => c.total > 0)
    .sort((a, b) => b.total - a.total);

  const totalGeral = categorias.reduce((s, c) => s + c.total, 0);

  return {
    regioes,
    categorias,
    maxRegiao,
    totalGeral,
    periodoLabel: `${inicio}–${fim}`,
  };
}
