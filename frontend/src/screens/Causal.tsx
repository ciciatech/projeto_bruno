import { useEffect, useMemo, useState } from "react";
import * as ss from "simple-statistics";
import { Panel, EditorialNote } from "../components/Panel";
import { useFiltros, aplicarPeriodo } from "../lib/filtros";
import { fmtNum } from "../lib/format";
import { carregarPainel, type Painel } from "../lib/painel";
import { REGIOES_BY_CODIGO } from "../lib/regioes";

/**
 * Tela 4 — Relação causal · investimento estadual → emprego.
 *
 * Especificação econométrica preliminar (univariado):
 *
 *     Δ_emprego_{r,t} = α + β · invest_estadual_{r,t} + ε_{r,t}
 *
 * onde Δ_emprego = saldo CAGED (admissões − desligamentos) e
 * invest_estadual = SIOF empenhado replicado anual→mensal (migração para
 * frequência bimestral em curso). Aguarda validação do Prof. Paulo para
 * incluir controles regionais (BF, BPC, transferências, IBCR) e as séries
 * da letter do Prof. Paulo (SELIC, ICMS/cota-parte, transferências
 * estaduais SIOF Função 08/Subf. 241-244) como controles candidatos, e
 * tratar endogeneidade (lag, IV ou Arellano-Bond).
 *
 * Implementação: OLS via `simple-statistics` no cliente. Resíduos usados
 * para banda de confiança 95% (heurística σ = sd(resíduos)).
 */
export default function Causal() {
  const [painel, setPainel] = useState<Painel | null>(null);
  const [erro, setErro] = useState<string | null>(null);
  const [filtros] = useFiltros();
  const [destacado, setDestacado] = useState<string | null>(null);

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
      {/* HEADER */}
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
          Relação causal · investimento estadual → emprego
        </h1>
        <span style={{ fontSize: 12, color: "var(--ink-3)" }}>
          OLS univariado · n = {dados.n} obs · 14 regiões × meses
        </span>
        <div style={{ flex: 1 }} />
        <span className="mono" style={{ fontSize: 11, color: "var(--ink-4)" }}>
          tela 04/05 · especificação preliminar
        </span>
      </div>

      {/* SCATTER */}
      <Panel
        padding={20}
        eyebrow="Modelo"
        title="Saldo de empregos vs investimento estadual SIOF"
        subtitle="Cada ponto = uma região × mês. Linha laranja = OLS; banda = IC 95%."
        footer={
          <>
            <span>
              X: SIOF empenhado (R$ mi anual replicado) · Y: saldo CAGED (postos)
            </span>
            <span className="mono">
              R² = {dados.r2.toFixed(3)} · β = {dados.beta.toFixed(3)} · α = {dados.alpha.toFixed(0)}
            </span>
          </>
        }
      >
        <Scatter dados={dados} destacado={destacado} setDestacado={setDestacado} />
      </Panel>

      {/* DIREITA */}
      <div className="flex flex-col gap-3 min-h-0 overflow-hidden">
        <Panel padding={14} eyebrow="Resultados" title="Coeficientes OLS">
          <table style={{ width: "100%", fontSize: 12, borderCollapse: "collapse" }}>
            <tbody>
              <Row label="β (SIOF)" value={dados.beta.toFixed(3)} sub="postos por R$ mi de investimento" />
              <Row label="α (intercepto)" value={dados.alpha.toFixed(1)} sub="postos médios sem SIOF" />
              <Row label="R²" value={dados.r2.toFixed(4)} sub={dados.r2 > 0.05 ? "ajuste fraco-moderado" : "ajuste fraco"} />
              <Row label="σ resíduos" value={dados.sigma.toFixed(1)} sub="sd dos erros" />
              <Row label="n" value={fmtNum(dados.n)} sub="observações" />
            </tbody>
          </table>
        </Panel>

        <Panel padding={14} eyebrow="Diagnóstico" title="Heterogeneidade regional">
          <p style={{ fontSize: 12, color: "var(--ink-2)", lineHeight: 1.55, margin: 0 }}>
            β agregado de <strong>{dados.beta.toFixed(2)}</strong> postos por R$
            mi sugere uma associação{" "}
            {dados.beta > 0 ? "positiva" : "negativa"} entre investimento
            estadual e emprego no agregado. R² baixo ({(dados.r2 * 100).toFixed(1)}%)
            indica que controles regionais (transferências, BF, BPC, IBCR)
            explicam a maior parte da variação. Próximo passo: especificação
            multivariada e teste de Granger.
          </p>
          <div className="mt-3">
            <EditorialNote>
              Especificação econométrica final aguarda definição do Prof. Paulo:
              defasagens (lag), transformações (log, diff), tratamento de
              endogeneidade (IV ou Arellano-Bond) e variáveis de controle —
              incluindo as séries da letter do Prof. Paulo (SELIC,
              ICMS/cota-parte, transferências estaduais SIOF Função 08/Subf.
              241-244) como controles candidatos. Migração para frequência
              bimestral em curso. Os números acima devem ser lidos como{" "}
              <em>indicativos</em>, não como estimativa final.
            </EditorialNote>
          </div>
        </Panel>
      </div>
    </div>
  );
}

function Row({ label, value, sub }: { label: string; value: string; sub: string }) {
  return (
    <tr style={{ borderBottom: "1px solid var(--border-soft)" }}>
      <td style={{ padding: "8px 0", color: "var(--ink-2)", verticalAlign: "top" }}>
        {label}
        <div style={{ fontSize: 10.5, color: "var(--ink-4)", fontStyle: "italic" }}>{sub}</div>
      </td>
      <td
        className="mono"
        style={{
          padding: "8px 0",
          textAlign: "right",
          color: "var(--ink-1)",
          fontWeight: 600,
          fontSize: 14,
        }}
      >
        {value}
      </td>
    </tr>
  );
}

type DadosCausal = ReturnType<typeof computar>;

function Scatter({
  dados,
  destacado,
  setDestacado,
}: {
  dados: NonNullable<DadosCausal>;
  destacado: string | null;
  setDestacado: (r: string | null) => void;
}) {
  const W = 760;
  const H = 460;
  const PAD = { top: 16, right: 16, bottom: 40, left: 60 };
  const innerW = W - PAD.left - PAD.right;
  const innerH = H - PAD.top - PAD.bottom;

  const sx = (x: number) =>
    PAD.left + ((x - dados.xMin) / (dados.xMax - dados.xMin || 1)) * innerW;
  const sy = (y: number) =>
    PAD.top + (1 - (y - dados.yMin) / (dados.yMax - dados.yMin || 1)) * innerH;

  // Linha de regressão
  const x0 = dados.xMin;
  const x1 = dados.xMax;
  const y0 = dados.alpha + dados.beta * x0;
  const y1 = dados.alpha + dados.beta * x1;

  // Banda IC 95% (±1.96 σ)
  const yU0 = y0 + 1.96 * dados.sigma;
  const yU1 = y1 + 1.96 * dados.sigma;
  const yL0 = y0 - 1.96 * dados.sigma;
  const yL1 = y1 - 1.96 * dados.sigma;

  const xTicks = niceTicks(dados.xMin, dados.xMax, 5);
  const yTicks = niceTicks(dados.yMin, dados.yMax, 5);

  return (
    <svg width="100%" height={H} viewBox={`0 0 ${W} ${H}`} style={{ display: "block" }}>
      {/* Eixos */}
      {xTicks.map((t) => (
        <g key={`xt-${t}`}>
          <line
            x1={sx(t)}
            x2={sx(t)}
            y1={PAD.top + innerH}
            y2={PAD.top + innerH + 4}
            stroke="var(--ink-3)"
          />
          <text
            x={sx(t)}
            y={PAD.top + innerH + 18}
            textAnchor="middle"
            className="mono"
            style={{ fontSize: 10, fill: "var(--ink-3)" }}
          >
            {t.toFixed(0)}
          </text>
        </g>
      ))}
      {yTicks.map((t) => (
        <g key={`yt-${t}`}>
          <line
            x1={PAD.left - 4}
            x2={PAD.left}
            y1={sy(t)}
            y2={sy(t)}
            stroke="var(--ink-3)"
          />
          <text
            x={PAD.left - 8}
            y={sy(t) + 3}
            textAnchor="end"
            className="mono"
            style={{ fontSize: 10, fill: "var(--ink-3)" }}
          >
            {t.toFixed(0)}
          </text>
        </g>
      ))}
      <line
        x1={PAD.left}
        x2={PAD.left}
        y1={PAD.top}
        y2={PAD.top + innerH}
        stroke="var(--ink-3)"
        strokeWidth={1}
      />
      <line
        x1={PAD.left}
        x2={PAD.left + innerW}
        y1={PAD.top + innerH}
        y2={PAD.top + innerH}
        stroke="var(--ink-3)"
        strokeWidth={1}
      />

      {/* Linha y=0 */}
      {dados.yMin < 0 && dados.yMax > 0 && (
        <line
          x1={PAD.left}
          x2={PAD.left + innerW}
          y1={sy(0)}
          y2={sy(0)}
          stroke="var(--ink-mute)"
          strokeWidth={0.6}
          strokeDasharray="3 3"
        />
      )}

      {/* Banda IC */}
      <polygon
        points={`${sx(x0)},${sy(yU0)} ${sx(x1)},${sy(yU1)} ${sx(x1)},${sy(yL1)} ${sx(x0)},${sy(yL0)}`}
        fill="var(--seq-3)"
        opacity={0.12}
      />

      {/* Linha de regressão */}
      <line
        x1={sx(x0)}
        x2={sx(x1)}
        y1={sy(y0)}
        y2={sy(y1)}
        stroke="var(--seq-3)"
        strokeWidth={2}
      />

      {/* Pontos */}
      {dados.pontos.map((p, i) => {
        const cor = REGIOES_BY_CODIGO[p.r]?.zona === "metropolitana"
          ? "var(--cat-2)"
          : "var(--cat-1)";
        const isDest = destacado === p.r;
        return (
          <circle
            key={i}
            cx={sx(p.x)}
            cy={sy(p.y)}
            r={isDest ? 4 : 2.6}
            fill={cor}
            opacity={destacado && !isDest ? 0.2 : 0.6}
            onMouseEnter={() => setDestacado(p.r)}
            onMouseLeave={() => setDestacado(null)}
            style={{ cursor: "pointer" }}
          />
        );
      })}

      {/* Labels eixos */}
      <text
        x={PAD.left + innerW / 2}
        y={H - 6}
        textAnchor="middle"
        style={{ fontSize: 11, fill: "var(--ink-2)", fontWeight: 600 }}
      >
        SIOF empenhado (R$ mi)
      </text>
      <text
        x={14}
        y={PAD.top + innerH / 2}
        textAnchor="middle"
        transform={`rotate(-90 14 ${PAD.top + innerH / 2})`}
        style={{ fontSize: 11, fill: "var(--ink-2)", fontWeight: 600 }}
      >
        Saldo CAGED (postos)
      </text>

      {destacado && (
        <text
          x={W - PAD.right}
          y={PAD.top + 12}
          textAnchor="end"
          className="serif"
          style={{ fontSize: 13, fill: "var(--ink-1)", fontWeight: 600 }}
        >
          {REGIOES_BY_CODIGO[destacado]?.nome ?? destacado}
        </text>
      )}
    </svg>
  );
}

function niceTicks(min: number, max: number, n: number): number[] {
  if (min === max) return [min];
  const step = (max - min) / (n - 1);
  return Array.from({ length: n }, (_, i) => min + step * i);
}

function Loading() {
  return (
    <div
      className="flex items-center justify-center"
      style={{ gridArea: "main", color: "var(--ink-3)", fontSize: 14 }}
    >
      Carregando regressão causal…
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
  // Reusa lógica de aplicarPeriodo via cálculo direto (evita import circular)
  const fim = todosAnos[todosAnos.length - 1] ?? 0;
  const inicio = periodo === "Tudo"
    ? todosAnos[0] ?? 0
    : Math.max(todosAnos[0] ?? 0, fim - (parseInt(periodo, 10) - 1));

  const pontos: { r: string; y: number; x: number }[] = [];
  for (const row of painel.rows) {
    if (row.y < inicio || row.y > fim) continue;
    if (typeof row.siof_emp !== "number" || typeof row.sal !== "number") continue;
    pontos.push({ r: row.r, x: row.siof_emp, y: row.sal });
  }

  if (pontos.length < 2) {
    return {
      pontos: [],
      n: 0,
      beta: 0,
      alpha: 0,
      r2: 0,
      sigma: 0,
      xMin: 0,
      xMax: 1,
      yMin: 0,
      yMax: 1,
    };
  }

  const xs = pontos.map((p) => p.x);
  const ys = pontos.map((p) => p.y);
  const reg = ss.linearRegression(pontos.map((p) => [p.x, p.y]));
  const beta = reg.m;
  const alpha = reg.b;
  const fn = ss.linearRegressionLine(reg);
  const residuos = pontos.map((p) => p.y - fn(p.x));
  const sigma = ss.standardDeviation(residuos);
  const meanY = ss.mean(ys);
  const ssTotal = ys.reduce((s, y) => s + (y - meanY) ** 2, 0);
  const ssRes = residuos.reduce((s, r) => s + r * r, 0);
  const r2 = ssTotal > 0 ? 1 - ssRes / ssTotal : 0;

  return {
    pontos,
    n: pontos.length,
    beta,
    alpha,
    r2,
    sigma,
    xMin: Math.min(...xs),
    xMax: Math.max(...xs),
    yMin: Math.min(...ys),
    yMax: Math.max(...ys),
  };
}

void aplicarPeriodo; // alias importado mantido para futuras extensões
