import { useEffect, useMemo, useState } from "react";
import { Panel, EditorialNote } from "../components/Panel";
import { KPI } from "../components/KPI";
import { Choropleth14CE } from "../components/Choropleth14CE";
import { Sparkline } from "../components/Sparkline";
import { aplicarPeriodo, useFiltros } from "../components/FilterBar";
import { fmtNum, fmtPct, fmtMes, fmtBRL } from "../lib/format";
import { carregarPainel, type Painel } from "../lib/painel";
import { REGIOES_BY_CODIGO, REGIOES_CE } from "../lib/regioes";

export default function Emprego() {
  const [painel, setPainel] = useState<Painel | null>(null);
  const [erro, setErro] = useState<string | null>(null);
  const [selecionado, setSelecionado] = useState<string>("03");
  const [filtros] = useFiltros();

  useEffect(() => {
    carregarPainel().then(setPainel).catch((e) => setErro(String(e)));
  }, []);

  const dados = useMemo(() => {
    if (!painel) return null;
    const todosAnos = Array.from(new Set(painel.rows.map((r) => r.y))).sort();
    const { inicio, fim } = aplicarPeriodo(todosAnos, filtros.periodo);
    const filtrados: Painel = {
      meta: painel.meta,
      rows: painel.rows.filter((r) => r.y >= inicio && r.y <= fim),
    };
    return computar(filtrados);
  }, [painel, filtros.periodo]);

  if (erro) return <ErrorBox msg={erro} />;
  if (!painel || !dados) return <Loading />;

  const regSelInfo = REGIOES_BY_CODIGO[selecionado];
  const regSelStats = dados.porRegiao.get(selecionado);

  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "320px 1fr 320px",
        gridTemplateRows: "auto 1fr",
        gap: 12,
        padding: 12,
        gridArea: "main",
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
          Dinâmica do emprego formal · 14 regiões CE
        </h1>
        <span style={{ fontSize: 12, color: "var(--ink-3)" }}>
          {dados.periodoLabel} · saldo agregado{" "}
          {fmtNum(dados.saldoTotal)} postos · cobertura {dados.regioesComDado}/14 regiões
        </span>
        <div style={{ flex: 1 }} />
        <span className="mono" style={{ fontSize: 11, color: "var(--ink-4)" }}>
          tela 02/05 · CAGED parcial
        </span>
      </div>

      {/* KPIs */}
      <div className="flex flex-col gap-3 min-h-0">
        <Panel padding={14} eyebrow="Síntese" title="Saldo CAGED · CE">
          <div className="flex flex-col gap-3">
            <KPI
              label="Saldo último ano"
              value={fmtNum(dados.saldoUltimoAno)}
              unit={`postos · ${dados.ultimoAno}`}
              delta={dados.saldoYoYPct}
              sparkData={dados.serieSaldoMensal}
              sparkColor={
                dados.serieSaldoMensal[dados.serieSaldoMensal.length - 1] >= 0
                  ? "var(--signal-good)"
                  : "var(--signal-bad)"
              }
              sub={`vs ${dados.ultimoAno - 1} · ${dados.serieSaldoMensal.length} meses na série`}
            />
            <KPI
              label="Admissões acumuladas"
              value={fmtNum(dados.admTotal)}
              unit="postos"
              sub={`média mensal: ${fmtNum(Math.round(dados.admMediaMensal))}`}
            />
            <KPI
              label="Desligamentos acumulados"
              value={fmtNum(dados.desTotal)}
              unit="postos"
              sub={`média mensal: ${fmtNum(Math.round(dados.desMediaMensal))}`}
            />
            <KPI
              label="Salário médio ponderado"
              value={dados.salMedio != null ? fmtBRL(dados.salMedio) : "—"}
              unit="por posto"
              sub="média ponderada por movimentações"
            />
          </div>
        </Panel>

        <Panel padding={14} eyebrow="Top variação" title="5 regiões com maior saldo">
          <div className="flex flex-col gap-2">
            {dados.topRegioes.map((r) => {
              const max = Math.max(...dados.topRegioes.map((x) => Math.abs(x.saldo))) || 1;
              const pct = Math.abs(r.saldo) / max;
              const positivo = r.saldo >= 0;
              return (
                <div key={r.codigo} className="flex items-center gap-2">
                  <span
                    className="mono"
                    style={{ fontSize: 11, color: "var(--ink-4)", width: 22 }}
                  >
                    {r.codigo}
                  </span>
                  <span style={{ flex: 1, fontSize: 12, color: "var(--ink-2)" }}>
                    {r.nome}
                  </span>
                  <div
                    style={{
                      flex: 1.2,
                      background: "var(--bg-sunken)",
                      height: 12,
                      position: "relative",
                    }}
                  >
                    <div
                      style={{
                        background: positivo ? "var(--signal-good)" : "var(--signal-bad)",
                        height: "100%",
                        width: `${pct * 100}%`,
                      }}
                    />
                  </div>
                  <span
                    className="mono"
                    style={{
                      fontSize: 11,
                      fontWeight: 600,
                      color: positivo ? "var(--signal-good)" : "var(--signal-bad)",
                      width: 70,
                      textAlign: "right",
                    }}
                  >
                    {positivo ? "+" : ""}
                    {fmtNum(r.saldo)}
                  </span>
                </div>
              );
            })}
          </div>
        </Panel>
      </div>

      {/* MAPA divergente */}
      <Panel
        padding={16}
        eyebrow="Mapa divergente · saldo acumulado"
        title="Variação líquida do estoque de empregos formais"
        subtitle="Cores frias = perda; quentes = ganho. Clique para fixar."
        footer={
          <>
            <span>
              Escala divergente · valores em postos de trabalho · 9 regiões com
              dado de {dados.ultimoAno}
            </span>
            <span className="mono">fonte: CAGED/MTE · municipal mensal</span>
          </>
        }
      >
        <div className="flex gap-6 items-start h-full">
          <div className="flex-1 flex justify-center items-center">
            <Choropleth14CE
              values={dados.mapaSaldo}
              scale="div"
              selected={selecionado}
              onSelect={setSelecionado}
              width={560}
              height={520}
              tile={92}
              gap={6}
            />
          </div>

          <aside className="flex flex-col gap-4 pt-2" style={{ width: 200 }}>
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
                Selecionado · {selecionado}
              </div>
              <div
                className="serif"
                style={{ fontSize: 18, fontWeight: 600, color: "var(--ink-1)", letterSpacing: -0.3 }}
              >
                {regSelInfo?.nome ?? selecionado}
              </div>
              {regSelStats ? (
                <>
                  <div
                    className="mono"
                    style={{
                      fontSize: 22,
                      fontWeight: 600,
                      color: regSelStats.saldo >= 0 ? "var(--signal-good)" : "var(--signal-bad)",
                      marginTop: 2,
                    }}
                  >
                    {regSelStats.saldo >= 0 ? "+" : ""}
                    {fmtNum(regSelStats.saldo)}
                  </div>
                  <div className="mono" style={{ fontSize: 11, color: "var(--ink-3)", fontWeight: 600 }}>
                    saldo acumulado
                  </div>
                  <div style={{ fontSize: 11, color: "var(--ink-3)", marginTop: 6, lineHeight: 1.5 }}>
                    Admissões: {fmtNum(regSelStats.adm)} · Desligamentos: {fmtNum(regSelStats.des)}
                    <br />
                    Salário médio: {regSelStats.salMed != null ? fmtBRL(regSelStats.salMed) : "—"}
                  </div>
                </>
              ) : (
                <div style={{ fontSize: 12, color: "var(--ink-4)", marginTop: 6, fontStyle: "italic" }}>
                  Sem dado CAGED para esta região no painel atual.
                </div>
              )}
            </div>

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
                Estatuto da coleta
              </div>
              <ul
                className="text-xs"
                style={{ color: "var(--ink-2)", margin: 0, paddingLeft: 14, lineHeight: 1.6 }}
              >
                <li>CAGED via FTP MTE: ✓ rodou</li>
                <li>Cobertura: {dados.regioesComDado}/14 regiões</li>
                <li>Salário médio: já calculado</li>
                <li>RAIS anual: pendente</li>
              </ul>
            </div>
          </aside>
        </div>
      </Panel>

      {/* DIREITA */}
      <div className="flex flex-col gap-3 min-h-0 overflow-hidden">
        <Panel padding={0} eyebrow="Drill-down" title="Snapshot · 14 regiões">
          <div className="overflow-auto" style={{ maxHeight: 360 }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
              <thead>
                <tr style={{ position: "sticky", top: 0, background: "var(--bg-surface)" }}>
                  {["Cód.", "Região", "Saldo", "Adm.", "Sal. méd."].map((h, i) => (
                    <th
                      key={h}
                      style={{
                        padding: "6px 8px",
                        textAlign: i <= 1 ? "left" : "right",
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
                {dados.tabela.map((r) => (
                  <tr
                    key={r.codigo}
                    onClick={() => setSelecionado(r.codigo)}
                    style={{
                      borderBottom: "1px solid var(--border-soft)",
                      background: r.codigo === selecionado ? "var(--bg-sunken)" : "transparent",
                      cursor: "pointer",
                    }}
                  >
                    <td className="mono" style={{ padding: "6px 8px", color: "var(--ink-3)" }}>
                      {r.codigo}
                    </td>
                    <td style={{ padding: "6px 8px", color: "var(--ink-1)" }}>{r.nome}</td>
                    <td
                      className="mono"
                      style={{
                        padding: "6px 8px",
                        textAlign: "right",
                        color:
                          r.saldo == null
                            ? "var(--ink-4)"
                            : r.saldo >= 0
                            ? "var(--signal-good)"
                            : "var(--signal-bad)",
                        fontWeight: 600,
                      }}
                    >
                      {r.saldo == null ? "—" : `${r.saldo >= 0 ? "+" : ""}${fmtNum(r.saldo)}`}
                    </td>
                    <td
                      className="mono"
                      style={{ padding: "6px 8px", textAlign: "right", color: "var(--ink-2)" }}
                    >
                      {r.adm != null ? fmtNum(r.adm) : "—"}
                    </td>
                    <td
                      className="mono"
                      style={{ padding: "6px 8px", textAlign: "right", color: "var(--ink-1)" }}
                    >
                      {r.salMed != null ? `R$ ${fmtNum(Math.round(r.salMed))}` : "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>

        <Panel padding={14} eyebrow="Série mensal" title="Saldo agregado CE · 24 meses">
          <Sparkline
            data={dados.serieSaldoMensal}
            color="var(--seq-3)"
            width={290}
            height={70}
          />
          <div
            className="mono"
            style={{ fontSize: 11, color: "var(--ink-3)", marginTop: 8, lineHeight: 1.6 }}
          >
            min: {fmtNum(Math.min(...dados.serieSaldoMensal))} ·{" "}
            max: {fmtNum(Math.max(...dados.serieSaldoMensal))}
          </div>
          <div className="mt-3">
            <EditorialNote>
              Cobertura CAGED ainda parcial (9/14 regiões). 5 regiões — Cariri,
              Litoral Leste, Litoral Oeste, Sertão de Canindé, Sertão dos
              Inhamuns — não aparecem no recorte municipal usado pelo coletor.
              Próxima coleta deve ampliar para os 184 municípios.
            </EditorialNote>
          </div>
        </Panel>
      </div>
    </div>
  );
}

function Loading() {
  return (
    <div
      className="flex items-center justify-center"
      style={{ gridArea: "main", color: "var(--ink-3)", fontSize: 14 }}
    >
      Carregando dinâmica de emprego CE…
    </div>
  );
}

function ErrorBox({ msg }: { msg: string }) {
  return (
    <div
      className="flex items-center justify-center"
      style={{ gridArea: "main", padding: 48 }}
    >
      <div
        style={{
          maxWidth: 520,
          padding: 32,
          background: "var(--bg-surface)",
          border: "1px solid var(--border-soft)",
          borderLeft: "3px solid var(--signal-bad)",
        }}
      >
        <div
          className="mono"
          style={{
            fontSize: 10.5,
            color: "var(--signal-bad)",
            textTransform: "uppercase",
            letterSpacing: 0.8,
            fontWeight: 600,
            marginBottom: 8,
          }}
        >
          Falha ao carregar painel
        </div>
        <p style={{ fontSize: 13, lineHeight: 1.6, color: "var(--ink-2)" }}>
          {msg}
        </p>
      </div>
    </div>
  );
}

type RegStat = {
  codigo: string;
  nome: string;
  adm: number;
  des: number;
  saldo: number;
  mov: number;
  salMed: number | null;
};

function computar(painel: Painel) {
  const rows = painel.rows;

  // Acumular por região
  const acc = new Map<string, { adm: number; des: number; sal: number; mov: number; salPesoTotal: number }>();
  for (const r of rows) {
    if (typeof r.adm !== "number") continue;
    const cur = acc.get(r.r) ?? { adm: 0, des: 0, sal: 0, mov: 0, salPesoTotal: 0 };
    cur.adm += r.adm ?? 0;
    cur.des += r.des ?? 0;
    cur.sal += r.sal ?? 0;
    cur.mov += r.mov ?? 0;
    if (typeof r.sal_med === "number" && typeof r.mov === "number") {
      cur.salPesoTotal += r.sal_med * r.mov;
    }
    acc.set(r.r, cur);
  }

  const porRegiao = new Map<string, RegStat>();
  acc.forEach((v, codigo) => {
    porRegiao.set(codigo, {
      codigo,
      nome: REGIOES_BY_CODIGO[codigo]?.nome ?? codigo,
      adm: v.adm,
      des: v.des,
      saldo: v.sal,
      mov: v.mov,
      salMed: v.mov > 0 ? v.salPesoTotal / v.mov : null,
    });
  });

  // Mapa: saldo escalonado pra div (-10..+10 representa pct relativo do max)
  const saldos = Array.from(porRegiao.values()).map((s) => s.saldo);
  const maxAbs = Math.max(...saldos.map(Math.abs), 1);
  const mapaSaldo: Record<string, number | null> = {};
  // Inicializa todas as 14 regiões; as sem dado ficam null (cinza)
  REGIOES_CE.forEach((r) => {
    const stat = porRegiao.get(r.codigo);
    mapaSaldo[r.codigo] = stat ? (stat.saldo / maxAbs) * 10 : null;
  });

  // KPIs agregados
  const saldoTotal = saldos.reduce((a, b) => a + b, 0);
  const admTotal = Array.from(porRegiao.values()).reduce((a, b) => a + b.adm, 0);
  const desTotal = Array.from(porRegiao.values()).reduce((a, b) => a + b.des, 0);
  const movTotal = Array.from(porRegiao.values()).reduce((a, b) => a + b.mov, 0);
  const salPesoSomado = rows.reduce((acc2, r) => {
    if (typeof r.sal_med === "number" && typeof r.mov === "number") {
      return acc2 + r.sal_med * r.mov;
    }
    return acc2;
  }, 0);
  const salMedio = movTotal > 0 ? salPesoSomado / movTotal : null;

  // YoY: último ano vs anterior
  const saldoPorAno = new Map<number, number>();
  for (const r of rows) {
    if (typeof r.sal === "number") {
      saldoPorAno.set(r.y, (saldoPorAno.get(r.y) ?? 0) + r.sal);
    }
  }
  const anosOrdenados = Array.from(saldoPorAno.keys()).sort();
  const ultimoAno = anosOrdenados.at(-1) ?? 0;
  const anoAnterior = anosOrdenados.at(-2) ?? 0;
  const saldoUltimoAno = saldoPorAno.get(ultimoAno) ?? 0;
  const saldoAnoAnterior = saldoPorAno.get(anoAnterior) ?? 0;
  const saldoYoYPct =
    saldoAnoAnterior !== 0
      ? ((saldoUltimoAno - saldoAnoAnterior) / Math.abs(saldoAnoAnterior)) * 100
      : 0;

  // Série mensal (últimos 24 meses)
  const mensalCE = new Map<string, number>();
  for (const r of rows) {
    if (typeof r.sal === "number") {
      const key = `${r.y}-${String(r.m).padStart(2, "0")}`;
      mensalCE.set(key, (mensalCE.get(key) ?? 0) + r.sal);
    }
  }
  const serieSaldoMensal = Array.from(mensalCE.entries())
    .sort()
    .slice(-24)
    .map(([, v]) => v);

  // Top 5 regiões por |saldo|
  const topRegioes = Array.from(porRegiao.values())
    .sort((a, b) => Math.abs(b.saldo) - Math.abs(a.saldo))
    .slice(0, 5)
    .map((s) => ({ codigo: s.codigo, nome: s.nome, saldo: s.saldo }));

  // Tabela: 14 regiões (com null pra ausentes)
  const tabela = REGIOES_CE.map((r) => {
    const s = porRegiao.get(r.codigo);
    return {
      codigo: r.codigo,
      nome: r.nome,
      saldo: s?.saldo ?? null,
      adm: s?.adm ?? null,
      salMed: s?.salMed ?? null,
    };
  }).sort((a, b) => {
    // ordenar por |saldo| desc, com null no final
    if (a.saldo == null && b.saldo == null) return a.codigo.localeCompare(b.codigo);
    if (a.saldo == null) return 1;
    if (b.saldo == null) return -1;
    return Math.abs(b.saldo) - Math.abs(a.saldo);
  });

  // periodoLabel (do painel.meta)
  const periodoLabel = painel.meta.periodo
    ? `${painel.meta.periodo.inicio.slice(0, 4)}–${painel.meta.periodo.fim.slice(0, 4)}`
    : "—";

  // labels úteis (fmtMes pode ser usado se quiser eixo)
  void fmtMes;
  void fmtPct;

  return {
    porRegiao,
    mapaSaldo,
    saldoTotal,
    saldoUltimoAno,
    saldoYoYPct,
    ultimoAno,
    serieSaldoMensal,
    admTotal,
    desTotal,
    admMediaMensal: admTotal / Math.max(1, mensalCE.size),
    desMediaMensal: desTotal / Math.max(1, mensalCE.size),
    salMedio,
    topRegioes,
    tabela,
    regioesComDado: porRegiao.size,
    periodoLabel,
  };
}
