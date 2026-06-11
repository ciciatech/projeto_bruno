import { useEffect, useMemo, useState } from "react";
import { Panel, EditorialNote } from "../components/Panel";
import { KPI } from "../components/KPI";
import { Choropleth14CE } from "../components/Choropleth14CE";
import { Sparkline } from "../components/Sparkline";
import { MapLegend } from "../components/MapLegend";
import { MapTooltip, type HoverInfo } from "../components/MapTooltip";
import { aplicarPeriodo, useFiltros } from "../lib/filtros";
import {
  ANO_SIOF_ESQUEMA_ANTIGO,
  ANO_SIOF_REGIONAL_INICIO,
  temCoberturaSiofRegional,
} from "../lib/sioCobertura";
import { fmtBRL, fmtBRLFull, fmtCompact, fmtNum, fmtMes } from "../lib/format";
import {
  carregarPainel,
  snapshotPorRegiao,
  somarAnualDedup,
  type Painel,
} from "../lib/painel";
import { REGIOES_BY_CODIGO, REGIOES_CE } from "../lib/regioes";

export default function Investimento() {
  const [painel, setPainel] = useState<Painel | null>(null);
  const [erro, setErro] = useState<string | null>(null);
  const [selecionado, setSelecionado] = useState<string>("03"); // Grande Fortaleza
  const [hover, setHover] = useState<HoverInfo | null>(null);
  const [filtros, setFiltros] = useFiltros();

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
    return computar(filtrados, filtros.recorte);
  }, [painel, filtros.periodo, filtros.recorte]);

  if (erro) return <ErrorBox msg={erro} />;
  if (!painel || !dados) return <Loading />;

  const regSel = dados.snapshot.get(selecionado);
  const nomeSel = REGIOES_BY_CODIGO[selecionado]?.nome ?? selecionado;

  const renderTooltip = (info: HoverInfo) => (
    <>
      <div
        className="mono"
        style={{ fontSize: 16, fontWeight: 600, color: "var(--ink-1)", marginTop: 2 }}
      >
        {info.value != null ? fmtBRLFull(info.value) : "sem cobertura da fonte"}
      </div>
      <div style={{ fontSize: 11, color: "var(--ink-3)", marginTop: 6, lineHeight: 1.5 }}>
        {info.value != null ? (
          <>
            SIOF empenhado · {dados.periodoLabel}
            <br />
            clique para fixar como referência
          </>
        ) : (
          <>
            SEPLAG-CE não publica SIOF regional
            <br />
            para esta região no período selecionado.
          </>
        )}
      </div>
    </>
  );

  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "minmax(260px, 320px) minmax(0, 1fr) minmax(260px, 320px)",
        gridTemplateRows: "auto 1fr",
        gap: 12,
        padding: 12,
        gridArea: "main",
        minHeight: 0,
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
          Investimento estadual em obras e equipamentos · 14 regiões CE
        </h1>
        <span style={{ fontSize: 12, color: "var(--ink-3)" }} title={fmtBRLFull(dados.totalSiof)}>
          {dados.periodoLabel} · {fmtBRL(dados.totalSiof)} agregado
        </span>
        <div style={{ flex: 1 }} />
        <span className="mono" style={{ fontSize: 11, color: "var(--ink-4)" }}>
          tela 01/05
        </span>
      </div>

      {/* KPIs */}
      <div className="flex flex-col gap-3 min-h-0">
        <Panel padding={14} eyebrow="Síntese" title="Investimento agregado CE">
          <div className="flex flex-col gap-3">
            <KPI
              label="Investimento estadual SIOF"
              value={dados.totalSiof > 0 ? fmtCompact(dados.totalSiof) : "—"}
              unit={dados.totalSiof > 0 ? "R$ (empenhado)" : "sem dado regional"}
              sparkData={dados.totalSiof > 0 ? dados.serieSiof : undefined}
              sub={
                dados.totalSiof > 0
                  ? `${dados.anosComSiof} anos com dado · ${dados.regioesAtivas} regiões`
                  : `${ANO_SIOF_ESQUEMA_ANTIGO} em tratamento (T47) · série regional ${ANO_SIOF_REGIONAL_INICIO}+`
              }
            />
            {dados.split.total > 0 && (
              <div aria-label="Quebra do SIOF pago entre obras e equipamentos">
                <div
                  style={{
                    fontSize: 10.5,
                    color: "var(--ink-3)",
                    textTransform: "uppercase",
                    letterSpacing: 0.8,
                    fontWeight: 600,
                    marginBottom: 6,
                  }}
                >
                  Obras × equipamentos · SIOF pago
                </div>
                <div
                  style={{
                    display: "flex",
                    height: 12,
                    border: "1px solid var(--border-soft)",
                    marginBottom: 6,
                  }}
                >
                  <div
                    title={`Obras: ${fmtBRLFull(dados.split.obras)}`}
                    style={{
                      width: `${(dados.split.obras / dados.split.total) * 100}%`,
                      background: "var(--seq-4)",
                    }}
                  />
                  <div
                    title={`Equipamentos: ${fmtBRLFull(dados.split.equip)}`}
                    style={{
                      width: `${(dados.split.equip / dados.split.total) * 100}%`,
                      background: "var(--cat-2)",
                    }}
                  />
                </div>
                <div className="flex flex-col" style={{ gap: 3, fontSize: 11 }}>
                  {[
                    { label: "Obras e instalações", v: dados.split.obras, cor: "var(--seq-4)" },
                    { label: "Equip. e mat. permanente", v: dados.split.equip, cor: "var(--cat-2)" },
                  ].map((s) => (
                    <div
                      key={s.label}
                      className="grid items-center"
                      style={{ gridTemplateColumns: "8px 1fr auto auto", gap: 8 }}
                    >
                      <span style={{ width: 8, height: 8, background: s.cor }} />
                      <span style={{ color: "var(--ink-2)" }}>{s.label}</span>
                      <span
                        className="mono"
                        title={fmtBRLFull(s.v)}
                        style={{ color: "var(--ink-1)", fontWeight: 600 }}
                      >
                        {fmtCompact(s.v)}
                      </span>
                      <span
                        className="mono"
                        style={{ color: "var(--ink-3)", width: 34, textAlign: "right" }}
                      >
                        {((s.v / dados.split.total) * 100).toFixed(0)}%
                      </span>
                    </div>
                  ))}
                </div>
                {dados.split.anosSemSplit > 0 && (
                  <div
                    className="mono"
                    style={{ fontSize: 10, color: "var(--ink-4)", marginTop: 4 }}
                  >
                    split publicado até {dados.split.anoMax} (rel. 544) — {dados.split.anoMax + 1}+
                    ainda sem quebra por natureza
                  </div>
                )}
              </div>
            )}
            <KPI
              label="Investimento total CE estimado"
              value={fmtCompact(dados.totalInvTotal * 1e6)}
              unit="R$ 2010"
              sparkData={dados.serieInvTotal}
              sparkColor="var(--cat-2)"
              sub={`FBCF Brasil × share PIB CE/BR (~${(dados.shareMedio * 100).toFixed(1)}%)`}
            />
            <KPI
              label="IBCR-CE último ponto"
              value={dados.ibcrUltimo != null ? dados.ibcrUltimo.toFixed(1) : "—"}
              unit={`pts · ${dados.ibcrLabelMes}`}
              delta={dados.ibcrYoY}
              sparkData={dados.serieIbcr}
              sparkColor="var(--cat-2)"
              sub="ajuste sazonal · BACEN SGS 25380"
            />
          </div>
          <div className="mt-3">
            <EditorialNote>
              A série regional do SIOF agora cobre {ANO_SIOF_REGIONAL_INICIO}–2026
              (relatório 544 retroativo + extração corrente), validada contra a
              planilha do Prof. Paulo. {ANO_SIOF_ESQUEMA_ANTIGO} usa a divisão
              regional antiga (8 macrorregiões) e está em tratamento (T47).
              Bases monetárias ainda mistas (SIOF em R$ correntes, FBCF em R$
              2010) — harmonização para R$ dez/25 na migração ao painel
              bimestral.
            </EditorialNote>
          </div>
        </Panel>

        <Panel padding={14} eyebrow="Top 5" title="Concentração regional · SIOF">
          <div className="flex flex-col gap-2">
            {dados.topRegioes.map((r) => {
              const pct = r.valor / dados.totalSiof;
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
                        background: "var(--seq-3)",
                        height: "100%",
                        width: `${pct * 100}%`,
                      }}
                    />
                  </div>
                  <span
                    className="mono"
                    title={fmtBRLFull(r.valor)}
                    style={{ fontSize: 11, fontWeight: 600, color: "var(--ink-1)", width: 70, textAlign: "right" }}
                  >
                    {fmtCompact(r.valor)}
                  </span>
                </div>
              );
            })}
          </div>
          <div className="mt-3">
            <EditorialNote>
              {dados.totalSiof > 0 ? (
                <>
                  {dados.topRegioes[0]?.nome} concentra{" "}
                  {(((dados.topRegioes[0]?.valor ?? 0) / dados.totalSiof) * 100).toFixed(0)}%
                  do investimento estadual empenhado no período — padrão típico de
                  projetos de infraestrutura em região metropolitana.
                </>
              ) : (
                <>
                  Sem dado regional do SIOF para este período.{" "}
                  {ANO_SIOF_ESQUEMA_ANTIGO} usa divisão regional antiga — em
                  tratamento (T47); a série regional cobre{" "}
                  {ANO_SIOF_REGIONAL_INICIO} em diante.
                </>
              )}
            </EditorialNote>
          </div>
        </Panel>
      </div>

      {/* MAPA */}
      <Panel
        padding={16}
        eyebrow="Mapa coroplético · 14 regiões"
        title={`SIOF empenhado · ${dados.periodoLabel}`}
        subtitle="Clique numa região para fixar como referência"
        footer={
          <>
            <span>Escala sequencial · valores em R$ correntes</span>
            <span className="mono">fonte: SEPLAG-CE / SIOF · rel. 544 + ano corrente</span>
          </>
        }
      >
        <div className="flex gap-6 items-start h-full">
          <div className="flex-1 flex justify-center items-center" style={{ position: "relative" }}>
            <Choropleth14CE
              values={dados.mapaSiof}
              selected={selecionado}
              onSelect={setSelecionado}
              onHover={setHover}
              width={460}
              height={368}
              tile={72}
              gap={4}
            />
            {/* Overlay de cobertura: com a série retroativa do rel. 544 (2016+)
                ele só acende quando a janela NÃO alcança 2016 — hoje, somente
                2015 (esquema antigo de 8 macrorregiões) ou painel vazio. */}
            {dados.totalSiof === 0 && (
              <div
                role="status"
                style={{
                  position: "absolute",
                  inset: 0,
                  display: "flex",
                  flexDirection: "column",
                  justifyContent: "center",
                  alignItems: "center",
                  background: "color-mix(in oklab, var(--bg-surface) 92%, transparent)",
                  padding: 24,
                  textAlign: "center",
                  gap: 12,
                  pointerEvents: "auto",
                }}
              >
                <div
                  style={{
                    fontSize: 11,
                    color: "var(--ink-3)",
                    textTransform: "uppercase",
                    letterSpacing: 0.8,
                    fontWeight: 600,
                  }}
                >
                  Sem detalhamento regional
                </div>
                <div
                  className="serif"
                  style={{
                    fontSize: 16,
                    color: "var(--ink-1)",
                    maxWidth: 360,
                    lineHeight: 1.4,
                  }}
                >
                  {dados.inicioPeriodo != null &&
                  dados.fimPeriodo != null &&
                  !temCoberturaSiofRegional(dados.inicioPeriodo, dados.fimPeriodo) ? (
                    <>
                      <strong>{ANO_SIOF_ESQUEMA_ANTIGO}</strong> usa divisão
                      regional antiga (8 macrorregiões) — em tratamento (T47).
                      A série nas 14 regiões cobre{" "}
                      <strong>{ANO_SIOF_REGIONAL_INICIO}</strong> em diante
                      (relatório 544 + extração corrente).
                    </>
                  ) : (
                    <>
                      Nenhum dado regional do SIOF foi carregado para este
                      período — o painel pode estar sendo regenerado.
                    </>
                  )}
                </div>
                <button
                  onClick={() => setFiltros({ periodo: "Tudo" })}
                  style={{
                    marginTop: 4,
                    padding: "8px 16px",
                    background: "var(--ink-1)",
                    color: "var(--bg-surface)",
                    border: "none",
                    fontSize: 12,
                    fontWeight: 600,
                    fontFamily: "var(--font-sans)",
                    cursor: "pointer",
                    letterSpacing: 0.4,
                  }}
                >
                  Ver série {ANO_SIOF_REGIONAL_INICIO}+ →
                </button>
                <div
                  className="mono"
                  style={{ fontSize: 10, color: "var(--ink-3)", marginTop: 4 }}
                >
                  fonte: SEPLAG-CE / SIOF-Web
                </div>
              </div>
            )}
          </div>

          <aside className="flex flex-col gap-4 pt-2" style={{ width: 180, flexShrink: 0 }}>
            <MapLegend
              scale="seq"
              min={dados.mapaMin}
              max={dados.mapaMax}
              label="R$ correntes · SIOF empenhado"
              format={fmtCompact}
            />

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
                {nomeSel}
              </div>
              <div
                className="mono"
                title={
                  dados.mapaSiof[selecionado] != null
                    ? fmtBRLFull(dados.mapaSiof[selecionado] as number)
                    : "sem cobertura da fonte no período"
                }
                style={{ fontSize: 22, fontWeight: 600, color: "var(--ink-1)", marginTop: 2 }}
              >
                {dados.mapaSiof[selecionado] != null
                  ? fmtBRL(dados.mapaSiof[selecionado] as number)
                  : "—"}
              </div>
              <div
                className="mono"
                style={{ fontSize: 11, color: "var(--ink-3)", fontWeight: 600 }}
              >
                SIOF acumulado · {dados.periodoLabel}
              </div>
              <div
                style={{ fontSize: 11, color: "var(--ink-3)", marginTop: 6, lineHeight: 1.5 }}
              >
                {regSel?.siof_n
                  ? `${Number(regSel.siof_n).toFixed(0)} ações no SIOF · último ano`
                  : "Sem ações SIOF no período."}
              </div>
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
                Por zona
              </div>
              <div className="flex flex-col" style={{ gap: 4, fontSize: 11 }}>
                {dados.porZona.map((z) => (
                  <div
                    key={z.zona}
                    className="grid items-center"
                    style={{ gridTemplateColumns: "8px 1fr auto", gap: 8 }}
                  >
                    <span style={{ width: 8, height: 8, background: z.cor }} />
                    <span style={{ color: "var(--ink-2)", textTransform: "capitalize" }}>
                      {z.zona}
                    </span>
                    <span
                      className="mono"
                      title={fmtBRLFull(z.valor)}
                      style={{ color: "var(--ink-1)", fontWeight: 600 }}
                    >
                      {fmtCompact(z.valor)}
                    </span>
                  </div>
                ))}
              </div>
            </div>

            <div style={{ borderTop: "1px solid var(--border-soft)", paddingTop: 10 }}>
              <div
                className="mono"
                style={{ fontSize: 10, color: "var(--ink-4)", lineHeight: 1.5 }}
              >
                Painel atualizado em
                <br />
                <span style={{ color: "var(--ink-2)", fontSize: 11 }}>
                  {dados.atualizado}
                </span>
              </div>
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
                  {["Cód.", "Região", "SIOF", "Ações", "IBCR"].map((h, i) => (
                    <th
                      key={i}
                      style={{
                        padding: "6px 8px",
                        textAlign: i === 0 ? "left" : i >= 2 ? "right" : "left",
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
                      title={
                        r.siof != null
                          ? fmtBRLFull(r.siof)
                          : "sem cobertura da fonte (SIOF regional cobre 2016+)"
                      }
                      style={{ padding: "6px 8px", textAlign: "right", color: "var(--ink-1)" }}
                    >
                      {r.siof != null ? fmtCompact(r.siof) : "—"}
                    </td>
                    <td
                      className="mono"
                      title={
                        r.acoes != null
                          ? undefined
                          : "nº de ações publicado apenas na extração do ano corrente"
                      }
                      style={{ padding: "6px 8px", textAlign: "right", color: "var(--ink-2)" }}
                    >
                      {r.acoes != null ? fmtNum(r.acoes) : "—"}
                    </td>
                    <td style={{ padding: "6px 8px", textAlign: "right" }}>
                      <Sparkline data={r.ibcrSerie} width={48} height={14} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>

        <Panel padding={14} eyebrow="Decomposição" title="Composição · investimento total CE">
          <div className="flex flex-col gap-2">
            {dados.composicao.map((c) => (
              <div
                key={c.label}
                className="grid items-center gap-2"
                style={{ gridTemplateColumns: "130px 1fr 80px", fontSize: 12 }}
              >
                <span style={{ color: "var(--ink-2)" }}>{c.label}</span>
                <div
                  style={{
                    background: "var(--bg-sunken)",
                    height: 12,
                    position: "relative",
                  }}
                >
                  <div
                    style={{
                      background: c.color,
                      height: "100%",
                      width: `${c.share * 100}%`,
                    }}
                  />
                </div>
                <span
                  className="mono"
                  style={{
                    textAlign: "right",
                    color: c.disponivel ? "var(--ink-1)" : "var(--ink-4)",
                    fontWeight: 600,
                  }}
                >
                  {c.disponivel ? `${(c.share * 100).toFixed(0)}%` : "pendente"}
                </span>
              </div>
            ))}
          </div>
          <div className="mt-3">
            <EditorialNote>
              SIOF cobre apenas o investimento estadual empenhado. RREO federal
              e SICONFI municipal já foram coletados; a composição usa o
              invest. municipal EMPENHADO — a substituição por PAGO aguarda
              confirmação do Prof. Paulo (recoleta ~13 mil requests).
            </EditorialNote>
          </div>
        </Panel>
      </div>
      <MapTooltip hover={hover}>{renderTooltip}</MapTooltip>
    </div>
  );
}

function Loading() {
  return (
    <div
      className="flex items-center justify-center"
      style={{ gridArea: "main", color: "var(--ink-3)", fontSize: 14 }}
    >
      Carregando painel regional CE…
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
        <h2
          className="serif"
          style={{
            margin: 0,
            fontSize: 20,
            fontWeight: 600,
            color: "var(--ink-1)",
            letterSpacing: -0.3,
          }}
        >
          Não conseguimos abrir o painel regional CE.
        </h2>
        <p
          style={{
            marginTop: 8,
            fontSize: 13,
            lineHeight: 1.6,
            color: "var(--ink-2)",
          }}
        >
          O arquivo de dados pode estar sendo regenerado neste momento — tente
          recarregar em alguns segundos. Se o problema persistir, abra o console
          do navegador para ver detalhes técnicos.
        </p>
        <pre
          className="mono"
          style={{
            marginTop: 12,
            padding: 10,
            background: "var(--bg-sunken)",
            color: "var(--ink-3)",
            fontSize: 11,
            lineHeight: 1.5,
            whiteSpace: "pre-wrap",
            overflow: "auto",
            maxHeight: 120,
          }}
        >
          {msg}
        </pre>
        <button
          onClick={() => window.location.reload()}
          style={{
            marginTop: 16,
            padding: "8px 16px",
            background: "var(--ink-1)",
            color: "var(--bg-surface)",
            border: "1px solid var(--ink-1)",
            borderRadius: "var(--radius-sm)",
            fontSize: 13,
            fontWeight: 600,
            cursor: "pointer",
          }}
        >
          Recarregar
        </button>
      </div>
    </div>
  );
}

function computar(painel: Painel, recorte: "Bruto" | "Per capita" | "% PIB" = "Bruto") {
  const rows = painel.rows;
  // Pop CE total: pega último ano disponível e soma todas as regiões
  const popPorRegiaoAno = new Map<string, number>();
  for (const r of rows) {
    if (typeof r.pop === "number") {
      const k = `${r.r}-${r.y}`;
      if (!popPorRegiaoAno.has(k)) popPorRegiaoAno.set(k, r.pop);
    }
  }
  // pop por região (último ano disponível)
  const popPorRegiao = new Map<string, number>();
  popPorRegiaoAno.forEach((v, k) => {
    const cod = k.split("-")[0];
    const cur = popPorRegiao.get(cod) ?? 0;
    if (v > cur) popPorRegiao.set(cod, v);
  });
  const popTotalCE = Array.from(popPorRegiao.values()).reduce((a, b) => a + b, 0);
  const isPerCap = recorte === "Per capita";
  const fator = isPerCap && popTotalCE > 0 ? 1000 / popTotalCE : 1; // R$/hab × mil = milhar per capita
  void fator;
  const snapshot = snapshotPorRegiao(rows);

  // SIOF (valores em R$ correntes no painel.json): o painel replica o valor
  // ANUAL em cada mês, então somarAnualDedup conta cada (região, ano) 1×.
  const siofAgg = somarAnualDedup(rows, "siof_emp");
  const siofAcum = siofAgg.porRegiao;
  const acoesAcum = somarAnualDedup(rows, "siof_n").porRegiao;
  const totalSiof = siofAgg.total;

  const mapaSiof: Record<string, number | null> = {};
  siofAcum.forEach((v, cod) => (mapaSiof[cod] = v));

  // série SIOF (soma anual)
  const serieSiof = Array.from(siofAgg.porAno.entries())
    .sort((a, b) => a[0] - b[0])
    .map((x) => x[1]);
  const anosComSiof = siofAgg.porAno.size;

  // Split obras × equipamentos do SIOF PAGO (relatório 544, 2016–2025).
  // O ano corrente (2026) publica o pago agregado mas ainda sem o split —
  // sinalizamos isso na UI quando a janela o inclui.
  const obrasAgg = somarAnualDedup(rows, "siof_obras");
  const equipAgg = somarAnualDedup(rows, "siof_equip");
  const pagoAgg = somarAnualDedup(rows, "siof_pago");
  const split = {
    obras: obrasAgg.total,
    equip: equipAgg.total,
    total: obrasAgg.total + equipAgg.total,
    anoMax: obrasAgg.porAno.size
      ? Math.max(...Array.from(obrasAgg.porAno.keys()))
      : 0,
    anosSemSplit: Math.max(0, pagoAgg.porAno.size - obrasAgg.porAno.size),
  };

  // invest_total CE (anual) — soma de uma região (já é estadual replicado)
  const ultimaInvTotal = new Map<number, number>();
  for (const r of rows) {
    if (r.r === "03" && typeof r.inv_tot === "number") {
      ultimaInvTotal.set(r.y, (ultimaInvTotal.get(r.y) ?? 0) + r.inv_tot);
    }
  }
  const totalInvTotal = Array.from(ultimaInvTotal.values()).reduce((a, b) => a + b, 0);
  const serieInvTotal = Array.from(ultimaInvTotal.entries())
    .sort((a, b) => a[0] - b[0])
    .map((x) => x[1]);

  // Share médio
  const shares: number[] = [];
  for (const r of rows) {
    if (typeof r.share === "number") shares.push(r.share);
  }
  const shareMedio = shares.reduce((a, b) => a + b, 0) / Math.max(1, shares.length);

  // IBCR (estadual replicado): pegar único por mês
  const ibcrPorMes = new Map<string, number>();
  for (const r of rows) {
    const k = `${r.y}-${String(r.m).padStart(2, "0")}`;
    if (!ibcrPorMes.has(k) && typeof r.ibcr === "number") {
      ibcrPorMes.set(k, r.ibcr);
    }
  }
  const ibcrEntries = Array.from(ibcrPorMes.entries()).sort();
  const serieIbcr = ibcrEntries.map(([, v]) => v).slice(-24);
  const ultimoIbcr = ibcrEntries.at(-1);
  const ibcrUltimo = ultimoIbcr ? ultimoIbcr[1] : null;
  const ibcrLabelMes = ultimoIbcr
    ? fmtMes(Number(ultimoIbcr[0].slice(0, 4)), Number(ultimoIbcr[0].slice(5)))
    : "—";
  const idx12mAtras = ibcrEntries.length - 13;
  const ibcrYoY =
    idx12mAtras >= 0 && ultimoIbcr
      ? ((ultimoIbcr[1] - ibcrEntries[idx12mAtras][1]) / ibcrEntries[idx12mAtras][1]) * 100
      : undefined;

  // Top regiões
  const topRegioes = Array.from(siofAcum.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([codigo, valor]) => ({
      codigo,
      valor,
      nome: REGIOES_BY_CODIGO[codigo]?.nome ?? codigo,
    }));

  // Tabela 14 regiões: snapshot + sparkline IBCR (mesmo IBCR pra todas, só visual)
  const tabela = Array.from(snapshot.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([codigo]) => {
      const ibcrSerie = serieIbcr.length ? serieIbcr.slice(-12) : [0, 0];
      return {
        codigo,
        nome: REGIOES_BY_CODIGO[codigo]?.nome ?? codigo,
        siof: siofAcum.get(codigo) ?? null,
        acoes: acoesAcum.get(codigo) ?? null,
        ibcrSerie,
      };
    });

  // Composição das 4 esferas + privado residual
  // Metodologia documentada em docs/metodologia-composicao-investimento.md
  // (transcrição dos áudios do Prof. Paulo, abr/2026):
  //   inv_total_ce ≈ FBCF Brasil × 2.2% (share PIB CE/BR)
  //   inv_privado  = inv_total_ce - inv_estadual - inv_federal - inv_municipal
  //
  // ATENÇÃO: bases monetárias ainda mistas (SIOF e municipal em R$ correntes,
  // FBCF em R$ 2010, federal em R$ correntes). Cálculo abaixo é didático;
  // refinamento monetário usa o deflator IPCA base dez/25
  // (pipeline/transform/deflator.py, T28; base parametrizável, dez/24
  // disponível se o Paulo preferir) e entra com a migração ao painel
  // bimestral, após confirmação final do Paulo.
  // Federal e InvTotal são estaduais (idênticos em todas as 14 regiões). Para
  // não somar 14× o mesmo valor, pegamos uma região-âncora ('03' Grande
  // Fortaleza) como representativa por mês. Municipal sim é regional —
  // somamos normal.
  let totalIfFedRep = 0; // R$ correntes, mensal, estadual
  let totalInvMun = 0;
  for (const r of rows) {
    if (r.r === "03" && typeof r.if_total === "number") totalIfFedRep += r.if_total;
    if (typeof r.inv_mun === "number") totalInvMun += r.inv_mun;
  }
  const totalEstadual = totalSiof / 1e6;       // R$ correntes → R$ mi
  const totalFederal = totalIfFedRep / 1e6;    // R$ correntes → R$ mi
  const totalMunicipal = totalInvMun / 1e6;    // R$ correntes → R$ mi (SICONFI)
  const totalPrivadoEstimado = Math.max(
    0,
    totalInvTotal - totalEstadual - totalFederal - totalMunicipal,
  );
  const totalComposicao =
    totalEstadual + totalFederal + totalMunicipal + totalPrivadoEstimado || 1;
  const composicao = [
    {
      label: "Estadual (SIOF)",
      color: "var(--seq-3)",
      share: totalEstadual / totalComposicao,
      disponivel: totalEstadual > 0,
    },
    {
      label: "Federal (RREO)",
      color: "var(--cat-2)",
      share: totalFederal / totalComposicao,
      disponivel: totalFederal > 0,
    },
    {
      label: "Municipal (SICONFI)",
      color: "var(--cat-1)",
      share: totalMunicipal / totalComposicao,
      disponivel: totalMunicipal > 0,
    },
    {
      label: "Privado (residual)",
      color: "var(--cat-5)",
      share: totalPrivadoEstimado / totalComposicao,
      disponivel: totalInvTotal > 0,
    },
  ];

  // periodo
  const anos = Array.from(new Set(rows.map((r) => r.y))).sort((a, b) => a - b);
  const periodoLabel = anos.length
    ? `${anos[0]}–${anos[anos.length - 1]}`
    : "—";
  const inicioPeriodo = anos.length ? anos[0] : null;
  const fimPeriodo = anos.length ? anos[anos.length - 1] : null;

  // regiões com dado
  const regioesAtivas = Array.from(siofAcum.values()).filter((v) => v > 0).length;

  // min/max do mapa para a legenda de cores
  const mapaValores = Object.values(mapaSiof).filter((v): v is number => typeof v === "number");
  const mapaMin = mapaValores.length ? Math.min(...mapaValores) : 0;
  const mapaMax = mapaValores.length ? Math.max(...mapaValores) : 1;

  // Soma SIOF agregada por ZONA (litoral/metropolitana/sertao/cariri) para o aside.
  // Substitui o "Por região" do design original (Sudeste/Sul/...) com o nosso
  // recorte intra-CE.
  const zonaCor: Record<string, string> = {
    metropolitana: "var(--cat-2)",
    litoral: "var(--cat-3)",
    sertao: "var(--cat-1)",
    cariri: "var(--cat-5)",
  };
  const zonaTotal = new Map<string, number>();
  for (const [cod, valor] of siofAcum.entries()) {
    const zona = REGIOES_CE.find((r) => r.codigo === cod)?.zona ?? "sertao";
    zonaTotal.set(zona, (zonaTotal.get(zona) ?? 0) + valor);
  }
  const porZona = Array.from(zonaTotal.entries())
    .map(([zona, valor]) => ({ zona, valor, cor: zonaCor[zona] ?? "var(--cat-3)" }))
    .sort((a, b) => b.valor - a.valor);

  // atualizado
  const atualizado = painel.meta.atualizado_em
    ? new Date(painel.meta.atualizado_em).toLocaleString("pt-BR", {
        timeZone: "America/Fortaleza",
      })
    : "—";

  return {
    snapshot,
    mapaSiof,
    mapaMin,
    mapaMax,
    porZona,
    totalSiof,
    serieSiof,
    totalInvTotal,
    serieInvTotal,
    shareMedio,
    ibcrUltimo,
    ibcrLabelMes,
    serieIbcr,
    ibcrYoY,
    topRegioes,
    tabela,
    composicao,
    periodoLabel,
    inicioPeriodo,
    fimPeriodo,
    anosComSiof,
    split,
    regioesAtivas,
    atualizado,
  };
}
