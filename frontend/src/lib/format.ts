/** Formatadores brasileiros usados em todo o app. */

export const fmtBRL = (v: number): string => {
  if (v == null || Number.isNaN(v)) return "—";
  const abs = Math.abs(v);
  if (abs >= 1e9) return "R$ " + (v / 1e9).toFixed(1).replace(".", ",") + " bi";
  if (abs >= 1e6) return "R$ " + (v / 1e6).toFixed(0) + " mi";
  if (abs >= 1e3) return "R$ " + (v / 1e3).toFixed(0) + " mil";
  return "R$ " + v.toFixed(0);
};

export const fmtNum = (v: number, digits = 0): string =>
  v == null || Number.isNaN(v)
    ? "—"
    : v.toLocaleString("pt-BR", { maximumFractionDigits: digits, minimumFractionDigits: digits });

export const fmtPct = (v: number, d = 1): string => {
  if (v == null || Number.isNaN(v)) return "—";
  return (v >= 0 ? "+" : "") + v.toFixed(d).replace(".", ",") + "%";
};

export const fmtCompact = (v: number): string => {
  if (v == null || Number.isNaN(v)) return "—";
  const abs = Math.abs(v);
  if (abs >= 1e9) return (v / 1e9).toFixed(1).replace(".", ",") + " bi";
  if (abs >= 1e6) return (v / 1e6).toFixed(0) + " mi";
  if (abs >= 1e3) return (v / 1e3).toFixed(0) + "k";
  return v.toFixed(0);
};

export const fmtMes = (ano: number, mes: number): string => {
  const meses = ["jan","fev","mar","abr","mai","jun","jul","ago","set","out","nov","dez"];
  return `${meses[mes - 1]}/${String(ano).slice(2)}`;
};
