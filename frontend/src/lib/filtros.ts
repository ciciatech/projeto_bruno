/**
 * Estado global de filtros (período/recorte) sincronizado com a URL.
 *
 * Vivia em components/FilterBar.tsx; movido para cá porque o plugin
 * react-refresh exige que arquivos de componente exportem apenas
 * componentes (constantes/hooks compartilhados quebram o fast refresh).
 */
import { useSearchParams } from "react-router-dom";

export type Periodo = "1A" | "3A" | "5A" | "10A" | "Tudo";
export type Recorte = "Bruto" | "Per capita" | "% PIB";

export const PERIODOS: { key: Periodo; label: string }[] = [
  { key: "1A", label: "1A" },
  { key: "3A", label: "3A" },
  { key: "5A", label: "5A" },
  { key: "10A", label: "10A" },
  { key: "Tudo", label: "Tudo" },
];

export const RECORTES: { key: Recorte; label: string; disabled?: boolean }[] = [
  { key: "Bruto", label: "Bruto" },
  { key: "Per capita", label: "Per capita" },
  { key: "% PIB", label: "% PIB", disabled: true },
];

export type Filtros = {
  periodo: Periodo;
  recorte: Recorte;
};

export const DEFAULT_FILTROS: Filtros = {
  periodo: "Tudo",
  recorte: "Bruto",
};

export function useFiltros(): [Filtros, (f: Partial<Filtros>) => void] {
  const [params, setParams] = useSearchParams();
  const filtros: Filtros = {
    periodo: (params.get("periodo") as Periodo) || DEFAULT_FILTROS.periodo,
    recorte: (params.get("recorte") as Recorte) || DEFAULT_FILTROS.recorte,
  };
  const setFiltros = (patch: Partial<Filtros>) => {
    const next = new URLSearchParams(params);
    Object.entries(patch).forEach(([k, v]) => {
      if (v == null || v === DEFAULT_FILTROS[k as keyof Filtros]) {
        next.delete(k);
      } else {
        next.set(k, String(v));
      }
    });
    setParams(next, { replace: true });
  };
  return [filtros, setFiltros];
}

/**
 * Aplica o recorte de período sobre a lista de anos disponíveis no painel.
 * Retorna o ano-início (inclusivo) e ano-fim (inclusivo).
 */
export function aplicarPeriodo(
  todosAnos: number[],
  periodo: Periodo,
): { inicio: number; fim: number } {
  if (todosAnos.length === 0) return { inicio: 0, fim: 0 };
  const ordenados = [...todosAnos].sort((a, b) => a - b);
  const fim = ordenados[ordenados.length - 1];
  const inicio = ordenados[0];
  if (periodo === "Tudo") return { inicio, fim };
  const n = parseInt(periodo, 10) - 1;
  return { inicio: Math.max(inicio, fim - n), fim };
}
