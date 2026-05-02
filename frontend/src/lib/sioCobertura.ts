/**
 * SIOF-CE: cobertura regional (14 regiões SEPLAG/IPECE).
 *
 * A SEPLAG-CE só publica detalhamento por região a partir de 2026.
 * Anos anteriores (2015–2025) só têm o nível "secretaria" no SIOF, sem
 * desagregação regional. Esse ponto é crítico porque o exercício causal
 * da tese (impacto do investimento estadual em obras → emprego nas 14
 * regiões) depende dessa série regional.
 *
 * Quando o frontend detecta período sem dado regional, mostra um overlay
 * editorial em vez de um mapa todo da mesma cor (que dá falsa sensação
 * de dado homogêneo).
 *
 * Fontes:
 *   - dados_nordeste/raw/execucao_orcamentaria/ce/siof_obras_consolidado.csv
 *   - pipeline/transform/etl.py::processar_siof_obras
 *   - docs/metodologia-composicao-investimento.md
 */
export const ANO_SIOF_REGIONAL_INICIO = 2026;
