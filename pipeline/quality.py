"""
Auditoria de qualidade dos dados públicos coletados pelo pipeline.

Objetivo:
  - Verificar cobertura temporal e territorial
  - Identificar nulos, duplicidades e arquivos ausentes
  - Gerar relatórios reutilizáveis para a tese

Uso:
  python -m pipeline.quality
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.config import BASE_DIR, ESTADOS_NE, PROCESSED_DIR, QUALITY_DIR, RAW_DIR


EXPECTED_UFS = sorted(ESTADOS_NE.keys())


@dataclass
class SourceAuditConfig:
    """Configuração de auditoria por dataset."""

    nome: str
    path: Path
    camada: str
    descricao: str
    frequencia_esperada: str
    expected_years: list[int] | None = None
    expected_ufs: list[str] | None = None
    key_columns: list[str] = field(default_factory=list)
    critical_columns: list[str] = field(default_factory=list)
    optional: bool = False


class DataQualityAuditor:
    """Executa auditoria padronizada sobre datasets do projeto."""

    QUALITY_DIR = QUALITY_DIR

    SOURCES = [
        SourceAuditConfig(
            nome="bacen_raw_wide",
            path=RAW_DIR / "bacen" / "nacional" / "bacen_sgs_wide.csv",
            camada="raw",
            descricao="Séries mensais do BACEN em formato wide.",
            frequencia_esperada="mensal",
            expected_years=list(range(2015, 2026)),
            critical_columns=["data", "credito_PF_nordeste", "credito_PJ_nordeste", "IBCR_NE_ajuste_sazonal", "ipca_mensal"],
            key_columns=["data"],
        ),
        SourceAuditConfig(
            nome="caged_antigo_raw",
            path=RAW_DIR / "caged" / "nordeste" / "caged_antigo_saldo_mensal.csv",
            camada="raw",
            descricao="Saldo mensal do CAGED antigo por UF.",
            frequencia_esperada="mensal",
            expected_years=list(range(2015, 2020)),
            expected_ufs=EXPECTED_UFS,
            key_columns=["ano", "mes", "sigla_uf"],
            critical_columns=["ano", "mes", "sigla_uf", "saldo"],
        ),
        SourceAuditConfig(
            nome="caged_novo_raw",
            path=RAW_DIR / "caged" / "nordeste" / "caged_saldo_mensal.csv",
            camada="raw",
            descricao="Saldo mensal do Novo CAGED por UF.",
            frequencia_esperada="mensal",
            expected_years=list(range(2020, 2026)),
            expected_ufs=EXPECTED_UFS,
            key_columns=["ano", "mes", "sigla_uf"],
            critical_columns=["ano", "mes", "sigla_uf", "saldo"],
        ),
        SourceAuditConfig(
            nome="rais_vinculos_raw",
            path=RAW_DIR / "rais" / "nordeste" / "rais_vinculos.csv",
            camada="raw",
            descricao="Estoque anual de vínculos formais por UF.",
            frequencia_esperada="anual",
            expected_years=list(range(2015, 2023)),
            expected_ufs=EXPECTED_UFS,
            key_columns=["ano", "sigla_uf"],
            critical_columns=["ano", "sigla_uf", "vinculos_ativos", "remuneracao_media"],
        ),
        SourceAuditConfig(
            nome="siconfi_rreo_raw",
            path=RAW_DIR / "siconfi" / "nordeste" / "siconfi_rreo_nordeste.csv",
            camada="raw",
            descricao="Extrato bruto do RREO para os 9 estados.",
            frequencia_esperada="bimestral",
            expected_years=list(range(2015, 2026)),
            expected_ufs=EXPECTED_UFS,
            critical_columns=["exercicio", "periodo", "uf", "cod_conta", "valor"],
        ),
        SourceAuditConfig(
            nome="siconfi_rgf_raw",
            path=RAW_DIR / "siconfi" / "nordeste" / "siconfi_rgf_nordeste.csv",
            camada="raw",
            descricao="Extrato bruto do RGF para os 9 estados.",
            frequencia_esperada="quadrimestral",
            expected_years=list(range(2015, 2026)),
            expected_ufs=EXPECTED_UFS,
            critical_columns=["exercicio", "periodo", "uf", "cod_conta", "valor"],
        ),
        SourceAuditConfig(
            nome="siconfi_dca_raw",
            path=RAW_DIR / "siconfi" / "nordeste" / "siconfi_dca_nordeste.csv",
            camada="raw",
            descricao="Extrato bruto da DCA para os 9 estados.",
            frequencia_esperada="anual",
            expected_years=list(range(2015, 2026)),
            expected_ufs=EXPECTED_UFS,
            critical_columns=["exercicio", "uf", "cod_conta", "valor"],
        ),
        SourceAuditConfig(
            nome="transferencias_raw",
            path=RAW_DIR / "transferencias" / "nordeste" / "transferencias_constitucionais_nordeste.csv",
            camada="raw",
            descricao="Transferências constitucionais filtradas do RREO.",
            frequencia_esperada="bimestral",
            expected_years=list(range(2015, 2026)),
            expected_ufs=EXPECTED_UFS,
            critical_columns=["exercicio", "periodo", "uf", "valor"],
        ),
        SourceAuditConfig(
            nome="bolsa_familia_uf_raw",
            path=RAW_DIR / "bolsa_familia" / "nordeste" / "bolsa_familia_uf_mensal.csv",
            camada="raw",
            descricao="Bolsa Família agregado por UF x mês via Portal da Transparência.",
            frequencia_esperada="mensal",
            expected_years=list(range(2015, 2026)),
            expected_ufs=EXPECTED_UFS,
            key_columns=["ano", "mes", "uf"],
            critical_columns=["ano", "mes", "uf"],
            optional=True,
        ),
        SourceAuditConfig(
            nome="bolsa_familia_portal_raw",
            path=RAW_DIR / "bolsa_familia" / "nordeste" / "bolsa_familia_portal_transparencia.csv",
            camada="raw",
            descricao="Registros municipais do Bolsa Família coletados no Portal da Transparência.",
            frequencia_esperada="mensal",
            expected_years=list(range(2015, 2026)),
            expected_ufs=EXPECTED_UFS,
            critical_columns=["ano", "mes", "uf"],
            optional=True,
        ),
        SourceAuditConfig(
            nome="bacen_bimestral_processed",
            path=PROCESSED_DIR / "bacen" / "nacional" / "bacen_bimestral.csv",
            camada="processed",
            descricao="Séries BACEN harmonizadas para bimestre.",
            frequencia_esperada="bimestral",
            expected_years=list(range(2015, 2026)),
            key_columns=["ano_bim", "bimestre"],
            critical_columns=["ano_bim", "bimestre", "credito_PF_nordeste", "credito_PJ_nordeste"],
        ),
        SourceAuditConfig(
            nome="caged_bimestral_processed",
            path=PROCESSED_DIR / "caged" / "nordeste" / "caged_bimestral.csv",
            camada="processed",
            descricao="Saldo do CAGED harmonizado para bimestre por UF.",
            frequencia_esperada="bimestral",
            expected_years=list(range(2015, 2026)),
            expected_ufs=EXPECTED_UFS,
            key_columns=["ano_bim", "bimestre", "sigla_uf"],
            critical_columns=["ano_bim", "bimestre", "sigla_uf", "saldo"],
        ),
        SourceAuditConfig(
            nome="execucao_orcamentaria_al_processed",
            path=PROCESSED_DIR / "execucao_orcamentaria" / "al" / "transparencia_al.csv",
            camada="processed",
            descricao="Execução orçamentária processada de Alagoas.",
            frequencia_esperada="anual",
            expected_years=list(range(2015, 2026)),
            critical_columns=["ano"],
            optional=True,
        ),
        SourceAuditConfig(
            nome="execucao_orcamentaria_ce_processed",
            path=PROCESSED_DIR / "execucao_orcamentaria" / "ce" / "siof_ce.csv",
            camada="processed",
            descricao="Execução orçamentária processada do Ceará.",
            frequencia_esperada="mensal",
            expected_years=list(range(2015, 2027)),
            critical_columns=["ano"],
            optional=True,
        ),
        SourceAuditConfig(
            nome="execucao_orcamentaria_pi_processed",
            path=PROCESSED_DIR / "execucao_orcamentaria" / "pi" / "transparencia_pi.csv",
            camada="processed",
            descricao="Execução orçamentária processada do Piauí.",
            frequencia_esperada="anual",
            expected_years=list(range(2015, 2026)),
            critical_columns=["ano"],
            optional=True,
        ),
        SourceAuditConfig(
            nome="painel_tese_bimestral",
            path=PROCESSED_DIR / "model_ready" / "painel_tese_bimestral.csv",
            camada="processed",
            descricao="Painel final model-ready da tese.",
            frequencia_esperada="bimestral",
            expected_years=list(range(2015, 2026)),
            expected_ufs=EXPECTED_UFS,
            key_columns=["ano_bim", "bimestre", "uf"],
            critical_columns=[
                "ano_bim",
                "bimestre",
                "uf",
                "saldo",
                "credito_PF_nordeste_real",
                "credito_PJ_nordeste_real",
            ],
        ),
    ]

    @classmethod
    def run(cls) -> dict[str, Any]:
        """Executa auditoria completa e salva artefatos."""
        cls.QUALITY_DIR.mkdir(parents=True, exist_ok=True)

        audits = [cls.audit_source(config) for config in cls.SOURCES]
        summary_df = pd.DataFrame(cls._flatten_summary(item) for item in audits)

        report = {
            "generated_at": datetime.now().isoformat(),
            "project_base_dir": str(BASE_DIR.resolve()),
            "summary": cls.build_global_summary(audits),
            "sources": audits,
        }

        json_path = cls.QUALITY_DIR / "quality_report.json"
        md_path = cls.QUALITY_DIR / "quality_report.md"
        csv_path = cls.QUALITY_DIR / "quality_summary.csv"

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        summary_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        md_path.write_text(cls.render_markdown(report), encoding="utf-8")

        return report

    @classmethod
    def audit_source(cls, config: SourceAuditConfig) -> dict[str, Any]:
        """Audita um dataset individual."""
        result: dict[str, Any] = {
            "nome": config.nome,
            "descricao": config.descricao,
            "camada": config.camada,
            "path": str(config.path),
            "frequencia_esperada": config.frequencia_esperada,
            "file_exists": config.path.exists(),
            "status": "erro",
            "warnings": [],
            "metrics": {},
        }

        if not config.path.exists():
            if config.optional:
                result["warnings"].append("Arquivo opcional não encontrado.")
                result["status"] = "alerta"
            else:
                result["warnings"].append("Arquivo não encontrado.")
            result["metrics"] = {
                "row_count": 0,
                "column_count": 0,
                "missing_percent_total": None,
                "duplicate_key_count": None,
            }
            return result

        df = pd.read_csv(config.path, low_memory=False)
        metrics = cls._basic_metrics(df)
        warnings = result["warnings"]

        metrics["key_columns_present"] = [c for c in config.key_columns if c in df.columns]
        metrics["critical_columns_present"] = [c for c in config.critical_columns if c in df.columns]
        metrics["critical_columns_missing"] = [c for c in config.critical_columns if c not in df.columns]

        if metrics["critical_columns_missing"]:
            warnings.append(
                "Colunas críticas ausentes: " + ", ".join(metrics["critical_columns_missing"])
            )

        duplicate_key_count = cls._duplicate_key_count(df, config.key_columns)
        metrics["duplicate_key_count"] = duplicate_key_count
        if duplicate_key_count:
            warnings.append(f"Foram encontradas {duplicate_key_count} chaves duplicadas.")

        critical_missing = cls._critical_missing_summary(df, config.critical_columns)
        metrics["critical_missing_by_column"] = critical_missing
        high_missing_critical = [col for col, pct in critical_missing.items() if pct > 20]
        if high_missing_critical:
            warnings.append(
                "Nulos relevantes em colunas críticas: " + ", ".join(high_missing_critical)
            )

        years_info = cls._year_coverage(df)
        metrics["year_coverage"] = years_info
        if config.expected_years:
            missing_years = sorted(set(config.expected_years) - set(years_info["years"]))
            metrics["missing_years_expected"] = missing_years
            if missing_years:
                warnings.append("Anos esperados ausentes: " + ", ".join(map(str, missing_years)))

        uf_info = cls._uf_coverage(df)
        metrics["uf_coverage"] = uf_info
        if config.expected_ufs:
            missing_ufs = sorted(set(config.expected_ufs) - set(uf_info["ufs"]))
            metrics["missing_ufs_expected"] = missing_ufs
            if missing_ufs:
                warnings.append("UFs esperadas ausentes: " + ", ".join(missing_ufs))

        periodicity_info = cls._periodicity_coverage(df)
        metrics["periodicity_coverage"] = periodicity_info

        continuity_info = cls._continuity_coverage(df, config)
        metrics["continuity_coverage"] = continuity_info
        if continuity_info.get("missing_combinations_total", 0) > 0:
            warnings.append(
                f"Lacunas de continuidade: {continuity_info['missing_combinations_total']} combinações período-UF ausentes."
            )

        result["metrics"] = metrics
        result["status"] = cls._resolve_status(metrics, warnings)
        return result

    @staticmethod
    def _basic_metrics(df: pd.DataFrame) -> dict[str, Any]:
        total_cells = int(df.shape[0] * df.shape[1]) if not df.empty else 0
        missing_cells = int(df.isna().sum().sum()) if total_cells else 0
        return {
            "row_count": int(len(df)),
            "column_count": int(len(df.columns)),
            "columns": list(df.columns),
            "missing_cells_total": missing_cells,
            "missing_percent_total": round((missing_cells / total_cells) * 100, 2) if total_cells else 0.0,
        }

    @staticmethod
    def _duplicate_key_count(df: pd.DataFrame, key_columns: list[str]) -> int | None:
        available = [c for c in key_columns if c in df.columns]
        if not available:
            return None
        return int(df.duplicated(subset=available).sum())

    @staticmethod
    def _critical_missing_summary(df: pd.DataFrame, critical_columns: list[str]) -> dict[str, float]:
        result: dict[str, float] = {}
        if df.empty:
            return result
        for col in critical_columns:
            if col in df.columns:
                result[col] = round(float(df[col].isna().mean() * 100), 2)
        return result

    @staticmethod
    def _year_coverage(df: pd.DataFrame) -> dict[str, Any]:
        year_cols = ["ano_bim", "exercicio", "ano"]
        for col in year_cols:
            if col in df.columns:
                years = sorted(pd.to_numeric(df[col], errors="coerce").dropna().astype(int).unique().tolist())
                return {
                    "source_column": col,
                    "years": years,
                    "min_year": min(years) if years else None,
                    "max_year": max(years) if years else None,
                    "count_years": len(years),
                }

        if "data" in df.columns:
            datas = pd.to_datetime(df["data"], errors="coerce").dropna()
            years = sorted(datas.dt.year.unique().tolist())
            return {
                "source_column": "data",
                "years": years,
                "min_year": min(years) if years else None,
                "max_year": max(years) if years else None,
                "count_years": len(years),
            }

        return {"source_column": None, "years": [], "min_year": None, "max_year": None, "count_years": 0}

    @staticmethod
    def _uf_coverage(df: pd.DataFrame) -> dict[str, Any]:
        if "sigla_uf" in df.columns:
            ufs = sorted(df["sigla_uf"].dropna().astype(str).unique().tolist())
            return {"source_column": "sigla_uf", "ufs": ufs, "count_ufs": len(ufs)}

        if "uf" in df.columns:
            ufs = sorted(df["uf"].dropna().astype(str).unique().tolist())
            return {"source_column": "uf", "ufs": ufs, "count_ufs": len(ufs)}

        return {"source_column": None, "ufs": [], "count_ufs": 0}

    @staticmethod
    def _periodicity_coverage(df: pd.DataFrame) -> dict[str, Any]:
        if {"ano", "mes"}.issubset(df.columns):
            out = df[["ano", "mes"]].copy()
            out["ano"] = pd.to_numeric(out["ano"], errors="coerce")
            out["mes"] = pd.to_numeric(out["mes"], errors="coerce")
            out = out.dropna().drop_duplicates()
            return {
                "source": ["ano", "mes"],
                "distinct_periods": int(len(out)),
                "min_period": f"{int(out['ano'].min())}-{int(out['mes'].min()):02d}" if not out.empty else None,
                "max_period": f"{int(out['ano'].max())}-{int(out['mes'].max()):02d}" if not out.empty else None,
            }

        if {"ano_bim", "bimestre"}.issubset(df.columns):
            out = df[["ano_bim", "bimestre"]].copy()
            out["ano_bim"] = pd.to_numeric(out["ano_bim"], errors="coerce")
            out["bimestre"] = pd.to_numeric(out["bimestre"], errors="coerce")
            out = out.dropna().drop_duplicates()
            return {
                "source": ["ano_bim", "bimestre"],
                "distinct_periods": int(len(out)),
                "min_period": f"{int(out['ano_bim'].min())}-B{int(out['bimestre'].min())}" if not out.empty else None,
                "max_period": f"{int(out['ano_bim'].max())}-B{int(out['bimestre'].max())}" if not out.empty else None,
            }

        if {"exercicio", "periodo"}.issubset(df.columns):
            out = df[["exercicio", "periodo"]].copy()
            out["exercicio"] = pd.to_numeric(out["exercicio"], errors="coerce")
            out["periodo"] = pd.to_numeric(out["periodo"], errors="coerce")
            out = out.dropna().drop_duplicates()
            return {
                "source": ["exercicio", "periodo"],
                "distinct_periods": int(len(out)),
                "min_period": f"{int(out['exercicio'].min())}-P{int(out['periodo'].min())}" if not out.empty else None,
                "max_period": f"{int(out['exercicio'].max())}-P{int(out['periodo'].max())}" if not out.empty else None,
            }

        if "data" in df.columns:
            datas = pd.to_datetime(df["data"], errors="coerce").dropna()
            return {
                "source": ["data"],
                "distinct_periods": int(datas.dt.to_period("M").nunique()),
                "min_period": str(datas.min().date()) if not datas.empty else None,
                "max_period": str(datas.max().date()) if not datas.empty else None,
            }

        return {"source": [], "distinct_periods": 0, "min_period": None, "max_period": None}

    @staticmethod
    def _continuity_coverage(df: pd.DataFrame, config: SourceAuditConfig) -> dict[str, Any]:
        if not config.expected_years:
            return {"available": False, "missing_combinations_total": 0, "sample_missing": []}

        uf_col = "sigla_uf" if "sigla_uf" in df.columns else "uf" if "uf" in df.columns else None

        if {"ano", "mes"}.issubset(df.columns) and uf_col:
            observed = {
                (int(ano), int(mes), str(uf))
                for ano, mes, uf in df[["ano", "mes", uf_col]].dropna().drop_duplicates().itertuples(index=False, name=None)
            }
            expected = {
                (ano, mes, uf)
                for ano in config.expected_years
                for mes in range(1, 13)
                for uf in (config.expected_ufs or [])
            }
        elif {"ano_bim", "bimestre"}.issubset(df.columns) and uf_col:
            observed = {
                (int(ano), int(bim), str(uf))
                for ano, bim, uf in df[["ano_bim", "bimestre", uf_col]].dropna().drop_duplicates().itertuples(index=False, name=None)
            }
            expected = {
                (ano, bim, uf)
                for ano in config.expected_years
                for bim in range(1, 7)
                for uf in (config.expected_ufs or [])
            }
        elif {"exercicio", "periodo"}.issubset(df.columns) and uf_col and config.frequencia_esperada in {"bimestral", "quadrimestral"}:
            max_period = 6 if config.frequencia_esperada == "bimestral" else 3
            observed = {
                (int(ano), int(per), str(uf))
                for ano, per, uf in df[["exercicio", "periodo", uf_col]].dropna().drop_duplicates().itertuples(index=False, name=None)
            }
            expected = {
                (ano, per, uf)
                for ano in config.expected_years
                for per in range(1, max_period + 1)
                for uf in (config.expected_ufs or [])
            }
        elif {"ano"}.issubset(df.columns) and uf_col:
            observed = {
                (int(ano), str(uf))
                for ano, uf in df[["ano", uf_col]].dropna().drop_duplicates().itertuples(index=False, name=None)
            }
            expected = {
                (ano, uf)
                for ano in config.expected_years
                for uf in (config.expected_ufs or [])
            }
        else:
            return {"available": False, "missing_combinations_total": 0, "sample_missing": []}

        missing = sorted(expected - observed)
        return {
            "available": True,
            "missing_combinations_total": len(missing),
            "sample_missing": [str(item) for item in missing[:20]],
        }

    @staticmethod
    def _resolve_status(metrics: dict[str, Any], warnings: list[str]) -> str:
        if metrics.get("row_count", 0) == 0:
            return "erro"
        if metrics.get("critical_columns_missing"):
            return "erro"
        if warnings:
            return "alerta"
        return "ok"

    @staticmethod
    def _flatten_summary(item: dict[str, Any]) -> dict[str, Any]:
        metrics = item["metrics"]
        year_cov = metrics.get("year_coverage", {})
        uf_cov = metrics.get("uf_coverage", {})
        continuity = metrics.get("continuity_coverage", {})
        return {
            "nome": item["nome"],
            "camada": item["camada"],
            "status": item["status"],
            "path": item["path"],
            "row_count": metrics.get("row_count"),
            "column_count": metrics.get("column_count"),
            "missing_percent_total": metrics.get("missing_percent_total"),
            "duplicate_key_count": metrics.get("duplicate_key_count"),
            "count_years": year_cov.get("count_years"),
            "min_year": year_cov.get("min_year"),
            "max_year": year_cov.get("max_year"),
            "count_ufs": uf_cov.get("count_ufs"),
            "missing_combinations_total": continuity.get("missing_combinations_total"),
            "warnings_count": len(item["warnings"]),
            "warnings": " | ".join(item["warnings"]),
        }

    @staticmethod
    def build_global_summary(audits: list[dict[str, Any]]) -> dict[str, Any]:
        total = len(audits)
        return {
            "total_sources": total,
            "ok": sum(1 for item in audits if item["status"] == "ok"),
            "alerta": sum(1 for item in audits if item["status"] == "alerta"),
            "erro": sum(1 for item in audits if item["status"] == "erro"),
        }

    @classmethod
    def render_markdown(cls, report: dict[str, Any]) -> str:
        lines = [
            "# Relatório de Qualidade dos Dados",
            "",
            f"Gerado em: {report['generated_at']}",
            "",
            "## Resumo Geral",
            "",
            f"- Total de fontes auditadas: {report['summary']['total_sources']}",
            f"- OK: {report['summary']['ok']}",
            f"- Alerta: {report['summary']['alerta']}",
            f"- Erro: {report['summary']['erro']}",
            "",
            "## Fontes Auditadas",
            "",
        ]

        for item in report["sources"]:
            metrics = item["metrics"]
            year_cov = metrics.get("year_coverage", {})
            uf_cov = metrics.get("uf_coverage", {})
            continuity = metrics.get("continuity_coverage", {})
            lines.extend(
                [
                    f"### {item['nome']}",
                    "",
                    f"- Status: **{item['status'].upper()}**",
                    f"- Camada: `{item['camada']}`",
                    f"- Arquivo: `{item['path']}`",
                    f"- Registros: {metrics.get('row_count', 0)}",
                    f"- Colunas: {metrics.get('column_count', 0)}",
                    f"- Nulos totais: {metrics.get('missing_percent_total')}%",
                    f"- Cobertura temporal: {year_cov.get('min_year')} a {year_cov.get('max_year')}",
                    f"- Cobertura territorial: {uf_cov.get('count_ufs', 0)} UFs",
                    f"- Lacunas de continuidade: {continuity.get('missing_combinations_total', 0)}",
                ]
            )

            if item["warnings"]:
                lines.append("- Alertas:")
                for warning in item["warnings"]:
                    lines.append(f"  - {warning}")
            lines.append("")

        return "\n".join(lines).rstrip() + "\n"


def executar_auditoria_qualidade() -> dict[str, Any]:
    """Função de conveniência para auditoria completa."""
    return DataQualityAuditor.run()


if __name__ == "__main__":
    executar_auditoria_qualidade()
