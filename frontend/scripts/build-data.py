"""
Converte o painel regional CE (CSV) em JSON otimizado para o frontend.

Saída: frontend/public/data/painel.json com:
  {
    "meta": {
      "regioes": [{"codigo": "01", "nome": "Cariri"}, ...],
      "periodo": {"inicio": "2015-01", "fim": "2025-12"},
      "linhas": 1848,
      "atualizado_em": "2026-04-30T..."
    },
    "rows": [{"r": "01", "y": 2015, "m": 1, "siof_emp": 1234.5, ...}, ...]
  }

Roda em build-time (npm run build chama via package.json), mantendo o app
estático sem precisar de backend.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "dados_nordeste" / "processed" / "model_ready" / "painel_regional_ce_mensal.csv"
DATA_DIR = Path(__file__).resolve().parents[1] / "public" / "data"
DST = DATA_DIR / "painel.json"
INDEX = DATA_DIR / "painel-index.json"


COL_RENAME = {
    # chaves
    "regiao_codigo": "r",
    "ano": "y",
    "mes": "m",
    # CAGED municipal
    "admissoes": "adm",
    "desligamentos": "des",
    "saldo": "sal",
    "total_movimentacoes": "mov",
    "salario_medio": "sal_med",
    # Bolsa Família
    "bf_valor_total": "bf_v",
    "bf_beneficiarios": "bf_b",
    # BPC
    "bpc_valor_total": "bpc_v",
    "bpc_beneficiarios": "bpc_b",
    # transferências constitucionais STN (federais)
    "transf_fed_fpm_dest": "tf_fpm",
    "transf_fed_fundeb_dest": "tf_fundeb",
    "transf_fed_itr_dest": "tf_itr",
    "transf_fed_outros_dest": "tf_outros",
    "transf_fed_royalties_dest": "tf_royalties",
    "transf_fed_total": "tf_total",
    # SIOF (estadual)
    "siof_anual_empenhado": "siof_emp",
    "siof_anual_pago": "siof_pago",
    "siof_anual_dotacao": "siof_dot",
    "siof_anual_n_acoes": "siof_n",
    # invest. federal (Portal Transp / RREO)
    "invest_fed_direto_ce": "if_direto",
    "invest_fed_ne_rateado": "if_ne",
    "invest_fed_nacional_rateado": "if_nac",
    "invest_fed_total": "if_total",
    # macro estadual
    "ibcr_ce": "ibcr",
    "invest_total_ce_r_2010_mi": "inv_tot",
    "share_pib_ce_br": "share",
}


def main() -> None:
    if not SRC.exists():
        print(f"painel não encontrado: {SRC}", file=sys.stderr)
        # Gera placeholder vazio para o build não falhar
        DST.parent.mkdir(parents=True, exist_ok=True)
        DST.write_text(json.dumps({
            "meta": {"regioes": [], "linhas": 0, "atualizado_em": None, "ausente": True},
            "rows": [],
        }), encoding="utf-8")
        print(f"WARN: gerado placeholder vazio em {DST}")
        return

    df = pd.read_csv(SRC, dtype={"regiao_codigo": str})
    df["regiao_codigo"] = df["regiao_codigo"].str.zfill(2)

    regioes = (
        df[["regiao_codigo", "regiao_nome"]]
        .drop_duplicates()
        .sort_values("regiao_codigo")
        .rename(columns={"regiao_codigo": "codigo", "regiao_nome": "nome"})
        .to_dict(orient="records")
    )

    keep_cols = [c for c in COL_RENAME if c in df.columns]
    payload = df[keep_cols].rename(columns=COL_RENAME)

    # Numeric clamp/round para reduzir tamanho do JSON
    for c in payload.columns:
        if payload[c].dtype.kind == "f":
            payload[c] = payload[c].round(2)
        if c in ("y", "m"):
            payload[c] = payload[c].astype(int)

    # Pandas .where(notna, None) preserva dtype float e Python json.dumps emite
    # `NaN` literal (JSON inválido). Convertemos pra object e mapeamos NaN→None
    # antes de json.dumps; alternativa equivalente: passar allow_nan=False ao
    # json.dumps e capturar erro — preferimos sanitizar.
    import math
    rows = [
        {
            k: (None if isinstance(v, float) and math.isnan(v) else v)
            for k, v in row.items()
        }
        for row in payload.to_dict(orient="records")
    ]

    out = {
        "meta": {
            "regioes": regioes,
            "linhas": len(rows),
            "periodo": {
                "inicio": f"{int(df['ano'].min())}-{int(df[df['ano']==df['ano'].min()]['mes'].min()):02d}",
                "fim": f"{int(df['ano'].max())}-{int(df[df['ano']==df['ano'].max()]['mes'].max()):02d}",
            },
            "atualizado_em": datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
            "fonte": str(SRC.relative_to(ROOT)),
        },
        "rows": rows,
    }

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    # allow_nan=False: garante JSON spec-compliant (sem NaN/Infinity).
    payload_str = json.dumps(out, ensure_ascii=False, separators=(",", ":"), allow_nan=False)

    import hashlib
    digest = hashlib.sha256(payload_str.encode("utf-8")).hexdigest()[:8]
    hashed_name = f"painel.{digest}.json"
    HASHED = DATA_DIR / hashed_name

    HASHED.write_text(payload_str, encoding="utf-8")
    INDEX.write_text(
        json.dumps(
            {
                "version": digest,
                "filename": hashed_name,
                "atualizado_em": out["meta"]["atualizado_em"],
                "linhas": out["meta"]["linhas"],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    DST.write_text(payload_str, encoding="utf-8")

    # cleanup: hashes antigos
    for old in DATA_DIR.glob("painel.*.json"):
        if old.name != hashed_name:
            old.unlink()

    print(
        f"OK · public/data/{hashed_name} · {HASHED.stat().st_size / 1024:.1f} KB · "
        f"{len(rows)} rows · {len(regioes)} regiões · index → {digest}"
    )


if __name__ == "__main__":
    main()
