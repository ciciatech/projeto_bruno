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
DST = Path(__file__).resolve().parents[1] / "public" / "data" / "painel.json"


COL_RENAME = {
    "regiao_codigo": "r",
    "ano": "y",
    "mes": "m",
    "siof_anual_empenhado": "siof_emp",
    "siof_anual_pago": "siof_pago",
    "siof_anual_dotacao": "siof_dot",
    "siof_anual_n_acoes": "siof_n",
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

    payload = payload.where(pd.notna(payload), None)
    rows = payload.to_dict(orient="records")

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

    DST.parent.mkdir(parents=True, exist_ok=True)
    DST.write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(
        f"OK · {DST.relative_to(ROOT)} · {DST.stat().st_size / 1024:.1f} KB · "
        f"{len(rows)} rows · {len(regioes)} regiões"
    )


if __name__ == "__main__":
    main()
