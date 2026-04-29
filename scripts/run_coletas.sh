#!/bin/bash
# Disparador de coletas no Mac Mini. Use via SSH ou diretamente.
#
# Uso:
#   bash scripts/run_coletas.sh onda1   # leves: BACEN + STN (~5 min)
#   bash scripts/run_coletas.sh onda2   # pesadas: BF + BPC + invest_federal (~6-10h)
#   bash scripts/run_coletas.sh onda3   # CAGED municipal (~12-24h, FTP MTE)
#   bash scripts/run_coletas.sh painel  # apenas reconstrói o painel
#   bash scripts/run_coletas.sh full    # tudo + painel
#
# Logs em ~/.local/log/bruno-pipeline-coletas.log

set -e

REPO_DIR="$HOME/dev/academico/bruno"
LOG_FILE="$HOME/.local/log/bruno-pipeline-coletas.log"
PY="$REPO_DIR/venv/bin/python"

mkdir -p "$(dirname "$LOG_FILE")"

cd "$REPO_DIR"

ONDA="${1:-onda1}"
echo "$(date): >>> RUN_COLETAS — onda=$ONDA <<<" | tee -a "$LOG_FILE"

run() {
    echo "$(date): $@" | tee -a "$LOG_FILE"
    "$@" 2>&1 | tee -a "$LOG_FILE"
}

case "$ONDA" in
    onda1)
        run "$PY" -m pipeline.run --modulos-ce bacen
        run "$PY" -m pipeline.extract.ipea_fbcf
        run "$PY" -m pipeline.run --modulos-ce transferencias_municipais
        ;;
    onda2)
        run "$PY" -m pipeline.run --modulos-ce bolsa_familia_ce
        run "$PY" -m pipeline.run --modulos-ce bpc
        run "$PY" -m pipeline.run --modulos-ce invest_federal
        ;;
    onda3)
        run "$PY" -m pipeline.run --modulos-ce caged_municipal
        ;;
    painel)
        run "$PY" -m pipeline.run --painel-ce
        ;;
    full)
        run "$PY" -m pipeline.run --modulos-ce bacen
        run "$PY" -m pipeline.extract.ipea_fbcf
        run "$PY" -m pipeline.run --modulos-ce transferencias_municipais
        run "$PY" -m pipeline.run --modulos-ce bolsa_familia_ce
        run "$PY" -m pipeline.run --modulos-ce bpc
        run "$PY" -m pipeline.run --modulos-ce invest_federal
        run "$PY" -m pipeline.run --modulos-ce caged_municipal
        run "$PY" -m pipeline.run --painel-ce
        ;;
    *)
        echo "Onda desconhecida: $ONDA"
        echo "Use: onda1 | onda2 | onda3 | painel | full"
        exit 1
        ;;
esac

echo "$(date): >>> RUN_COLETAS — concluído onda=$ONDA <<<" | tee -a "$LOG_FILE"
