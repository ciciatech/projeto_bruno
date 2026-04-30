#!/bin/bash
# Quality gate adaptado para o projeto Bruno.
#
# O gate genérico do ciciatech-task-pilot espera app/ (FastAPI). Aqui o
# pacote Python é pipeline/ e só os módulos puros (sem I/O externo) entram
# no alvo de cobertura. Frontend usa Vite + ESLint; suite de testes Vitest
# entrará na T15.

set -e
cd "$(dirname "$0")/.."

LOG=".claude/task-pilot/quality-gate.log"
mkdir -p "$(dirname "$LOG")"
: > "$LOG"

ts() { date "+%H:%M:%S"; }
note() { echo "[$(ts)] $*" | tee -a "$LOG"; }

note "Quality gate · projeto Bruno"
echo "---" | tee -a "$LOG"

if [ -d frontend ]; then
  note "Frontend · npm run lint"
  ( cd frontend && npm run lint ) 2>&1 | tee -a "$LOG"
  note "Frontend · npm run build (TS check + Vite)"
  ( cd frontend && npm run build ) 2>&1 | tee -a "$LOG" >/dev/null
  note "Frontend OK"
fi

if [ -d pipeline ]; then
  if [ ! -d venv ]; then
    note "venv ausente -- rode python3 -m venv venv && pip install -r requirements.txt"
    exit 1
  fi
  # shellcheck disable=SC1091
  source venv/bin/activate
  note "Pipeline · pytest com cobertura sobre pipeline/ (>=10% overall, modulos com I/O omitidos)"
  pytest pipeline/tests/ -q --cov=pipeline --cov-report=term-missing --cov-fail-under=10 2>&1 | tee -a "$LOG"
  note "Pipeline OK"
fi

note "Verificacao de secrets staged"
if git diff --cached --name-only 2>/dev/null | grep -qE '\.env$|\.env\.[^.]+$'; then
  note "Arquivo .env staged -- abortando"
  exit 1
fi
note "Secrets OK"

note "TUDO PASSOU"
