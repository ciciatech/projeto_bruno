#!/bin/bash
# Auto-pull do projeto bruno — roda via LaunchAgent a cada 5 min no Mac Mini.
# Acompanha a branch `dev` (branch ativa de desenvolvimento da pipeline regional CE).

REPO_DIR="$HOME/dev/academico/bruno"
LOG_FILE="$HOME/.local/log/bruno-pipeline-autopull.log"
BRANCH="dev"

mkdir -p "$(dirname "$LOG_FILE")"

cd "$REPO_DIR" || {
    echo "$(date): ERRO - diretório não encontrado: $REPO_DIR" >> "$LOG_FILE"
    exit 1
}

# Pula se houver mudanças locais não commitadas (proteção contra perda de trabalho)
if ! git diff --quiet HEAD 2>/dev/null; then
    echo "$(date): SKIP - mudanças locais não commitadas no Mac Mini" >> "$LOG_FILE"
    exit 0
fi

git fetch origin "$BRANCH" --quiet 2>/dev/null

LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse "origin/$BRANCH")

if [[ "$LOCAL" != "$REMOTE" ]]; then
    OUTPUT=$(git pull origin "$BRANCH" --ff-only 2>&1)
    if [[ $? -eq 0 ]]; then
        echo "$(date): PULL OK - $(echo "$OUTPUT" | tail -1)" >> "$LOG_FILE"
    else
        echo "$(date): ERRO pull - $OUTPUT" >> "$LOG_FILE"
    fi
fi
