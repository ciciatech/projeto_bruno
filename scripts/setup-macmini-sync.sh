#!/bin/bash
# Setup do auto-sync git no Mac Mini para o projeto bruno (pipeline regional CE).
# Rode UMA vez no Mac Mini. Idempotente — pode rodar de novo sem problemas.
#
# Uso:
#   bash scripts/setup-macmini-sync.sh
#
# O que faz:
#   1. Clona/atualiza o repo em $HOME/dev/academico/bruno
#   2. Configura core.hooksPath para .githooks
#   3. Garante venv com dependências (requirements.txt)
#   4. Cria LaunchAgent que roda git-auto-pull.sh a cada 5 min
#   5. Carrega o LaunchAgent

set -e

REPO_URL="https://github.com/ciciatech/projeto_bruno.git"
REPO_DIR="$HOME/dev/academico/bruno"
NOME_PROJETO="bruno-pipeline"
BRANCH="dev"
PLIST_FILE="$HOME/Library/LaunchAgents/com.${NOME_PROJETO}.autopull.plist"

echo "=== 1. Clonando/atualizando repo ==="
if [[ -d "$REPO_DIR/.git" ]]; then
    echo "Repo já existe — fazendo pull"
    cd "$REPO_DIR"
    git fetch origin "$BRANCH"
    git checkout "$BRANCH"
    git pull origin "$BRANCH" --ff-only
else
    mkdir -p "$(dirname "$REPO_DIR")"
    git clone -b "$BRANCH" "$REPO_URL" "$REPO_DIR"
    cd "$REPO_DIR"
fi

echo
echo "=== 2. Configurando hooks path ==="
git config core.hooksPath .githooks
chmod +x .githooks/post-commit .githooks/pre-push 2>/dev/null || true
chmod +x scripts/git-auto-pull.sh scripts/run_coletas.sh 2>/dev/null || true

echo
echo "=== 3. Setup do venv Python ==="
if [[ ! -d "$REPO_DIR/venv" ]]; then
    python3 -m venv "$REPO_DIR/venv"
fi
"$REPO_DIR/venv/bin/pip" install --upgrade pip --quiet
"$REPO_DIR/venv/bin/pip" install -r "$REPO_DIR/requirements.txt" --quiet
echo "venv pronto: $REPO_DIR/venv"

echo
echo "=== 4. Diretório de logs ==="
mkdir -p "$HOME/.local/log"

echo
echo "=== 5. Criando/recarregando LaunchAgent ==="
if launchctl list | grep -q "com.${NOME_PROJETO}.autopull"; then
    launchctl unload "$PLIST_FILE" 2>/dev/null || true
fi

cat > "$PLIST_FILE" << PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.${NOME_PROJETO}.autopull</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>${REPO_DIR}/scripts/git-auto-pull.sh</string>
    </array>
    <key>StartInterval</key>
    <integer>300</integer>
    <key>RunAtLoad</key>
    <true/>
    <key>StandardOutPath</key>
    <string>${HOME}/.local/log/${NOME_PROJETO}-autopull-out.log</string>
    <key>StandardErrorPath</key>
    <string>${HOME}/.local/log/${NOME_PROJETO}-autopull-err.log</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin</string>
        <key>HOME</key>
        <string>${HOME}</string>
    </dict>
</dict>
</plist>
PLIST

launchctl load "$PLIST_FILE"
launchctl list | grep "${NOME_PROJETO}" >/dev/null && echo "[OK] LaunchAgent ${NOME_PROJETO}.autopull ativo"

echo
echo "=== Setup concluído ==="
echo
echo "Próximos passos:"
echo "  - Auto-pull rodando a cada 5 min (log: ~/.local/log/${NOME_PROJETO}-autopull.log)"
echo "  - Para disparar coletas manualmente:"
echo "      bash $REPO_DIR/scripts/run_coletas.sh onda1"
echo "      bash $REPO_DIR/scripts/run_coletas.sh onda2"
echo "      bash $REPO_DIR/scripts/run_coletas.sh onda3"
echo "  - Para forçar pull agora:"
echo "      bash $REPO_DIR/scripts/git-auto-pull.sh"
echo "  - Para parar o auto-pull:"
echo "      launchctl unload $PLIST_FILE"
