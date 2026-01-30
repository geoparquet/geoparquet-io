#!/bin/bash
# Distill - PreToolUse Hook for Read
# Suggests smart_file_read for code files (non-blocking to allow Edit to work)

INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')
BASENAME=$(basename "$FILE_PATH")

# Skip suggestion for configuration files
if echo "$BASENAME" | grep -qiE "^(CLAUDE|README|CHANGELOG|LICENSE)"; then
  exit 0
fi
if echo "$BASENAME" | grep -qiE "\.(md|json|yaml|yml|toml|ini|config)$"; then
  exit 0
fi
if echo "$BASENAME" | grep -qiE "^(Dockerfile|Makefile|\.gitignore|\.env|\.prettierrc|\.eslintrc)"; then
  exit 0
fi

# Suggest smart_file_read for source code files (non-blocking)
if echo "$FILE_PATH" | grep -qE "\.(ts|tsx|js|jsx|py|go|rs|java|cpp|c|h|hpp)$"; then
  # Use systemMessage to suggest without blocking (allows Edit to work)
  cat << EOF
{"systemMessage": "TIP: Consider using mcp__distill__smart_file_read for '$BASENAME' to save 50-70% tokens. Example: mcp__distill__smart_file_read filePath=\"$FILE_PATH\" target={\"type\":\"function\",\"name\":\"myFunc\"}"}
EOF
  exit 0
fi

# Allow everything else
exit 0
