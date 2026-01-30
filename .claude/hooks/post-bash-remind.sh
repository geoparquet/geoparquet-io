#!/bin/bash
# Distill - PostToolUse Hook for Bash
# Reminds to use MCP tools for large outputs

INPUT=$(cat)
TOOL_RESPONSE=$(echo "$INPUT" | jq -r '.tool_response // empty')
RESPONSE_SIZE=${#TOOL_RESPONSE}

# Threshold: 5000 chars ~ 1250 tokens
THRESHOLD=5000

# Skip if output is small
if [ "$RESPONSE_SIZE" -lt "$THRESHOLD" ]; then
  exit 0
fi

# Detect content type and suggest appropriate tool
if echo "$TOOL_RESPONSE" | grep -qiE "(error TS|warning TS|error\[E|npm ERR|ERROR in|failed|FAILED)"; then
  echo '{"systemMessage": "TIP: Large build output detected. Use mcp__distill__auto_optimize to compress errors (95%+ reduction)."}'
  exit 0
fi

if echo "$TOOL_RESPONSE" | grep -qiE "(\[INFO\]|\[ERROR\]|\[WARN\]|\[DEBUG\]|[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2})"; then
  echo '{"systemMessage": "TIP: Large log output detected. Use mcp__distill__summarize_logs to compress (80-90% reduction)."}'
  exit 0
fi

# Generic large output
echo '{"systemMessage": "TIP: Large output ('$RESPONSE_SIZE' chars). Consider using mcp__distill__auto_optimize for compression (40-60% reduction)."}'
exit 0
