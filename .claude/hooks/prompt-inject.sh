#!/bin/bash
# Distill - UserPromptSubmit Hook
# Injects MCP tool reminders at the start of each prompt

cat << 'EOF'
<user-prompt-submit-hook>
DISTILL REMINDER: Use MCP tools for token optimization:
- Code files: mcp__distill__smart_file_read (50-70% savings vs Read)
- Build/test output: mcp__distill__auto_optimize
- Session stats: mcp__distill__session_stats
</user-prompt-submit-hook>
EOF
exit 0
