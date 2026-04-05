# Forge OS — Ralph Iteration Prompt
# This file is used by frankbria/ralph-claude-code as the prompt for each loop iteration.
# It gets placed at .ralph/prompt.md in your project by /forge-build or ralph-setup.

Read tasks.json in this project root. Find the next chunk with status "pending".

## If NO pending chunks remain:
- Run: bash ~/.forge/bin/post-build-collect.sh
- Output the following block exactly:

```
RALPH_STATUS:
  STATUS: COMPLETE
  EXIT_SIGNAL: true
  SUMMARY: All chunks implemented, reviewed, and shipped.
```

## If a pending chunk exists:
1. Set its status to "in_progress" in tasks.json
2. Read the chunk spec from docs/specs/chunks/chunk-N.md
3. Read ~/.forge/knowledge/wiki/index.md — identify 1-2 relevant pattern articles and read ONLY those
4. Activate /guard for scope and destructive command protection
5. Use the implementer agent with the chunk spec path as argument
6. Run pre-commit checks:
   - `ruff check . --select E,F,W` (Python lint)
   - `pytest tests/ -v --tb=short` (tests)
   - Fix any failures, max 3 attempts
7. Run /review — let it auto-fix obvious issues
8. Run /ship to commit with coverage audit
9. Set chunk status to "done" in tasks.json
10. Append a summary to docs/decisions/session-log.md with: chunk name, files changed, bugs found, tests added

If a chunk fails after 3 fix attempts, set its status to "failed" in tasks.json, log the failure reason to docs/decisions/session-log.md, and move to the next pending chunk.

For frontend chunks (*.tsx, *.jsx, *.css, *.html files):
- If no DESIGN.md exists, run /design-consultation first
- After implementation, run /design-review

## Rules (Four Laws)
1. Prove, never claim — run tests, show output
2. No synthetic data — ever
3. Backend first always
4. See what you build — verify visually
