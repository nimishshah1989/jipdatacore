# Fix Plan — JIP Data Engine

When a chunk fails or tests break, follow this plan:

1. Read the error output carefully
2. Identify the root cause (missing import, wrong type, bad query)
3. Fix the specific issue — do not refactor surrounding code
4. Re-run the failing test to verify the fix
5. Run full test suite to check for regressions
6. If fix introduces new failures, revert and try a different approach
7. After 3 failed fix attempts, mark chunk as "failed" and move on
