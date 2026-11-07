# Memory enforcement

## Plan

- [ ] Add per-command peak RSS accounting that catches a command which exits between samples.
- [ ] Keep process-group sampling for concurrent descendants and kill the whole group on an observed overage.
- [ ] Cover quick exits, descendants, parallel commands, and `--no-enforce-memory`; document sampling and peak scope.
- [ ] Run the focused and full test suites, then delete this plan in a separate final commit.
