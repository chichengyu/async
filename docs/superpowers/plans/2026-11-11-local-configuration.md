# Local configuration isolation implementation plan

**Goal:** All core fluent configuration, including Default methods, affects only its builder and resulting execution/instances. Only explicit global setters change global state.

**Architecture:** Carry an immutable logger override in context; nil override restores global inheritance. Context replacement preserves the builder override, and constructed Group/Pool and shard instances retain it for submissions with independently supplied contexts. Do not swap global loggers during execution. Existing non-logger defaults retain their documented values and local scope.

**User requirement:** 所有核心库单独设置及 default* 都只能对当前起作用，不能影响其他。

- [x] Retain public fluent API regression tests for global mutation and execution routing.
- [x] Test context logger routing and inheritance without global mutation.
- [x] Replace fluent global writes across all core modules; propagate through context replacement, mode conversion, built instances, shard children, Flow/Pipe and bounded execution.
- [x] Test interleaved independent instances, local defaults, global inheritance and snapshot isolation; audit other fluent setters for global writes.
- [x] Update API documentation and generated reference, run race tests, full short suite, vet and a separate self-review pass.

## Verification

- `go test ./... -short -count=1 -timeout=30m`: passed, root package 432.972s. An earlier 3-minute package timeout was too short for the existing root suite.
- `go test -race ./internal/... -short -count=1 -timeout=10m`: passed for all ten core packages.
- `go test -race . ./internal/core ./internal/ratelimit -run 'TestFluentLogger|TestLocalLogger|Test.*RateLimiterBuiltLoggerIsolation' -count=1 -timeout=120s`: passed, including Reset, bounded and void execution.
- `go vet ./...`, `go run ./docs/cmd/apidoc -check`, `git diff --check`: passed.
- RateLimiter and sharded RateLimiter logger propagation regressions were observed failing before the additional fix and passing afterwards. Builder cancellation and deadline inheritance remains unchanged.
- Final review: self-review; no independent reviewer completed. Checked global writes, context propagation, borrowed-pool task scopes, constructed snapshots, Reset, trace rebinding and generated documentation.

The test processes used `GOROOT=D:/soft/go` and `GOPATH=D:/soft/pkg` because the default Go environment pointed GOROOT at an incomplete tool directory. No persistent Go environment settings were changed.

## Follow-up: timeout and other defaults

Audited timeout, submit timeout, workers, shards, result retention, ring buffers, overflow, streaming, retry and limiter defaults. These non-logger setters already store receiver-local values; reading a global default does not mutate it. Added `configuration_defaults_isolation_test.go` to exercise all parameterless builder Default methods under non-default global settings and to verify timeout isolation between builders and previously built instances.

- New tests and the existing defaults audit passed under `-race`.
- Existing timeout capture, pool ring buffer, retry DefaultConfig, limiter defaults and mode-switch contract tests passed; `go vet ./...` and `git diff --check` passed.
- A temporary Go overlay changed Pool.DefaultTimeout into a global reset; the sentinel test failed with `DefaultTimeout changed application-wide defaults`, proving the regression is detected without changing production source.

## Final verification audit

- Rechecked global configuration writes and public setter aliases, default factories, receiver-local configuration, value policies, autoscale copies, mode conversion, shard copies and borrowed-pool execution. No additional configuration-isolation defect was found. Production source was unchanged during this audit.
- Fresh `go test -race ./... -short -count=1 -timeout=30m` passed all ten internal packages and documentation packages. The root process exhausted Windows memory commit in ThreadSanitizer (`TracePart`, error 1455); this run did not complete successfully.
- Enumerated all 451 root `Test*` entries and reran them under `-race -short -count=1` in 23 batches. Twenty-two batches passed. The remaining batch encountered the same sanitizer allocation error; all 20 entries in that batch subsequently passed individually with process-local `GOMAXPROCS=4`. Thus all 451 root test entries completed successfully across these runs, subject to their existing short-mode skips. This does not claim that the monolithic root race run passed or that large non-short tiers ran.
- Fresh `go vet ./...`, `go run ./docs/cmd/apidoc -check` and `git diff --check` passed.
- Independent review could not be started because agent dispatch failed; this remains a self-review, not a completed independent review.
- Evidence logs are in the ignored `.gotmp/configuration-isolation/` directory.
