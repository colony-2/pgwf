# Repository Guidelines

## Project Structure & Module Organization
The workflow engine lives entirely in `pgwf.sql`, which defines the `pgwf` schema, its tables, and all helper functions. Integration tests under `test/` apply that script to an ephemeral Postgres instance via Go’s testing framework. The README doubles as a conceptual guide; consult it before reshaping schema objects so the documented guarantees (leases, wait-for semantics) stay accurate. Auxiliary tooling (e.g., `.devcontainer/`) is optional but mirrors the expected Postgres 15+ environment.

## Build, Test, and Development Commands
- `psql -d $PGDATABASE -f pgwf.sql` — installs or upgrades the schema; re-run after any SQL edit as the script is idempotent.
- `go test ./test -v` — compiles helper harnesses and exercises leasing, wait-for arrays, and notification paths end-to-end.
- `PGDATABASE=pgwf_dev psql -c "SELECT * FROM pgwf.jobs LIMIT 5"` — quick inspection pattern for verifying migrations and traces.
Run these commands from the repository root to keep relative paths valid.

## Coding Style & Naming Conventions
SQL objects use lowercase `snake_case` identifiers with the `pgwf.` schema prefix; keep keywords uppercase and align clauses on new lines for readability. Store procedure bodies should prefer explicit column lists over `SELECT *` to avoid drift. Go helpers (today limited to `test/pgwf_test.go`) must be `gofmt`-clean, use tabs for indentation, and expose test helpers via `camelCase` functions inside `_test.go` files. New SQL files should stay ASCII to match the existing style.

## Testing Guidelines
Every change to leasing, dependency, or archival behavior needs a regression in `test/pgwf_test.go` or a new `_test.go` module under `test/`. Follow Go’s `TestXxx` naming so `go test` discovers the cases automatically. Prefer table-driven tests when adding more capability scenarios, and exercise both the happy path and failure paths (e.g., lease expiration, wait-for normalization). Aim to keep the suite fast enough (<10s) for CI, so prefer truncating helper tables between cases instead of recreating databases.

## Commit & Pull Request Guidelines
Recent history (`git log`) shows short, imperative, lowercase subjects (e.g., “clean up overly complex wait_for normalization”). Match that style, keep body text optional but include SQL rationale when behavior changes. Pull requests should describe schema changes, include the exact test command you ran, link any tracking issues, and provide screenshots or `EXPLAIN` output only when relevant to performance claims. Flag backward-incompatible migrations explicitly so reviewers can plan rollout sequencing.
