# Agent instructions

## Coding style

- Follow the Nginx core development guide https://nginx.org/en/docs/dev/development_guide.html
- Follow established pattern of CI steps -- ensure all steps can be run locally and are composed of existing tools. Update or introduce tools that can be used for composing the CI steps.

## Verifying changes

- Follow the Development section in [README.md](README.md): use the included containers for development, testing, packaging, and manual validation work.
- For source builds, tests, formatting, benchmarks, and manual nginx validation, run `make` inside the `dev` container. Prefer one of these patterns:
  - `docker compose run --rm dev make <target>` for one-off commands.
  - `make shell` first, then run `make <target>` from inside the container for multi-step interactive work.
- For Debian packaging work, use the `packaging` container:
  - `docker compose run --rm packaging make debian-package-smoke`
  - `docker compose run --rm packaging make debian-source-package ...`
- In GitHub Codespaces, the `.devcontainer` is already the development environment, so run the usual `make` targets directly instead of nesting `docker compose`.
- CI validates source builds and benchmarks against nginx `1.26.3`, `1.28.3`, and `1.29.8`. Keep workflow matrices and published dev-image versions in sync when changing supported nginx versions; note that the local Makefile default may differ.
- CI formatting runs `docker compose run --rm dev make format` and then checks for source diffs. After formatting locally, inspect the diff before closing the task.
- For performance-sensitive purge or index changes, run `docker compose run --rm dev make bench-quick` when practical.
- For non-trivial C refactors, check editor diagnostics first, then run containerized tests. Static diagnostics are useful for fast feedback, but they do not reliably catch nginx-version-specific or preprocessor-heavy regressions in this repo.

## Current ngx_cache_pilot assumptions

- `ngx_cache_pilot` currently has a single shared-memory index backend. Do not reintroduce backend selectors or store ops indirection unless a real second backend is being added.
- Indexed tag purges no longer bootstrap the index on demand inside the request path. If the target cache zone is not registered for indexing or its index is not ready, the purge declines.
- Protocol-specific purge integration is table-driven in `src/ngx_cache_pilot_module.c`. Prefer extending the shared protocol descriptor/helpers instead of copying per-protocol handler or attach logic.
- Request-level purge metrics are intentionally centralized through shared helpers in `src/ngx_cache_pilot_module.c`. Keep new `purges_*` and key-index counters routed through those helpers instead of scattering `NGX_CACHE_PILOT_METRICS_INC()` calls.

## Build notes

- On dynamic builds using `--add-dynamic-module`, a missing `ngx_modules` symbol at runtime points to a packaging or build-flow problem outside the normal nginx `auto/module` path.
- The dynamic `.so` still depends on nginx protocol modules such as fastcgi, proxy, scgi, and uwsgi; packaged deployments may require those modules to be statically linked into nginx or loaded before this module.

## Purge semantics

- `fastcgi_cache_purge`, `proxy_cache_purge`, `scgi_cache_purge`, and `uwsgi_cache_purge` accept one or more complex-value conditions followed by optional `soft` and `purge_all` flags.
- Purge is enabled when at least one configured condition evaluates to a value that is non-empty, not `"0"`, and not `"off"`. Request-method and client-IP restrictions should be expressed with normal nginx mechanisms such as `map`, `allow`, and `deny`.
- `cache_pilot_purge_mode_header` values `soft`, `true`, or `1` only switch soft versus hard purge mode after a purge request has already matched; they do not enable purge conditionally.
- `purge_all` uses the configured `soft` flag and does not honor `cache_pilot_purge_mode_header`.

## Documentation

- After any change that affects user-visible behaviour, known limitations, configuration options, or the public API, **review `README.md` and update it** to reflect the current state before closing the task. Pay particular attention to the **Known issues** section, which documents known limitations — keep each entry accurate and up to date.
- Before completing work, review `CHANGELOG.md` and add every notable user-visible change, fix, packaging change, or release-tooling change to `UNRELEASED`. During release preparation, move those entries into a dated version section and keep the top `debian/changelog` version and notes aligned with the Git tag and GitHub release because packaging tools derive the source package version from that file.
