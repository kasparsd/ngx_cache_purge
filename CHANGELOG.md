# Changelog

## UNRELEASED

## 2.0.0 - 2026-07-29

### Changed

- Purge misses are now idempotent: known not-found results return `200` with zero purged counters instead of `412`. Clients that need to detect no-op purges must inspect the response counters.

### Fixed

- Hardened purge-miss accounting so only explicit not-found declines are accepted as successful misses; unknown decline reasons still fail closed.
- Kept cache-key indexing correct for cache writes without tags.

## 1.2.0 - 2026-06-10

### Added

- Added Ubuntu PPA source-package publishing for Jammy and Noble.
- Added published development and packaging container images.
- Exposed cache-index health diagnostics and added benchmark summaries to pull requests.
- Expanded tests for conditional locations, purge accounting, index readiness, diagnostics, and response types.

### Changed

- Reused source artifacts between PPA uploads and prebuilt container images in CI.
- Removed the redundant runtime `NGINX_CACHE` guard because cache support is a build-time requirement.
- Documented release versioning and container-based packaging workflows.

### Fixed

- Inherited purge handlers correctly in child locations created by `if`.
- Corrected hard-purge accounting and stale cache-index cleanup.
- Marked the cache index ready after cache writes without tags.
- Improved Docker build performance.

## 1.1.0 - 2026-05-21

### Added

- Added a GitHub Codespaces development container for the build and test workflow.

### Changed

- Moved the `NGINX_CACHE` requirement check from runtime code to the NGINX module build configuration.

## 1.0.0 - 2026-04-21

- First reference release of the fork.
