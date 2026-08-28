---
name: coroot-cluster-agent-rebase-upstream
description: Rework the MongoDB TLS+SNI fork onto an upstream coroot-cluster-agent tag using a candidate branch, detect upstream SNI capability exactly, verify with tests/build/docker build before any push, and push only on explicit user confirmation
---

# coroot-cluster-agent — Rework Upstream (candidate-first)

Reworks the MongoDB TLS+SNI fork onto an upstream release tag on a dedicated
candidate branch, detects whether upstream already ships equivalent SNI
support using exact patterns, and publishes (branch push + Docker image) only
after explicit user confirmation.

The fork adds SNI plumbing and fail-closed TLS on top of upstream. The old
`f60c3cd` implementation (tls=true + synthesized pod-name SNI, log-and-continue
on TLS errors) is **superseded — do not re-apply it**. Upstream's
`common.DatabaseTLSConfig` (CA chain-only verification via `VerifyConnection`,
mTLS, skip-verify) and all upstream deep MongoDB monitoring
(server_status, profiler, repl_status, current_op, obfuscate) are **preserved
as-is**.

## Prerequisites

- Run inside the fork repository: `github.com/tribunadigital/coroot-cluster-agent`
- `git`, `go 1.25+`, `docker`, and network access to GitHub and GHCR
- Authenticated Docker session for `ghcr.io`
- Clean working tree on the shared branch `add_mongodb_tls-2`
- Upstream remote URL must be `https://github.com/coroot/coroot-cluster-agent.git`
- User message includes a release version such as `v1.9.0` or `1.9.0`

## Procedure

### Step 1: Validate input

Extract the release version from `user_message`, normalize it to an upstream
tag, and derive the Docker image version without the `v` prefix.

```bash
TAG=$(printf '%s\n' "${USER_MESSAGE:-$1}" | grep -oE 'v?[0-9]+\.[0-9]+\.[0-9]+' | head -n1)
if [ -z "$TAG" ]; then
  echo "ERROR: No release tag found in user_message. Example inputs: v1.9.0 or 1.9.0"
  exit 1
fi
if [[ "$TAG" != v* ]]; then TAG="v${TAG}"; fi
VERSION="${TAG#v}"  # image suffix uses this, e.g. 1.9.0-mongo-tls-2
printf 'TAG=%s\nVERSION=%s\n' "$TAG" "$VERSION"
```

### Step 2: Pre-flight checks

Stop on a dirty tree or an in-progress rebase before touching refs.

```bash
if [ -n "$(git status --porcelain)" ]; then
  echo "ERROR: Working tree is dirty. Commit or stash first."; exit 1
fi
if git rev-parse --verify REBASE_HEAD 2>/dev/null; then
  echo "ERROR: Rebase in progress. Run: git rebase --abort OR git rebase --continue"; exit 1
fi
```

### Step 3: Configure and fetch upstream

```bash
UPSTREAM_URL="https://github.com/coroot/coroot-cluster-agent.git"
if ! git remote get-url upstream 2>/dev/null; then
  git remote add upstream "$UPSTREAM_URL"
elif [ "$(git remote get-url upstream)" != "$UPSTREAM_URL" ]; then
  git remote set-url upstream "$UPSTREAM_URL"
fi
git fetch upstream --tags
git fetch origin
git rev-parse --verify "refs/tags/$TAG" >/dev/null 2>&1 || {
  echo "ERROR: Tag $TAG not found. Recent tags:"; git tag -l 'v*' | sort -V | tail -10; exit 1;
}
```

After fetch/preflight, record the exact SHA the shared branch is expected to
have on `origin`. Every later branch push MUST lease against this exact SHA —
never a bare `--force-with-lease`:

```bash
SHARED=add_mongodb_tls-2
EXPECTED_TIP=$(git rev-parse "origin/${SHARED}")
printf 'EXPECTED_TIP=%s\n' "$EXPECTED_TIP"
```

### Step 4: Resolve the exact tag SHA

Never branch from a tag *name* alone; resolve and record the exact commit so
the candidate can always be reproduced:

```bash
TAG_SHA=$(git rev-list -n 1 "refs/tags/$TAG")
printf 'TAG_SHA=%s\n' "$TAG_SHA"
```

### Step 5: Immutable backup ref of the shared branch tip

Create `add_mongodb_tls-2-pre-rebase-<VERSION>` at the current shared-branch
tip. **Fail if a ref with this name already points anywhere else** — it must
stay immutable. Never delete or move it.

```bash
BACKUP_REF="add_mongodb_tls-2-pre-rebase-${VERSION}"
TIP=$(git rev-parse "$SHARED")
if git show-ref --verify --quiet "refs/heads/$BACKUP_REF"; then
  EXISTING=$(git rev-parse "refs/heads/$BACKUP_REF")
  if [ "$EXISTING" != "$TIP" ]; then
    echo "FATAL: ref $BACKUP_REF exists at $EXISTING, expected $TIP — aborting"; exit 1
  fi
  echo "backup ref already correct: $EXISTING"
else
  git branch "$BACKUP_REF" "$TIP"
fi
```

### Step 6: Candidate branch (shared branch stays untouched)

Create or reset the candidate branch from the exact tag SHA. The name follows
the established style `add_mongodb_tls-2-v<VERSION>-candidate` (e.g.
`add_mongodb_tls-2-v1.9.0-candidate`). All rework happens here;
`add_mongodb_tls-2` is only updated at publication time.

```bash
CANDIDATE="add_mongodb_tls-2-v${VERSION}-candidate"
if git show-ref --verify --quiet "refs/heads/$CANDIDATE"; then
  git checkout "$CANDIDATE"
  git reset --hard "$TAG_SHA"   # candidate is disposable; backup ref protects history
else
  git checkout -b "$CANDIDATE" "$TAG_SHA"
fi
```

### Step 7: Detect upstream SNI capability (exact patterns)

Inspect the upstream tag itself. Upstream counts as SNI-capable only when all
three exact indicators exist — SNI must be plumbed through config, target, and
the mongo TLS config. Do NOT use the old heuristic (`crypto/tls` +
`SetTLSConfig` + `mongodb-scrape-param-tls`): it matched v1.9.0, which ships
TLS **without** SNI, producing a false positive.

```bash
SNI_CONFIG=1; SNI_TARGET=1; SNI_MONGO=1
git show "${TAG}:config/types.go"         2>/dev/null | grep -q 'Sni'        && SNI_CONFIG=0
git show "${TAG}:metrics/target.go"       2>/dev/null | grep -q 'Sni'        && SNI_TARGET=0
git show "${TAG}:metrics/mongo/mongo.go"  2>/dev/null | grep -q 'ServerName' && SNI_MONGO=0
printf 'SNI_CONFIG=%s SNI_TARGET=%s SNI_MONGO=%s\n' "$SNI_CONFIG" "$SNI_TARGET" "$SNI_MONGO"
```

**If all three are `0`** (upstream ships SNI): diff the fork against the tag
(`git diff "${TAG_SHA}"...HEAD`), present the behavioral differences, and
**stop for a user decision** (keep fork / retire branch / adapt). Do not
publish an image in this case.

**Otherwise**: rework the fork changes on the candidate (Step 8).

### Step 8: Rework fork changes on the candidate

Re-apply (or cherry-pick and adapt) the fork commits on top of the new base:

- `config.ApplicationInstrumentation.Sni` + `metrics.Target.Sni`, included in
  `Target.Equal`, mapped in `TargetFromConfig`.
- `TargetFromPod` must **never** synthesize SNI (targets are scraped by IP).
- `mongo.New` receives the server name and sets `cfg.ServerName` only when
  non-empty, after `common.DatabaseTLSConfig` succeeds.
- Invalid `tls` param, CA, or client cert must **fail closed** (exporter fails
  to start) — never log-and-continue into a plaintext connection.
- Preserve upstream `common.DatabaseTLSConfig` semantics (CA chain-only via
  `VerifyConnection`, mTLS, skip-verify) and all deep MongoDB monitoring.

Resolve conflicts by keeping upstream code intact and re-expressing the fork
delta on top. A `go.sum`-only conflict auto-resolves with
`git checkout --theirs go.sum && go mod tidy && git add go.sum`.

### Step 9: Verify BEFORE any publication (Go 1.25+)

Verification includes the Docker **build** (not push). The build needs no
publication confirmation and is a pure local verification step. All of the
following must pass before anything is published; a Docker build failure
blocks ALL publication (branch push and Docker push alike):

```bash
go test ./...        # includes config mapping/equality, fail-closed, TLS handshake tests
go build ./...
go vet ./... || true # fix new findings; pre-existing findings are reported, not blocking
git diff --check
docker build --build-arg VERSION="${VERSION}-mongo-tls-2" \
  -t "ghcr.io/tribunadigital/coroot-cluster-agent:${VERSION}-mongo-tls-2" .
```

The image tag is always `ghcr.io/tribunadigital/coroot-cluster-agent:<VERSION>-mongo-tls-2`.

### Step 10: Publication (branch push + Docker push; requires explicit user confirmation)

Do **not** push anything without the user explicitly confirming publication in
the current conversation. Present the exact publication plan and ask for
confirmation, e.g.:

> Publication plan (needs your explicit confirmation):
> 1. `git push --force-with-lease=refs/heads/add_mongodb_tls-2:<EXPECTED_TIP> origin add_mongodb_tls-2-v<VERSION>-candidate:add_mongodb_tls-2`
> 2. `docker push ghcr.io/tribunadigital/coroot-cluster-agent:<VERSION>-mongo-tls-2` (local image already built and verified)
> Image: `ghcr.io/tribunadigital/coroot-cluster-agent:<VERSION>-mongo-tls-2`

Only after explicit confirmation, run exactly these commands:

```bash
# Explicit per-ref lease: fails if origin/add_mongodb_tls-2 moved away from
# EXPECTED_TIP. Never use a bare --force-with-lease.
git push --force-with-lease="refs/heads/${SHARED}:${EXPECTED_TIP}" origin "${CANDIDATE}:${SHARED}"
docker push "ghcr.io/tribunadigital/coroot-cluster-agent:${VERSION}-mongo-tls-2"
docker manifest inspect "ghcr.io/tribunadigital/coroot-cluster-agent:${VERSION}-mongo-tls-2"
```

### Step 11: Summary report

Report: candidate branch + SHA, backup ref + SHA, tag + exact tag SHA,
EXPECTED_TIP, SNI detection result, test/build/vet results, Docker build
result, commits added, files changed, and publication status (published /
awaiting confirmation / blocked / stopped for review).

## Error Handling

| Situation | Action |
|-----------|--------|
| Dirty working tree | Stop; require commit/stash before any ref changes. |
| Rebase in progress | Stop; require `git rebase --abort`/`--continue`. |
| Backup ref exists elsewhere | **Fail loudly**; never move or delete the existing backup ref. |
| Tag not found | Print requested tag + last 10 tags; touch nothing. |
| Upstream ships SNI (all indicators `0`) | Diff and stop for human review; no publication. |
| Broad rework conflicts | Keep upstream semantics; re-express the fork delta; abort with `git rebase --abort` if unresolvable. |
| `go test`/`go build`/`go vet` failure | Fix on candidate; publication stays blocked. |
| Docker build failure | Blocks ALL publication (branch push and Docker push); fix and rerun Step 9. |
| `origin/add_mongodb_tls-2` moved (lease mismatch) | The explicit-lease push fails by design; re-run Step 3 to re-record EXPECTED_TIP, re-verify, and ask for confirmation again. |
| Missing explicit publication confirmation | Do not push branches or images; report "awaiting confirmation". |

## Rollback

The shared branch is never rewritten until the confirmed explicit-lease push,
so rollback is usually a no-op. To discard a candidate:

```bash
git checkout "$SHARED"   # untouched shared branch
# candidate can be deleted/re-created from $TAG_SHA at any time
```

If a publication push already happened and must be reverted, push the backup
ref back with an explicit lease against the exact SHA currently on origin
(explicit confirmation required; never a bare `--force-with-lease`):

```bash
EXPECTED_TIP=$(git rev-parse "origin/${SHARED}")
git push --force-with-lease="refs/heads/${SHARED}:${EXPECTED_TIP}" origin "${BACKUP_REF}:${SHARED}"
```

## Notes

- SNI detection is exact (three required patterns), replacing the old
  `f60c3cd`-era heuristic that false-positived on v1.9.0's TLS-without-SNI.
- Upstream TLS behavior (chain-only CA verification, mTLS, skip-verify,
  authSource, implicit TLS when creds are present) is upstream code — preserve it.
- Docker commands build a single-architecture local image; multi-arch builds,
  CI pipelines, and deployment automation are out of scope.
