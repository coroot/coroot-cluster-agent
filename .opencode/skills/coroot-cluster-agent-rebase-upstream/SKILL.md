---
name: coroot-cluster-agent-rebase-upstream
description: Rebase add_mongodb_tls-2 branch onto upstream coroot-cluster-agent tag, check for upstream MongoDB TLS support, build and push Docker image
---

# coroot-cluster-agent — Rebase Upstream

Rebases the MongoDB TLS fork onto an upstream release tag, detects whether upstream already ships equivalent TLS support, and only builds/pushes the fork image when the fork still adds needed behavior.

## Prerequisites

- Run inside the fork repository: `github.com/tribunadigital/coroot-cluster-agent`
- `git`, `go 1.24+`, `docker`, and network access to GitHub and GHCR
- Authenticated Docker session for `ghcr.io`
- Clean working tree on the branch you will rebase
- Target branch is `add_mongodb_tls-2`
- Upstream remote URL must be `https://github.com/coroot/coroot-cluster-agent.git`
- User message includes a release version such as `v1.5.3` or `1.5.3`

## Procedure

### Step 1: Validate input

Extract the release version from `user_message`, normalize it to an upstream tag, and derive the Docker image version without the `v` prefix.

```bash
USER_MESSAGE="${USER_MESSAGE:-$1}"
TAG=$(printf '%s\n' "$USER_MESSAGE" | grep -oE 'v?[0-9]+\.[0-9]+\.[0-9]+' | head -n1)
if [ -z "$TAG" ]; then
  echo "ERROR: No release tag found in user_message. Example inputs: v1.5.3 or 1.5.3"
  exit 1
fi

# Parse version from user message
TAG="$TAG"  # e.g., "v1.5.3" or "1.5.3"
# Normalize: ensure TAG starts with 'v'
if [[ "$TAG" != v* ]]; then TAG="v${TAG}"; fi
# Derive Docker version (no 'v' prefix)
VERSION="${TAG#v}"  # e.g., "1.5.3"

printf 'TAG=%s\nVERSION=%s\n' "$TAG" "$VERSION"
```

### Step 2: Pre-flight checks

Stop on a dirty tree or an in-progress rebase before touching remotes or tags.

```bash
# Check working tree is clean
if [ -n "$(git status --porcelain)" ]; then
  echo "ERROR: Working tree is dirty. Stash or commit changes first: git stash"; exit 1
fi
# Check no rebase in progress
if git rev-parse --verify REBASE_HEAD 2>/dev/null; then
  echo "ERROR: Rebase in progress. Run: git rebase --abort OR git rebase --continue"; exit 1
fi

git status --short
```

### Step 3: Configure upstream remote

Create the `upstream` remote if missing, repair it if it points to the wrong repository, and verify the final URL.

```bash
UPSTREAM_URL="https://github.com/coroot/coroot-cluster-agent.git"
if ! git remote get-url upstream 2>/dev/null; then
  git remote add upstream "$UPSTREAM_URL"
elif [ "$(git remote get-url upstream)" != "$UPSTREAM_URL" ]; then
  git remote set-url upstream "$UPSTREAM_URL"
fi
git remote -v  # verify
```

### Step 4: Fetch upstream and validate tag

Fetch upstream tags and fail early if the requested tag does not exist.

```bash
git fetch upstream --tags
if ! git rev-parse --verify "refs/tags/$TAG" 2>/dev/null; then
  echo "ERROR: Tag $TAG not found. Available recent tags:"
  git tag -l 'v*' | sort -V | tail -10
  exit 1
fi

git rev-list -n 1 "$TAG"
```

### Step 5: Checkout branch and save rollback point

Check out `add_mongodb_tls-2`, creating it from `origin` when it only exists remotely, then capture the rollback SHA.

```bash
if git show-ref --verify --quiet refs/heads/add_mongodb_tls-2; then
  git checkout add_mongodb_tls-2
else
  git checkout -b add_mongodb_tls-2 origin/add_mongodb_tls-2
fi
PRE_REBASE_SHA=$(git rev-parse HEAD)
git log --oneline -5
printf 'PRE_REBASE_SHA=%s\n' "$PRE_REBASE_SHA"
```

### Step 6: Rebase onto upstream tag

Run the rebase and branch on success, no-op, `go.sum`-only conflict, or a broader manual conflict.

```bash
git rebase "$TAG"
REBASE_EXIT=$?
if [ $REBASE_EXIT -eq 0 ]; then
  NEW_SHA=$(git rev-parse HEAD)
  if [ "$NEW_SHA" = "$PRE_REBASE_SHA" ]; then
    echo "No-op: branch is already based on $TAG. Skipping to Step 8."
  else
    echo "Rebase successful."
    git log --oneline -5
  fi
else
  CONFLICTED=$(git diff --name-only --diff-filter=U)
  echo "Conflicts in: $CONFLICTED"
  git ls-files -u
  if [ "$CONFLICTED" = "go.sum" ]; then
    echo "Only go.sum conflict — can auto-resolve:"
    echo "  git checkout --theirs go.sum && go mod tidy && git add go.sum && git rebase --continue"
    exit 1
  else
    echo "Manual resolution required:"
    echo "  1. Edit conflicted files, then: git rebase --continue"
    echo "  2. Or abort: git rebase --abort"
    echo "  Rollback point: git reset --hard $PRE_REBASE_SHA"
    exit 1
  fi
fi
```

### Step 7: Update dependencies

Recompute module files after the rebase, amend the current commit if `go.mod` or `go.sum` changed, and compile-check the full module.

```bash
go mod tidy
if ! git diff --quiet go.mod go.sum; then
  git add go.mod go.sum
  git commit --amend --no-edit
fi
go build ./...  # compile check
```

### Step 8: Check upstream MongoDB TLS support

Inspect the upstream tag itself, not the rebased branch, and require all three TLS indicators to count as supported.

```bash
TLS_CRYPTO=1; TLS_CONFIG=1; TLS_ANNOTATION=1
git show "${TAG}:metrics/mongo/mongo.go" 2>/dev/null | grep -q "crypto/tls" && TLS_CRYPTO=0
git show "${TAG}:metrics/mongo/mongo.go" 2>/dev/null | grep -q "SetTLSConfig" && TLS_CONFIG=0
git show "${TAG}:metrics/target.go" 2>/dev/null | grep -q "mongodb-scrape-param-tls" && TLS_ANNOTATION=0

printf 'TLS_CRYPTO=%s TLS_CONFIG=%s TLS_ANNOTATION=%s\n' "$TLS_CRYPTO" "$TLS_CONFIG" "$TLS_ANNOTATION"
```

### Step 9: Act on detection result

Use the detection outcome to decide whether to stop for human review or publish a new fork image.

**If upstream SUPPORTS MongoDB TLS** (all three variables are `0`):

```bash
git show "${TAG}:metrics/mongo/mongo.go"
git show "${TAG}:metrics/target.go"
git show "${TAG}:config/types.go"
git diff "${TAG}...HEAD"
```

Explain what upstream added and stop for a user decision:

- **Keep our fork**: upstream implementation differs, our changes still needed
- **Retire branch**: upstream fully covers our needs
- **Adapt manually**: take best of both, requires manual review

Do not build or push a Docker image in this branch of the workflow.

**If upstream does NOT support MongoDB TLS**:

```bash
git push --force-with-lease origin add_mongodb_tls-2
docker build --build-arg VERSION="${VERSION}-mongo-tls-2" \
  -t "ghcr.io/tribunadigital/coroot-cluster-agent:${VERSION}-mongo-tls-2" .
docker images "ghcr.io/tribunadigital/coroot-cluster-agent:${VERSION}-mongo-tls-2"
docker push "ghcr.io/tribunadigital/coroot-cluster-agent:${VERSION}-mongo-tls-2"
docker manifest inspect "ghcr.io/tribunadigital/coroot-cluster-agent:${VERSION}-mongo-tls-2"
```

### Step 10: Summary report

Print the rebase result, rollback SHA, TLS detection result, branch status, and Docker image reference or stop reason.

```bash
git status --short
printf 'Tag: %s\nVersion: %s\nRollback: %s\n' "$TAG" "$VERSION" "$PRE_REBASE_SHA"
printf 'TLS detection: crypto=%s config=%s annotation=%s\n' "$TLS_CRYPTO" "$TLS_CONFIG" "$TLS_ANNOTATION"
printf 'Image: ghcr.io/tribunadigital/coroot-cluster-agent:%s-mongo-tls-2\n' "$VERSION"
```

## Error Handling

| Situation | Action |
|-----------|--------|
| Dirty working tree | Stop and print `git stash` or commit instructions before any fetch/rebase work. |
| Rebase in progress | Stop and require `git rebase --abort` or `git rebase --continue` before restarting the skill. |
| `upstream` remote points to the wrong URL | Repair it with `git remote set-url upstream "$UPSTREAM_URL"`, then re-run `git remote -v`. |
| Network failure during `git fetch upstream --tags` | Retry once after confirming connectivity; if it still fails, stop before any branch changes. |
| Tag not found in upstream | Print the requested tag, show the last 10 version tags, and exit without touching the branch. |
| Branch `add_mongodb_tls-2` missing locally | Create it from `origin/add_mongodb_tls-2`; if the remote branch is also missing, stop and ask for the correct branch source. |
| Rebase no-op | Report that the branch already sits on the target tag and continue at Step 8 without rewriting history. |
| `go.sum`-only conflict | Present the exact auto-resolution command: `git checkout --theirs go.sum && go mod tidy && git add go.sum && git rebase --continue`. |
| Any other rebase conflict | Stop for manual resolution, show conflicted files, keep `PRE_REBASE_SHA`, and offer `git rebase --abort` plus `git reset --hard "$PRE_REBASE_SHA"`. |
| `go build ./...` fails after rebase | Stop before push/build, fix compile errors on the branch, and rerun Step 7. |
| Docker build fails | Stop before push, inspect the build log, fix the branch or Docker context, and rerun Step 9. |
| Docker push auth failure | Re-authenticate to GHCR, verify repository permissions, and rerun `docker push` plus `docker manifest inspect`. |

## Rollback

Return the branch to the saved pre-rebase state, update the remote branch with lease protection, and delete the local image tag if it was built.

If a rebase is in progress, first run: `git rebase --abort`

```bash
git reset --hard "$PRE_REBASE_SHA"
git push --force-with-lease origin add_mongodb_tls-2
docker rmi "ghcr.io/tribunadigital/coroot-cluster-agent:${VERSION}-mongo-tls-2" 2>/dev/null || true
```

## Notes

- TLS detection is heuristic: upstream counts as supported only when `crypto/tls`, `SetTLSConfig`, and `mongodb-scrape-param-tls` all exist on the upstream tag.
- The support branch in Step 9 stops for human review because matching grep patterns do not prove behavior parity.
- Docker commands here build a single-architecture local image; multi-architecture builds, CI pipelines, and deployment automation are out of scope.
