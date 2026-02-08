---
description: Release the tsquery-parser-codegen tool — build, test, commit, tag, and push a new version
disable-model-invocation: true
argument-hint: <version>
---

# Release tsquery-parser-codegen

Run the full tsquery-parser-codegen release pipeline for version `$ARGUMENTS`.

## Steps

### 1. Validate version

- If `$ARGUMENTS` is empty, stop and ask the user for a version number.
- Verify that `$ARGUMENTS` matches the pattern `X.Y.Z` (digits only, e.g. `0.4.25`). If not, stop and tell the user the format is invalid.

Set `VERSION` = `$ARGUMENTS` for the remaining steps.

### 2. Refresh codegen tool embedded files

Run `bash build.sh` from:
```
utils/timeseries/tsquery/queryopenapi/cmd/tsquery-parser-codegen/
```

Stop if the script fails.

### 3. Regenerate codegen testdata

Run `bash generate.sh` from:
```
utils/timeseries/tsquery/queryopenapi/codegen/testdata/
```

Stop if the script fails.

### 4. Test codegen testdata module

Run `go test ./...` from:
```
utils/timeseries/tsquery/queryopenapi/codegen/testdata/
```

This directory has its own `go.mod`. Stop if tests fail.

### 5. Full build and test

From the repository root, run:
```
go build ./...
go test ./...
```

Stop if either command fails.

### 6. Show changes for review

Run `git status` and `git diff --stat` and display the output so the user can review what changed.

### 7. Commit (ask for confirmation)

**Stop and ask the user for confirmation before proceeding.** Show a suggested commit message like:

> Release v$VERSION

Let the user approve, modify, or provide their own message. Then stage all changes and commit.

### 8. Tag

Create both tags:
- `vVERSION` — repo-level tag
- `utils/timeseries/tsquery/queryopenapi/cmd/tsquery-parser-codegen/vVERSION` — tool-scoped tag

### 9. Push (ask for confirmation)

**Stop and ask the user for confirmation before pushing.** Then push the commit and both tags to origin:
```
git push origin HEAD
git push origin vVERSION
git push origin utils/timeseries/tsquery/queryopenapi/cmd/tsquery-parser-codegen/vVERSION
```
