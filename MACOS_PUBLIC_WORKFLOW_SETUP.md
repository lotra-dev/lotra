# Safe public macOS build workflow

`.github/workflows/build-macos.yml` runs only from the public `lotra-dev/lotra`
repository, so its standard `macos-15` job does not consume the private
repository's Actions-minute allowance.

The source remains in the private `lotra-dev/lotra-build-private` repository.
The workflow uses a short-lived GitHub App token with read-only Contents access
to that repository, checks out only the requested reviewed ref, and uploads
only `Lotra-macOS-Apple-Silicon.zip` to an existing GitHub release. It does not
upload source files or use signing credentials.

## One-time GitHub setup

1. Create a GitHub App for the `lotra-dev` account with only repository
   **Contents: Read-only** permission.
2. Install that App only on `lotra-build-private`.
3. In the public `lotra` repository, create an environment named
   `macos-release`. Add required reviewers to that environment and add these
   environment secrets:
   - `LOTRA_BUILDER_APP_ID` — the App's numeric ID.
   - `LOTRA_BUILDER_APP_PRIVATE_KEY` — the App's PEM private key.
4. Protect the public repository's `main` branch and require review for changes
   to `.github/workflows/`. Keep this workflow manual-only; never add a pull
   request trigger.

## Running a Mac release from Windows

Publish the Windows asset and create the GitHub release first. Then run:

```powershell
gh workflow run build-macos.yml `
  --repo lotra-dev/lotra `
  -f release_tag=lotra-v1.21.2 `
  -f builder_ref=codex/v1.21.2
```

The workflow validates that the builder's `pyproject.toml` and runtime release
match `release_tag`, builds an unsigned Apple Silicon Nuitka app, verifies the
existing release, and uploads the Mac archive directly. A signed build should
remain on a private workflow or a controlled Mac because signing secrets must
not be exposed to a public workflow.
