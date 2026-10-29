#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Compile requirements/<job>.in into requirements/<job>.txt: every transitive dependency pinned with its hashes, one
# file per CI job, so CI installs exactly the tested environment. Existing pins are kept unless a pyproject.toml
# changed; pass --upgrade to move every transitive dependency to its latest release. CI runs this and fails on a diff.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
for job in gates spark spark35; do
  uv pip compile "requirements/$job.in" -o "requirements/$job.txt" --universal --python-version 3.11 \
    --generate-hashes --no-emit-package cleanroom-sql --no-emit-package opencleanroom --quiet "$@"
done
