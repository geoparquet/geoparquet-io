#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# apply_branch_protection.sh
#
# WHAT THIS DOES
#   Migrates geoparquet/geoparquet-io from legacy "classic" branch protection
#   to GitHub repository rulesets, and enables repo-level auto-merge.
#
#   It is a one-shot admin task: run it ONCE after the CI-hardening PR merges.
#
#   Concretely it:
#     1. Enables auto-merge on the repository.
#     2. Deletes the classic branch-protection rule on `main` (if present).
#     3. Creates (or updates) ONE branch ruleset targeting `main`:
#          A. "main: PR + green checks" - forces PR + passing status checks
#             for EVERYONE (no bypass actors, including admins).
#     4. Deletes the retired "main: review required" ruleset if it is still
#        present. See section 3b for why it went away.
#     5. Prints a verification summary.
#
# REQUIREMENTS
#   - `gh` CLI authenticated as a user with ADMIN permission on the repo.
#     (Ruleset + branch-protection + repo-settings writes all require admin.)
#   - `jq` on PATH (used to build/validate the JSON payloads and parse output).
#
# SAFE TO RE-RUN
#   Idempotent. Rulesets are matched by NAME via a GET, then updated with PUT
#   if they already exist, or created with POST if they don't. Re-running
#   converges the live config to what this script declares - it will not create
#   duplicate rulesets. Enabling auto-merge and deleting classic protection are
#   likewise no-ops on a second run.
#
# RELATED MANUAL STEP (NOT done by this script)
#   The security-audit bot opens/updates PRs and needs a token that can bypass
#   the rulesets' identity. Create a fine-grained Personal Access Token scoped
#   to THIS repository only, with:
#       - Contents:      Read and write
#       - Pull requests: Read and write
#   and store it as the repository Actions secret named BOT_PR_TOKEN.
#   (A fine-grained PAT is preferred over a classic PAT so the grant stays
#   limited to geoparquet/geoparquet-io.)
#
#   NOTE: a fine-grained PAT must name the geoparquet ORG as its resource
#   owner, which requires the org to have opted in to fine-grained PAT
#   access (org owners: Settings -> Third-party Access -> Personal access
#   tokens). If the org does not appear in your resource-owner dropdown,
#   fall back to a classic PAT with the `repo` scope - it works with plain
#   repo write access, at the cost of covering every repo your account can
#   reach. Either way, store it with:
#       gh secret set BOT_PR_TOKEN --repo geoparquet/geoparquet-io
# =============================================================================

REPO="geoparquet/geoparquet-io"

# -----------------------------------------------------------------------------
# 1. Enable repository auto-merge.
# -----------------------------------------------------------------------------
echo ">> Enabling auto-merge on ${REPO} ..."
gh api -X PATCH "repos/${REPO}" -F allow_auto_merge=true >/dev/null

# -----------------------------------------------------------------------------
# 2. Delete classic branch protection on main (tolerate 404 = already gone).
# -----------------------------------------------------------------------------
echo ">> Removing classic branch protection on main (if present) ..."
if gh api "repos/${REPO}/branches/main/protection" >/dev/null 2>&1; then
  gh api -X DELETE "repos/${REPO}/branches/main/protection" >/dev/null
  echo "   classic protection deleted."
else
  echo "   no classic protection found (nothing to delete)."
fi

# -----------------------------------------------------------------------------
# Helper: create-or-update a ruleset by name.
#   $1 = ruleset name
#   $2 = full ruleset JSON payload (must contain matching "name")
# Looks the name up in the existing rulesets; PUTs to update if found,
# POSTs to create otherwise. This name-keyed lookup is the idempotency
# mechanism that makes the whole script safe to re-run.
# -----------------------------------------------------------------------------
apply_ruleset() {
  local name="$1"
  local payload="$2"
  local existing_id

  existing_id="$(
    gh api --paginate "repos/${REPO}/rulesets" \
      --jq ".[] | select(.name == \"${name}\") | .id" 2>/dev/null || true
  )"

  if [[ -n "${existing_id}" ]]; then
    echo ">> Updating ruleset '${name}' (id ${existing_id}) ..."
    printf '%s' "${payload}" \
      | gh api -X PUT "repos/${REPO}/rulesets/${existing_id}" \
          --input - >/dev/null
  else
    echo ">> Creating ruleset '${name}' ..."
    printf '%s' "${payload}" \
      | gh api -X POST "repos/${REPO}/rulesets" \
          --input - >/dev/null
  fi
}

# -----------------------------------------------------------------------------
# Helper: delete a ruleset by name, if it exists.
#   $1 = ruleset name
# Used to retire rulesets this script no longer declares, so that a re-run
# converges an older configuration instead of leaving orphans behind.
# -----------------------------------------------------------------------------
delete_ruleset() {
  local name="$1"
  local existing_id

  existing_id="$(
    gh api --paginate "repos/${REPO}/rulesets" \
      --jq ".[] | select(.name == \"${name}\") | .id" 2>/dev/null || true
  )"

  if [[ -n "${existing_id}" ]]; then
    echo ">> Deleting retired ruleset '${name}' (id ${existing_id}) ..."
    gh api -X DELETE "repos/${REPO}/rulesets/${existing_id}" >/dev/null
  else
    echo ">> Retired ruleset '${name}' not present (nothing to delete)."
  fi
}

# -----------------------------------------------------------------------------
# 3a. Ruleset A: "main: PR + green checks"
#
#     enforcement: active, NO bypass actors -> applies to EVERYONE, admins
#     included. Requires a PR (0 approvals) and both required status checks
#     green, and blocks force-pushes / deletion of main.
#
#     The required contexts are exactly two (#1367):
#       - all-checks: the aggregator job in .github/workflows/tests.yml. It
#         `needs:` every other job in that workflow and fails unless each one
#         passed (slow-tests and network-tests may be skipped, by design), so
#         a job added to tests.yml becomes a gate without editing this script.
#         tests/test_ci_wiring.py keeps its `needs:` list equal to the job
#         list. It replaces the 13 contexts (lint, security, notebooks and ten
#         `test (os, py)` cells) this ruleset used to name one by one, which
#         left every job added after them advisory.
#       - pr-title: .github/workflows/pr-title.yml. It stays its own workflow
#         and its own context because it must rerun on a title edit
#         (`pull_request: edited`); folding it into tests.yml would re-run the
#         whole suite on every edit.
#
#     integration_id is intentionally omitted from each required check so a
#     check with that name counts no matter which app/source reports it.
#     strict_required_status_checks_policy=false: PR branch need not be
#     up-to-date with main before merging.
# -----------------------------------------------------------------------------
RULESET_A_NAME="main: PR + green checks"
RULESET_A_PAYLOAD="$(
  jq -n --arg name "${RULESET_A_NAME}" '
  {
    name: $name,
    target: "branch",
    enforcement: "active",
    bypass_actors: [],
    conditions: {
      ref_name: {
        include: ["~DEFAULT_BRANCH"],
        exclude: []
      }
    },
    rules: [
      {
        type: "pull_request",
        parameters: {
          required_approving_review_count: 0,
          dismiss_stale_reviews_on_push: false,
          require_code_owner_review: false,
          require_last_push_approval: false,
          required_review_thread_resolution: false
        }
      },
      {
        type: "required_status_checks",
        parameters: {
          strict_required_status_checks_policy: false,
          required_status_checks: [
            {context: "all-checks"},
            {context: "pr-title"}
          ]
        }
      },
      { type: "non_fast_forward" },
      { type: "deletion" }
    ]
  }'
)"

# -----------------------------------------------------------------------------
# 3b. (removed) Ruleset B: "main: review required"
#
#     A second ruleset used to require 1 approving review, with bypass actors
#     for the admin role. It was deleted on 2026-08-13. This repository has a
#     single active reviewer, and GitHub disables self-approval, so the rule
#     could never be satisfied on that reviewer's own PRs. Every merge became
#     a "merge without waiting for requirements" click, which is a step
#     rather than a second opinion.
#
#     Neither bot nor draft PRs depended on it. Dependabot auto-merge is
#     controlled by .github/workflows/dependabot-automerge.yml, and GitHub
#     refuses to enable auto-merge on a draft PR at all.
#
#     To reinstate a review requirement, build a second payload here, call
#     apply_ruleset with it, and drop the delete_ruleset call below.
# -----------------------------------------------------------------------------
STALE_RULESET_NAME="main: review required"

# -----------------------------------------------------------------------------
# NET EFFECT
#   Ruleset A carries no bypass actors, so everyone - contributors and admins
#   alike - must open a PR and get both required checks (all-checks and
#   pr-title) green before anything lands on main. Nobody pushes straight to
#   main, force-pushes it, or deletes it.
#
#   No approving review is required. Contributors without write access still
#   cannot merge their own PRs, because merging needs write access.
# -----------------------------------------------------------------------------

apply_ruleset "${RULESET_A_NAME}" "${RULESET_A_PAYLOAD}"
delete_ruleset "${STALE_RULESET_NAME}"

# -----------------------------------------------------------------------------
# 5. Verification.
# -----------------------------------------------------------------------------
echo ""
echo ">> Rulesets now on ${REPO}:"
gh api "repos/${REPO}/rulesets" \
  --jq '.[] | "   id=\(.id)  name=\(.name)  enforcement=\(.enforcement)"'

echo ""
echo -n ">> allow_auto_merge = "
gh api "repos/${REPO}" --jq .allow_auto_merge

echo ""
echo ">> Done. Remember to create the BOT_PR_TOKEN repo secret (fine-grained"
echo "   PAT, contents+PRs write, this repo only; or a classic PAT with the"
echo "   'repo' scope if the org hasn't enabled fine-grained PAT access)."
