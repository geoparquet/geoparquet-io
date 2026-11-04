#!/usr/bin/env python3
"""
Baseline management utilities for gpio benchmarks.

Provides commands to list, download, and compare benchmark baselines
stored in GitHub Actions artifacts.

Usage:
    # List available baselines
    uv run python scripts/manage_baselines.py list

    # Download a specific baseline
    uv run python scripts/manage_baselines.py download v0.9.0

    # Download multiple baselines
    uv run python scripts/manage_baselines.py download v0.9.0 v0.8.0 v0.7.0

    # Compare specific baselines
    uv run python scripts/manage_baselines.py compare v0.8.0 v0.9.0

    # Analyze trends
    uv run python scripts/manage_baselines.py trends v0.7.0 v0.8.0 v0.9.0
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path


def get_github_token() -> str | None:
    """Get GitHub token from environment or gh CLI."""
    # Try environment variable first
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if token:
        return token

    # Try gh CLI
    try:
        result = subprocess.run(
            ["gh", "auth", "token"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass

    return None


def get_repository() -> str:
    """Get repository name from git remote."""
    try:
        result = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            # Extract owner/repo from URL
            url = result.stdout.strip()
            # Handle both HTTPS and SSH URLs
            if url.startswith("git@"):
                # git@github.com:owner/repo.git
                parts = url.split(":")
                if len(parts) == 2:
                    repo = parts[1].replace(".git", "")
                    return repo
            elif "github.com" in url:
                # https://github.com/owner/repo.git
                parts = url.split("github.com/")
                if len(parts) == 2:
                    repo = parts[1].replace(".git", "")
                    return repo
    except Exception:
        pass

    return "geoparquet/geoparquet-io"  # Default


def list_baselines(repo: str, token: str | None = None) -> list[dict]:
    """List available benchmark baseline artifacts."""
    env = os.environ.copy()
    if token:
        env["GH_TOKEN"] = token

    try:
        # Use gh CLI to list artifacts
        result = subprocess.run(
            [
                "gh",
                "api",
                f"repos/{repo}/actions/artifacts",
                "--jq",
                '.artifacts[] | select(.name | startswith("release-benchmark-")) | {name, id, created_at, expired}',
            ],
            capture_output=True,
            text=True,
            env=env,
            timeout=30,
        )

        if result.returncode != 0:
            print(f"Error listing artifacts: {result.stderr}", file=sys.stderr)
            return []

        # Parse JSON lines
        artifacts = []
        for line in result.stdout.strip().split("\n"):
            if line:
                try:
                    artifacts.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

        return artifacts

    except Exception as e:
        print(f"Error listing baselines: {e}", file=sys.stderr)
        return []


def download_baseline(
    version: str, repo: str, output_dir: Path, token: str | None = None
) -> Path | None:
    """Download a specific baseline artifact."""
    env = os.environ.copy()
    if token:
        env["GH_TOKEN"] = token

    artifact_name = f"release-benchmark-{version}"
    print(f"Downloading baseline for {version}...")

    try:
        # Get artifact ID
        result = subprocess.run(
            [
                "gh",
                "api",
                f"repos/{repo}/actions/artifacts",
                "--jq",
                f'.artifacts[] | select(.name == "{artifact_name}") | .id',
            ],
            capture_output=True,
            text=True,
            env=env,
            timeout=30,
        )

        if result.returncode != 0 or not result.stdout.strip():
            print(f"  Artifact not found for {version}", file=sys.stderr)
            return None

        artifact_id = result.stdout.strip().split("\n")[0]

        # Download artifact
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            tmp_path = tmp.name

        subprocess.run(
            ["gh", "api", f"repos/{repo}/actions/artifacts/{artifact_id}/zip"],
            stdout=open(tmp_path, "wb"),
            env=env,
            timeout=60,
            check=True,
        )

        # Extract results file
        output_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(tmp_path, "r") as zf:
            # Find results JSON
            results_files = [name for name in zf.namelist() if name.startswith("results_")]
            if not results_files:
                print("  No results file found in artifact", file=sys.stderr)
                os.unlink(tmp_path)
                return None

            # Extract first results file
            results_file = results_files[0]
            zf.extract(results_file, output_dir)

            # Rename to standardized name
            extracted = output_dir / results_file
            final_path = output_dir / f"results_{version}.json"
            extracted.rename(final_path)

        os.unlink(tmp_path)
        print(f"  ✓ Downloaded to {final_path}")
        return final_path

    except Exception as e:
        print(f"  Error downloading baseline: {e}", file=sys.stderr)
        return None


def cmd_list(args):
    """List available baselines."""
    token = get_github_token()
    repo = args.repo or get_repository()

    artifacts = list_baselines(repo, token)

    if not artifacts:
        print("No benchmark baselines found.")
        return

    print(f"\nAvailable baselines in {repo}:")
    print(f"{'Version':<20} {'Created':<25} {'Artifact ID':<12} {'Status':<10}")
    print("-" * 70)

    for artifact in artifacts:
        # Extract version from artifact name
        version = artifact["name"].replace("release-benchmark-", "")
        created = artifact["created_at"][:19].replace("T", " ")
        artifact_id = str(artifact["id"])
        status = "expired" if artifact.get("expired") else "available"

        print(f"{version:<20} {created:<25} {artifact_id:<12} {status:<10}")

    print()


def cmd_download(args):
    """Download baselines."""
    token = get_github_token()
    repo = args.repo or get_repository()
    output_dir = Path(args.output_dir)

    if not token:
        print(
            "Error: GitHub token required. Set GITHUB_TOKEN or authenticate with gh CLI.",
            file=sys.stderr,
        )
        sys.exit(1)

    for version in args.versions:
        download_baseline(version, repo, output_dir, token)


def cmd_compare(args):
    """Compare two baselines."""
    # Download baselines if needed
    baseline_dir = Path(args.baseline_dir)
    baseline_dir.mkdir(parents=True, exist_ok=True)

    files = []
    for version in [args.version1, args.version2]:
        filepath = baseline_dir / f"results_{version}.json"
        if not filepath.exists():
            print(f"Baseline for {version} not found locally, downloading...")
            token = get_github_token()
            repo = args.repo or get_repository()
            filepath = download_baseline(version, repo, baseline_dir, token)
            if not filepath:
                print(f"Failed to download baseline for {version}", file=sys.stderr)
                sys.exit(1)
        files.append(filepath)

    # Run comparison
    result = subprocess.run(
        [
            "uv",
            "run",
            "python",
            "scripts/benchmark_history.py",
            "--compare",
            str(files[0]),
            str(files[1]),
        ],
        check=False,
    )
    sys.exit(result.returncode)


def cmd_trends(args):
    """Analyze trends across multiple baselines."""
    # Download baselines if needed
    baseline_dir = Path(args.baseline_dir)
    baseline_dir.mkdir(parents=True, exist_ok=True)

    files = []
    for version in args.versions:
        filepath = baseline_dir / f"results_{version}.json"
        if not filepath.exists():
            print(f"Baseline for {version} not found locally, downloading...")
            token = get_github_token()
            repo = args.repo or get_repository()
            filepath = download_baseline(version, repo, baseline_dir, token)
            if not filepath:
                print(f"Failed to download baseline for {version}", file=sys.stderr)
                sys.exit(1)
        files.append(str(filepath))

    # Run trend analysis
    cmd = ["uv", "run", "python", "scripts/benchmark_history.py", "--trend"] + files
    if args.threshold:
        cmd.extend(["--trend-threshold", str(args.threshold)])

    result = subprocess.run(cmd, check=False)
    sys.exit(result.returncode)


def main():
    parser = argparse.ArgumentParser(
        description="Manage benchmark baselines stored in GitHub artifacts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--repo",
        help="Repository name (owner/repo). Auto-detected from git remote if not provided.",
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # List command
    subparsers.add_parser("list", help="List available baselines")

    # Download command
    download_parser = subparsers.add_parser("download", help="Download baselines")
    download_parser.add_argument("versions", nargs="+", help="Version tags to download")
    download_parser.add_argument(
        "-o",
        "--output-dir",
        default="baselines",
        help="Output directory for baselines (default: baselines/)",
    )

    # Compare command
    compare_parser = subparsers.add_parser("compare", help="Compare two baselines")
    compare_parser.add_argument("version1", help="First version to compare")
    compare_parser.add_argument("version2", help="Second version to compare")
    compare_parser.add_argument(
        "-d",
        "--baseline-dir",
        default="baselines",
        help="Directory containing baselines (default: baselines/)",
    )

    # Trends command
    trends_parser = subparsers.add_parser("trends", help="Analyze trends across baselines")
    trends_parser.add_argument(
        "versions", nargs="+", help="Version tags to analyze (oldest to newest)"
    )
    trends_parser.add_argument(
        "-d",
        "--baseline-dir",
        default="baselines",
        help="Directory containing baselines (default: baselines/)",
    )
    trends_parser.add_argument(
        "-t",
        "--threshold",
        type=float,
        help="Degradation threshold (default: 0.05 = 5%%)",
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "list":
        cmd_list(args)
    elif args.command == "download":
        cmd_download(args)
    elif args.command == "compare":
        cmd_compare(args)
    elif args.command == "trends":
        cmd_trends(args)


if __name__ == "__main__":
    main()
