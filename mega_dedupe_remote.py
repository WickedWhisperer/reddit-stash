from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple


def rclone_lsjson(remote: str) -> List[dict]:
    proc = subprocess.run(
        ["rclone", "lsjson", "-R", "--files-only", remote],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(proc.stdout)


def sha256_of_remote_file(remote_path: str, tmp_dir: str) -> str:
    local_tmp = Path(tmp_dir) / "candidate.tmp"
    if local_tmp.exists():
        local_tmp.unlink()
    subprocess.run(["rclone", "copyto", remote_path, str(local_tmp)], check=True)
    h = hashlib.sha256()
    with open(local_tmp, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    local_tmp.unlink(missing_ok=True)
    return h.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("remote", help="rclone remote:path to scan, e.g. mega:reddit")
    parser.add_argument("--apply", action="store_true", help="Actually delete confirmed duplicates from the remote")
    parser.add_argument("--manifest", default=None, help="Optional path to write deleted relative paths to")
    parser.add_argument(
        "--skip-verify",
        action="store_true",
        help="Trust name+size match without downloading to verify (faster, slightly riskier)",
    )
    parser.add_argument(
        "--keep-newest",
        action="store_true",
        help="Keep the most recently modified file in each group instead of the alphabetically-first path",
    )
    args = parser.parse_args()

    print(f"Listing {args.remote} (metadata only, no content transfer)...")
    entries = rclone_lsjson(args.remote)
    print(f"Found {len(entries)} file(s).")

    groups: Dict[Tuple[str, int], List[dict]] = defaultdict(list)
    for e in entries:
        key = (Path(e["Path"]).name, e["Size"])
        groups[key].append(e)

    dup_groups = {k: v for k, v in groups.items() if len(v) > 1}
    candidate_file_count = sum(len(v) for v in dup_groups.values())
    print(
        f"{len(dup_groups)} candidate duplicate group(s) by filename+size "
        f"({candidate_file_count} file(s) involved, out of {len(entries)} total)."
    )

    to_delete: List[str] = []

    with tempfile.TemporaryDirectory() as tmp:
        for (name, size), items in dup_groups.items():
            if args.keep_newest:
                items_sorted = sorted(items, key=lambda e: e.get("ModTime", ""), reverse=True)
            else:
                items_sorted = sorted(items, key=lambda e: e["Path"])

            keeper = items_sorted[0]
            candidates = items_sorted[1:]

            if not args.skip_verify:
                verified = []
                keeper_hash = sha256_of_remote_file(f"{args.remote}/{keeper['Path']}", tmp)
                for e in candidates:
                    dupe_hash = sha256_of_remote_file(f"{args.remote}/{e['Path']}", tmp)
                    if dupe_hash == keeper_hash:
                        verified.append(e)
                    else:
                        print(f"  NOT a true duplicate (same name+size, different hash): {e['Path']}")
                candidates = verified

            for e in candidates:
                print(f"DUPLICATE: {e['Path']}  (keeping {keeper['Path']})")
                to_delete.append(e["Path"])

    print(f"\n{len(to_delete)} confirmed duplicate file(s).")

    if args.manifest:
        manifest_path = Path(args.manifest)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text("\n".join(to_delete) + ("\n" if to_delete else ""), encoding="utf-8")
        print(f"Wrote manifest to {manifest_path}")

    if args.apply:
        for rel in to_delete:
            remote_path = f"{args.remote}/{rel}"
            print(f"Deleting {remote_path}")
            subprocess.run(["rclone", "deletefile", remote_path], check=True)
        print(f"Deleted {len(to_delete)} duplicate file(s) from {args.remote}.")
    else:
        print("Dry run only — nothing was deleted. Re-run with --apply to delete.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
