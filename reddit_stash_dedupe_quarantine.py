from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
from typing import Dict, List, Tuple

CHUNK_SIZE = 1024 * 1024


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def is_within_quarantine(path: Path, quarantine_root: Path) -> bool:
    try:
        path.resolve().relative_to(quarantine_root.resolve())
        return True
    except Exception:
        return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Move exact duplicate files into a quarantine folder for manual inspection."
    )
    parser.add_argument(
        "root",
        nargs="?",
        default="reddit",
        help="Archive root directory to scan",
    )
    parser.add_argument(
        "--quarantine",
        default="_duplicates_quarantine",
        help="Folder created under the root that receives duplicates",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually move duplicates into quarantine",
    )
    parser.add_argument(
        "--keep-oldest",
        action="store_true",
        help="Keep the oldest file in each duplicate set instead of the first path in sorted order",
    )
    parser.add_argument(
        "--manifest",
        default=None,
        help=(
            "Optional path to write a newline-delimited list of quarantined "
            "files' original relative paths (only written with --apply). "
            "Lets a caller (e.g. a CI workflow) delete the same files from "
            "remote storage after quarantining them locally."
        ),
    )
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    if not root.exists():
        raise SystemExit(f"Root does not exist: {root}")

    quarantine_root = (root / args.quarantine).resolve()
    quarantine_root.mkdir(parents=True, exist_ok=True)

    groups: Dict[Tuple[str, int], List[Path]] = {}

    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.name == "file_log.json":
            continue
        if is_within_quarantine(path, quarantine_root):
            continue

        size = path.stat().st_size
        if size == 0:
            continue

        key = (sha256_file(path), size)
        groups.setdefault(key, []).append(path)

    moved = 0
    quarantined_relative_paths: List[str] = []
    for paths in groups.values():
        if len(paths) < 2:
            continue

        if args.keep_oldest:
            keeper = min(paths, key=lambda p: (p.stat().st_mtime, p.as_posix()))
        else:
            keeper = sorted(paths, key=lambda p: p.as_posix())[0]

        for path in paths:
            if path == keeper:
                continue

            rel = path.relative_to(root)
            dest = quarantine_root / rel
            dest.parent.mkdir(parents=True, exist_ok=True)

            final_dest = dest
            counter = 1
            while final_dest.exists():
                final_dest = dest.with_name(f"{dest.stem}__dup{counter}{dest.suffix}")
                counter += 1

            print(f"DUPLICATE: {path} -> {final_dest} | keep: {keeper}")
            if args.apply:
                path.rename(final_dest)
                # Record the file's *original* relative path (before it moved
                # into the quarantine folder) so a caller can delete the same
                # object from remote storage (e.g. Mega) by that path.
                quarantined_relative_paths.append(rel.as_posix())
            moved += 1

    if args.apply:
        print(f"Moved {moved} duplicate files into {quarantine_root}")
        print("Only exact duplicate files were moved.")
        if args.manifest:
            manifest_path = Path(args.manifest).expanduser()
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text(
                "\n".join(quarantined_relative_paths) + ("\n" if quarantined_relative_paths else ""),
                encoding="utf-8",
            )
            print(f"Wrote manifest of {len(quarantined_relative_paths)} quarantined path(s) to {manifest_path}")
    else:
        print(f"Dry run complete. {moved} duplicate files would be moved into {quarantine_root}.")
        print("No files were changed.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
