#!/usr/bin/env python3
"""
wipe_nc_storage.py

Wipes txnlog/, coredump/, and iodevice* dirs for nc1..nc4
under /scratch/asterixdb_eightynode/asterixdb_eightnode/target/tmp.

Default = DRY RUN (prints what would be deleted).
Use --apply to actually delete.
"""

import argparse
import os
import shutil

BASE_DIR = "/scratch//asterixdb_eightynode/asterixdb_eightnode/target/tmp"
NCS = ["asterix_nc1", "asterix_nc2", "asterix_nc3", "asterix_nc4"]

def wipe(path: str, apply: bool):
    if not os.path.exists(path):
        print(f"[skip] not found: {path}")
        return
    if not path.startswith(BASE_DIR):
        print(f"[WARN] refusing suspicious path: {path}")
        return
    if apply:
        shutil.rmtree(path, ignore_errors=True)
        print(f"[wipe] {path}")
    else:
        print(f"[dry ] would delete {path}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="actually delete")
    args = ap.parse_args()

    print(f"[info] mode={'APPLY' if args.apply else 'DRY-RUN'}")

    for nc in NCS:
        base = os.path.join(BASE_DIR, nc)
        print(f"\n=== {nc} ===")
        for sub in ["txnlog", "coredump"]:
            wipe(os.path.join(base, sub), args.apply)
        if os.path.exists(base):
            for d in os.listdir(base):
                if d.startswith("iodevice"):
                    wipe(os.path.join(base, d), args.apply)

    if not args.apply:
        print("\nNothing deleted. Re-run with --apply to execute.")

if __name__ == "__main__":
    main()
