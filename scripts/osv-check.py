"""Ask OSV about every Maven coordinate in this project's lockfiles, plus one control that has known
advisories (lz4-java 1.11.2, which kafka-streams 4.4.0 asks for through kafka-clients and the build replaces); the check
fails if the control is not found, so an empty answer cannot pass for "no advisories".
Usage: python3 -I scripts/osv-check.py
"""
import json
import pathlib
import sys
import urllib.request

CONTROL = ("at.yawk.lz4:lz4-java", "1.11.2")
root = pathlib.Path(__file__).resolve().parent.parent
coordinates = set()
for lockfile in ("gradle.lockfile", "settings-gradle.lockfile"):
    for line in (root / lockfile).read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#") or line.startswith("empty="):
            continue
        group, artifact, version = line.split("=")[0].split(":")
        coordinates.add((group + ":" + artifact, version))
queries = sorted(coordinates) + [CONTROL]
body = json.dumps({"queries": [{"package": {"ecosystem": "Maven", "name": name}, "version": version}
                               for name, version in queries]}).encode("utf-8")
request = urllib.request.Request("https://api.osv.dev/v1/querybatch", data=body,
                                 headers={"Content-Type": "application/json", "User-Agent": "osv-check"})
with urllib.request.urlopen(request, timeout=60) as answer:
    results = json.load(answer)["results"]
found = {query: [vuln["id"] for vuln in result.get("vulns", [])] for query, result in zip(queries, results)}
for (name, version), ids in sorted(found.items()):
    if ids and (name, version) != CONTROL:
        print(f"ADVISORY {name}:{version}: {', '.join(ids)}")
print(f"coordinates checked: {len(coordinates)}; with advisories: "
      f"{sum(1 for query, ids in found.items() if ids and query != CONTROL)}")
print(f"control {CONTROL[0]}:{CONTROL[1]}: {len(found[CONTROL])} advisories found ({', '.join(found[CONTROL][:3])})")
if not found[CONTROL]:
    sys.exit("the control was not found: the check itself is broken")
sys.exit(1 if any(ids for query, ids in found.items() if query != CONTROL) else 0)
