# Vendored Elle drop

Holds the uberjar `elle-cli-standalone.jar` built from `../../elle_shim/` by `../../build_elle.sh`. The jar is not checked in — every fresh clone rebuilds it (cached in CI).

## Required tooling

- OpenJDK 21 LTS (tested with `openjdk version "21.0.5"` and `25.0.2`)
- Leiningen 2.x

## Rebuild

From the repo root:

```bash
./testing/build_elle.sh
```

This is what the matrix runner and CI call. The build is idempotent — it skips work when the shim source hasn't changed.

## Why subprocess (not JPype)?

- Cold-start (~3 s) amortizes over ≥30 s workloads.
- Zero JVM lifecycle in the Python process; failures are isolated to the subprocess.
- Simple upgrade path: rebuild the uberjar, swap the file. No coupled Python deps.
