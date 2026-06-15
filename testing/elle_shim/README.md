# pgbattery-elle-shim

Tiny Clojure project. Wraps [Elle](https://github.com/jepsen-io/elle) behind a JSON-in/JSON-out CLI so the Python test harness can drive it without a JVM in-process.

## Build (reproducible, one command)

```bash
# Install prerequisites (one-time, macOS).
brew install openjdk@21 leiningen

# From the repo root — builds the uberjar directly to its canonical location.
./testing/build_elle.sh
```

`build_elle.sh` is idempotent: it sha-stamps `project.clj` + `src/**/*.clj` and rebuilds only when the shim source changes. The matrix runner (`testing/run_elle_matrix.sh`) calls it automatically. CI does too.

The resulting uberjar lives at `testing/third_party/elle/elle-cli-standalone.jar` (~12 MB, self-contained).

## Smoke test

```bash
java -jar testing/third_party/elle/elle-cli-standalone.jar \
     rw-register testing/elle_shim/smoke_history.json
# Expected stdout: {"valid?":true,"elapsed-ms":...,"_meta":{...}}
# Expected exit:   0
```

## Pins

- `elle 0.2.2`
- `clojure 1.11.3`
- `cheshire 5.13.0`
- `spootnik/unilog 0.7.32` (transitive requirement of `elle.txn`, not declared by Elle itself)
- `io.jepsen/history 0.1.3` (transitive — provides the `History` wrapper Elle expects)
- OpenJDK 21 LTS recommended (tested with `openjdk version "21.0.5"` and `25.0.2`)

Bumping Elle requires re-running the smoke test plus the full matrix in `testing/run_elle_matrix.sh`.
