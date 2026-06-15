(defproject pgbattery-elle-shim "0.1.0"
  :description "JSON-in/JSON-out wrapper around Elle for the pgbattery test harness."
  :url "https://github.com/jepsen-io/elle"
  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.3"]
                 [elle "0.2.2"]
                 [cheshire "5.13.0"]
                 ;; elle.txn transitively requires `unilog.config` (jepsen's
                 ;; logging shim) but Elle's project.clj doesn't pull it in
                 ;; as a hard dep — downstream users have to. Pinning here.
                 [spootnik/unilog "0.7.32"]]
  :main pgbattery-elle-shim.core
  ;; Write the uberjar directly to the canonical location. `:uberjar-name`
  ;; is interpreted relative to `:target-path` (defaults to "target"), so
  ;; "../../third_party/elle/..." resolves to
  ;; `testing/third_party/elle/elle-cli-standalone.jar` from this dir.
  ;; This means `lein uberjar` is enough — no separate mv step.
  :uberjar-name "../../third_party/elle/elle-cli-standalone.jar"
  :jvm-opts ["-Xmx2g"]
  ;; Full AOT. Partial AOT breaks at runtime because our compiled shim
  ;; emits static references to `cheshire/core$loading__6789__auto____NNN`
  ;; with non-deterministic gensym IDs that don't match cheshire's
  ;; freshly-loaded classes. Compiling everything keeps the IDs consistent.
  :profiles {:uberjar {:aot :all}})
