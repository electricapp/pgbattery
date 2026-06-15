(ns pgbattery-elle-shim.core
  "Thin CLI wrapper around Elle's consistency checkers.

  Usage:
    java -jar elle-cli-standalone.jar <model> <history-path>

    <model>        rw-register | list-append
    <history-path> path to JSON file (or - for stdin) containing
                   a list of Jepsen-format op records.

  Input contract (JSON, exactly what `jepsen.history/history` accepts after
  parsing-out the obvious JSON-vs-Clojure type coercions):

    [{\"type\":    \"invoke|ok|fail|info\",
      \"process\": <int>,
      \"time\":    <int nanoseconds>,
      \"f\":       \"txn\",
      \"value\":   [[\"r|w|append\", <key>, <val-or-null>], ...]} ...]

  This shim performs exactly three operations on each record:

    1. Stringly-typed JSON keys -> Clojure keywords (:type, :f, mop kind).
    2. Numeric coercion to Long (jepsen.history.Op record fields demand it).
    3. Wrapping the seq in `(jepsen.history/history ...)` -- required by
       Elle's checker API, which only accepts a History (not a vector).

  No format inference, no time fudging, no value rewriting. If Elle reports
  an anomaly, that anomaly is a property of the input history -- not of the
  shim. All workload-side encoding decisions live in the test harness.

  Output (JSON to stdout):
    {\"valid?\":     true | false | \"unknown\",
     \"anomalies\":  {<class>: [<instance> ...] ...},
     \"elapsed-ms\": <float>,
     \"_meta\":      {\"model\": ..., \"op-count\": ...}}

  Exit codes:
    0  :valid? is true
    1  :valid? is false or :unknown (caller should inspect anomalies)
    2  input parse error or internal failure"
  (:require [cheshire.core :as json]
            [elle.rw-register :as rw]
            [elle.list-append :as la]
            [jepsen.history :as history])
  (:gen-class))

(defn- coerce-mop
  "Stringly-typed micro-op [\"r\" 0 nil] -> [:r 0 nil]."
  [[k key val]]
  [(keyword k) key val])

(defn- coerce-op
  "JSON op (Cheshire-keywordized keys) -> jepsen.history-acceptable map.

  Numeric fields :process and :time must be Long; Cheshire defaults to
  Integer for small JSON numbers which fails jepsen.history.Op/create's
  long-cast."
  [{:keys [type process time f value]}]
  {:type    (keyword type)
   :process (long process)
   :time    (long time)
   :f       (keyword f)
   :value   (mapv coerce-mop value)})

(defn- checker-for
  "Returns the elle check function for `model`."
  [model]
  (case model
    "rw-register" rw/check
    "list-append" la/check
    (throw (ex-info (str "unknown model: " model) {:model model}))))

(defn check-history
  "Returns Elle's result map augmented with timing + metadata."
  [model raw-records]
  (let [t0       (System/nanoTime)
        ops      (mapv coerce-op raw-records)
        h        (history/history ops)
        opts     {:consistency-models [:strict-serializable]
                  :directory          nil}
        result   ((checker-for model) opts h)
        elapsed  (double (/ (- (System/nanoTime) t0) 1e6))]
    (assoc result
           :elapsed-ms elapsed
           :_meta {:model model
                   :op-count (count ops)})))

(defn- read-history
  [path]
  (let [src (if (= path "-") *in* path)]
    (json/parse-string (slurp src) true)))

(defn- exit-code
  "Map :valid? to a process exit code. :unknown counts as failure --
   operator must inspect the anomaly report."
  [result]
  (case (:valid? result)
    true  0
    false 1
    1))

(defn -main
  [& args]
  (try
    (let [model        (or (first args) "rw-register")
          history-path (or (second args) "-")
          records      (read-history history-path)
          result       (check-history model records)]
      (println (json/generate-string result {:key-fn name}))
      (System/exit (exit-code result)))
    (catch Throwable t
      (binding [*out* *err*]
        (println (.getMessage t))
        (.printStackTrace t *err*))
      (System/exit 2))))
