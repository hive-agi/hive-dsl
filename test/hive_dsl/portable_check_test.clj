(ns hive-dsl.portable-check-test
  "Gate for the tri-runtime differential oracle.

   The JVM arm runs in the normal suite. The native arms drive the cljw and
   cljrs binaries, whose locations are machine-local: they resolve from
   HIVE_CLJW_BIN / HIVE_CLJRS_BIN, else from `[:runtimes <rt> :binary]` in
   ~/.config/hive-mcp/config.edn, the same key hive-emacs reads.

   An unresolvable binary skips. Set HIVE_REQUIRE_NATIVE_ARMS=1 to make it a
   failure instead, so a run that is supposed to cover all three runtimes
   cannot pass by silently covering one."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.test :refer [deftest is testing]]
            [hive-dsl.portable-check :as check]))

(def ^:private config-file
  (io/file (System/getProperty "user.home") ".config/hive-mcp/config.edn"))

(defn- expand-home
  [path]
  (if (.startsWith ^String path "~")
    (str (System/getProperty "user.home") (subs path 1))
    path))

(defn- configured-binary
  "Path configured for `runtime` under [:runtimes <runtime> :binary], or nil."
  [runtime]
  (when (.exists config-file)
    (some-> (try (edn/read-string (slurp config-file))
                 (catch Exception _ nil))
            (get-in [:runtimes runtime :binary])
            expand-home)))

(defn- resolve-binary
  "An executable path for `runtime`, or nil. Env var wins over config."
  [runtime env-var]
  (->> [(System/getenv env-var) (configured-binary runtime)]
       (keep identity)
       (filter #(.canExecute (io/file %)))
       first))

(defn- native-arm
  [bin argv]
  (let [{:keys [out err]} (apply shell/sh (concat [bin] argv))]
    (str out err)))

(defn- check-arm
  "Run the replay runner under `runtime` and assert it passed."
  [runtime env-var argv]
  (if-let [bin (resolve-binary runtime env-var)]
    (let [output (native-arm bin argv)]
      (is (re-find #"portable-check: PASS" output) output))
    (let [msg (str "no " (name runtime) " binary: set " env-var
                   " or [:runtimes " runtime " :binary] in " config-file)]
      (if (System/getenv "HIVE_REQUIRE_NATIVE_ARMS")
        (is false msg)
        (println "SKIP" (name runtime) "arm -" msg)))))

(deftest jvm-arm-matches-the-frozen-values
  (testing "the reference arm agrees with what it froze"
    (is (empty? (check/diff)))))

(deftest ^:integration cljw-arm-agrees
  (check-arm :cljw "HIVE_CLJW_BIN" ["-cp" "src:test" "-m" "hive-dsl.portable-check"]))

(deftest ^:integration cljrs-arm-agrees
  (let [entry (doto (java.io.File/createTempFile "portable-check" ".cljc")
                (.deleteOnExit))]
    (spit entry "(require '[hive-dsl.portable-check :as pc])\n(pc/report)\n")
    (check-arm :cljrs "HIVE_CLJRS_BIN"
               ["run" "--src-path" "src" "--src-path" "test" (.getAbsolutePath entry)])))
