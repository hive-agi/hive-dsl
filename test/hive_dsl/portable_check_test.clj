(ns hive-dsl.portable-check-test
  "Gate for the tri-runtime differential oracle.

   The JVM arm runs in the normal suite. The native arms drive the cljw and
   cljrs binaries, whose locations are machine-local: they resolve from
   HIVE_CLJW_BIN / HIVE_CLJRS_BIN, else from `[:runtimes <rt> :binary]` in
   ~/.config/hive-mcp/config.edn, the same key hive-emacs reads.

   The cloture arm drives an sbcl image instead, through the runner script in
   test/native; it resolves from HIVE_SBCL_BIN, then the same config key, then
   PATH.

   An unresolvable binary skips. Set HIVE_REQUIRE_NATIVE_ARMS=1 to make it a
   failure instead, so a run that is supposed to cover every runtime cannot
   pass by silently covering one."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [hive-dsl.portable-cases :as cases]
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

(defn- on-path
  "The first executable named `program` on PATH, or nil."
  [program]
  (->> (str/split (or (System/getenv "PATH") "") (re-pattern java.io.File/pathSeparator))
       (map #(io/file % program))
       (filter #(.canExecute ^java.io.File %))
       (map #(.getAbsolutePath ^java.io.File %))
       first))

(defn- resolve-binary
  "An executable path for `runtime`, or nil. Env var wins over config, which
   wins over `fallback-program` on PATH."
  ([runtime env-var] (resolve-binary runtime env-var nil))
  ([runtime env-var fallback-program]
   (->> [(System/getenv env-var)
         (configured-binary runtime)
         (some-> fallback-program on-path)]
        (keep identity)
        (filter #(.canExecute (io/file %)))
        first)))

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

;;; The cloture arm — Clojure hosted on Common Lisp. Unlike cljw and cljrs it
;;; cannot yet load the whole portable core, so its gate is a ratchet: the
;;; namespaces it fails on are declared with their reason, and the number of
;;; cases it agrees on may rise but never fall.

(def ^:private cloture-runner "test/native/cloture_portable_check.lisp")

(def ^:private cloture-unloadable
  "Portable-core namespaces the cloture arm cannot load, each with the gap that
   stops it. Empty since 2026-08-18, measured against BuddhiLW/cloture 47970a6."
  {})

(def ^:private cloture-agrees-at-least
  "Cases the cloture arm reproduced on 2026-08-18. A ratchet, not a target."
  69)

(defn- parse-arm-output
  "The arm's verdict as {:kind :agrees :total :unloadable}, or nil when it
   printed no verdict line at all."
  [output]
  (when-let [[_ kind agrees total]
             (re-find #"portable-check: (PASS|FAIL|LOAD-FAIL) (\d+)/(\d+)" output)]
    {:kind kind
     :agrees (parse-long agrees)
     :total (parse-long total)
     :unloadable (set (map second (re-seq #"(?m)^load-fail (\S+) ::" output)))}))

(deftest ^:integration cloture-arm-agrees
  (if-let [bin (resolve-binary :cloture "HIVE_SBCL_BIN" "sbcl")]
    (let [output (native-arm bin ["--script" cloture-runner])
          {:keys [kind agrees total unloadable]} (parse-arm-output output)]
      (is (some? kind) (str "the arm printed no verdict:\n" output))
      (when kind
        (testing "the arm ran every case the JVM has"
          (is (= (count cases/cases) total) output))
        (testing "the declared unloadable set is exactly what fails"
          (is (= (set (keys cloture-unloadable)) unloadable)
              (str "update cloture-unloadable — cloture moved:\n" output)))
        (testing "agreement with the frozen values never regresses"
          (is (<= cloture-agrees-at-least agrees)
              (str "cloture agreed on " agrees ", was " cloture-agrees-at-least
                   ":\n" output)))))
    (let [msg (str "no sbcl: set HIVE_SBCL_BIN or [:runtimes :cloture :binary] in "
                   config-file)]
      (if (System/getenv "HIVE_REQUIRE_NATIVE_ARMS")
        (is false msg)
        (println "SKIP cloture arm -" msg)))))
