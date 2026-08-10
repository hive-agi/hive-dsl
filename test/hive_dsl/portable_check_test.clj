(ns hive-dsl.portable-check-test
  "Gate for the tri-runtime differential oracle.

   The JVM arm runs in the normal suite. The native arms are real integration:
   they need the ClojureWasm / clojurust binaries, whose locations are
   machine-local and therefore injected through HIVE_CLJW_BIN and
   HIVE_CLJRS_BIN. Each native test skips when its variable is unset."
  (:require [clojure.java.shell :as shell]
            [clojure.test :refer [deftest is testing]]
            [hive-dsl.portable-check :as check]))

(deftest jvm-arm-matches-the-frozen-values
  (testing "the reference arm agrees with what it froze"
    (is (empty? (check/diff)))))

(defn- native-arm
  "Run the replay runner under `bin` and return its combined output."
  [bin argv]
  (let [{:keys [out err]} (apply shell/sh (concat [bin] argv))]
    (str out err)))

(deftest ^:integration cljw-arm-agrees
  (if-let [bin (System/getenv "HIVE_CLJW_BIN")]
    (let [output (native-arm bin ["-m" "hive-dsl.portable-check"])]
      (is (re-find #"portable-check: PASS" output) output))
    (println "SKIP cljw-arm-agrees — HIVE_CLJW_BIN unset")))

(deftest ^:integration cljrs-arm-agrees
  (if-let [bin (System/getenv "HIVE_CLJRS_BIN")]
    (let [entry (doto (java.io.File/createTempFile "portable-check" ".cljc")
                  (.deleteOnExit))]
      (spit entry "(require '[hive-dsl.portable-check :as pc])\n(pc/report)\n")
      (let [output (native-arm bin ["run" "--src-path" "src" "--src-path" "test"
                                    (.getAbsolutePath entry)])]
        (is (re-find #"portable-check: PASS" output) output)))
    (println "SKIP cljrs-arm-agrees — HIVE_CLJRS_BIN unset")))
