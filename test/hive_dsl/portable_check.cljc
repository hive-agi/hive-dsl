(ns hive-dsl.portable-check
  "Replay half of the tri-runtime differential oracle.

   `hive-dsl.portable-golden/expected` is frozen on the JVM; this namespace
   replays `hive-dsl.portable-cases/cases` on whatever runtime loads it and
   diffs against that map. Host-free, so the identical code is the verdict on
   the JVM, ClojureWasm and clojurust.

   Values are compared as DATA, never as printed strings: the three runtimes
   disagree on map printing (`#:adt{...}` vs `{:adt/type ...}`) while comparing
   equal.

   Run on a native runtime:
     cljw <src+test on CLJW_PATH> -m hive-dsl.portable-check
     cljrs run --src-path src --src-path test <a file calling report>"
  (:require [hive-dsl.portable-cases :as cases]
            [hive-dsl.portable-golden :as golden]))

(defn diff
  "Cases whose observed value differs from the frozen one, as
   {id {:expected v :actual v}}. Also reports ids present on only one side."
  []
  (let [observed (cases/observe)
        ids (distinct (concat (map first cases/cases) (keys golden/expected)))]
    (reduce (fn [acc id]
              (let [e (get golden/expected id ::missing)
                    a (get observed id ::missing)]
                (if (= e a)
                  acc
                  (assoc acc id {:expected e :actual a}))))
            {}
            ids)))

(defn report
  "Print a one-line verdict plus every divergence. Returns the divergence map."
  []
  (let [d (diff)
        total (count cases/cases)]
    (if (empty? d)
      (println (str "portable-check: PASS " total "/" total))
      (do (println (str "portable-check: FAIL " (- total (count d)) "/" total))
          (doseq [[id {:keys [expected actual]}] (sort-by (comp str key) d)]
            (println " " id "expected" (pr-str expected) "actual" (pr-str actual)))))
    d))

(defn -main [& _]
  (report))
