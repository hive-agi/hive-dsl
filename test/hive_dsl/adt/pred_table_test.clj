(ns hive-dsl.adt.pred-table-test
  "ONE predicate table (hive-dsl.adt/pred-table) feeds both projections:
   pred-sym->malli (symbols, macroexpansion) and adt.schema/pred->schema
   (evaluated fns, runtime). These tests gate that the two agree, that the
   upgrade never changes which values a field accepts, and that the
   generator-capable claim holds except for the declared validate-only preds."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.generators :as gen]
            [hive-dsl.adt :as adt]
            [hive-dsl.adt.schema :as as]
            [hive-test.trifecta :refer [deftrifecta]]
            [malli.core :as m]
            [malli.generator :as mg]))

(defn upgrade
  "Public handle on the private macroexpansion-time upgrade."
  [pred-form]
  (#'adt/pred-sym->schema pred-form))

(def table-syms (vec (sort (keys adt/pred-table))))

(deftrifecta pred-upgrade
  hive-dsl.adt.pred-table-test/upgrade
  {:cases {'string?                 'string?
           'uuid?                   'uuid?
           'fn?                     'fn?
           'pos?                    [:fn 'pos?]
           '(some-fn nil? int?)     [:maybe 'int?]
           '(some-fn nil? my/pred?) [:fn '(some-fn nil? my/pred?)]
           '(constantly true)       :any
           '(constantly false)      [:fn '(constantly false)]
           'my/pred?                [:fn 'my/pred?]}
   :gen   (gen/elements table-syms)
   :pred  #(and (symbol? %) (some? (m/schema %)))
   :num-tests 100})

(deftest both-directions-agree
  (testing "non-vacuity floor: the table is at least the 20 legacy + 8 widened entries"
    (is (<= 28 (count adt/pred-table))))
  (testing "pred-sym->malli and pred->schema cover exactly the table"
    (is (= (set (keys adt/pred-table)) (set (keys adt/pred-sym->malli))))
    (is (= (set (keys adt/pred-table)) (set (vals as/pred->schema))))
    (is (= (count adt/pred-table) (count as/pred->schema))))
  (testing "for every entry, the symbol path and the fn path land on the same schema"
    (doseq [[sym f] adt/pred-table]
      (is (= (get adt/pred-sym->malli sym) (get as/pred->schema f)) (str sym)))))

(deftest generator-claim-is-measured
  (testing "every entry compiles; generators exist exactly outside validate-only-preds"
    (doseq [sym table-syms]
      (is (some? (m/schema sym)) (str sym))
      (let [gens? (try (mg/generate sym {:seed 1}) true (catch Throwable _ false))]
        (is (= (not (contains? adt/validate-only-preds sym)) gens?) (str sym))))))

(def sample-universe
  [nil true false 0 1 -1 42 1.5 -2.5 (float 1.0) 1/2 :k :ns/k 'sym 'ns/sym "" "s"
   [] [1] '() '(1) {} {:a 1} #{} #{1} (java.util.UUID/randomUUID) (java.util.Date.)
   inc (range 3)])

(deftest upgrade-preserves-accepted-values
  (testing "the symbolic schema accepts exactly what the predicate fn accepts"
    (doseq [[sym f] adt/pred-table
            v sample-universe]
      (is (= (boolean (f v)) (m/validate sym v)) (str sym " on " (pr-str v))))))

(adt/defadt PredTableDemo "fields outside the legacy 20-entry table"
  [:demo/a {:id uuid? :opt (some-fn nil? string?) :any (constantly true)}]
  :demo/b)

(defn validates?
  "Verdict of the hand-rolled adt/validate path (it throws when invalid)."
  [v]
  (try (adt/validate PredTableDemo v) true (catch Exception _ false)))

(deftest defadt-emits-the-rewrites
  (let [schema (->> (drop 2 PredTableDemoMalli)
                    (some (fn [[v s]] (when (= :demo/a v) s))))
        fields (into {} (map (fn [[k s]] [k s])) (drop 1 schema))]
    (is (= 'uuid? (:id fields)))
    (is (= [:maybe 'string?] (:opt fields)))
    (is (= :any (:any fields))))
  (testing "same verdicts as the hand-rolled validator, and it generates"
    (doseq [v [(pred-table-demo :demo/a {:id (random-uuid) :opt nil :any 1})
               (pred-table-demo :demo/a {:id (random-uuid) :opt "x" :any nil})
               {:adt/type :PredTableDemo :adt/variant :demo/a :id "no" :opt nil :any 1}
               {:adt/type :PredTableDemo :adt/variant :demo/a :id (random-uuid) :opt 3 :any 1}]]
      (is (= (validates? v) (m/validate PredTableDemoMalli v)) (pr-str v)))
    (doseq [v (mg/sample PredTableDemoMalli {:seed 5})]
      (is (m/validate PredTableDemoMalli v)))))
