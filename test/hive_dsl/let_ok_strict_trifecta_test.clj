(ns hive-dsl.let-ok-strict-trifecta-test
  "Trifecta coverage for `r/let-ok` strict-binding semantics.

   The test surface is `let-ok-outcome` — a single-arg classifier that
   runs one r/let-ok binding against `value` and reports the structural
   outcome as data. This makes the macro's runtime contract directly
   golden-snapshottable, property-testable, and mutation-testable in
   one trifecta declaration.

   Why this exists: the macro went strict 2026-05-02 (non-Result
   bindings now throw instead of silently terminating the chain). The
   most dangerous regression would be a silent revert — an implementer
   reintroducing the loose `(if (ok? r#) ... r#)` shape. The mutation
   `swallow-throw` is *exactly* the pre-strict behavior; if it ever
   stops being caught, the bug is back."
  (:require [clojure.test :refer [use-fixtures]]
            [clojure.test.check.generators :as gen]
            [hive-dsl.result :as r]
            [hive-test.trifecta :refer [deftrifecta]]))

;; ============================================================
;; Test surface
;; ============================================================

(defn let-ok-outcome
  "Run `(r/let-ok [x value] body)` and classify the outcome:

     {:phase :body-ran          :bound v}      — value was (r/ok v)
     {:phase :err-short-circuit :error cat}    — value was (r/err cat ...)
     {:phase :throw-non-result  :binding s
                                :type t}       — strict throw

   Pure (no IO). The throw is caught + reified into data so the
   classifier itself never escapes a stack trace into trifecta."
  [value]
  (try
    (let [result (r/let-ok [x value] {:phase :body-ran :bound x})]
      (if (r/err? result)
        {:phase :err-short-circuit :error (:error result)}
        result))
    (catch clojure.lang.ExceptionInfo e
      (let [d (ex-data e)]
        (if (= :result/non-result-binding (:category d))
          {:phase   :throw-non-result
           :binding (:binding d)
           :type    (:type d)}
          (throw e))))))

;; ============================================================
;; Generators
;; ============================================================

(def ^:private gen-ok-value
  (gen/fmap r/ok gen/small-integer))

(def ^:private gen-err-value
  (gen/fmap r/err gen/keyword))

(def ^:private gen-raw-value
  (gen/one-of [gen/small-integer
               gen/keyword
               gen/string
               (gen/return nil)
               (gen/vector gen/small-integer 0 3)
               (gen/map gen/keyword gen/small-integer)]))

(def ^:private gen-input
  (gen/one-of [gen-ok-value gen-err-value gen-raw-value]))

;; ============================================================
;; Invariant — every input maps to a known phase
;; ============================================================

(def ^:private valid-phases
  #{:body-ran :err-short-circuit :throw-non-result})

(defn- known-phase?
  "Property post-condition: classifier output always carries one of
   the three sanctioned phase keywords. Catches any classifier that
   forgets a branch or invents a new outcome."
  [outcome]
  (contains? valid-phases (:phase outcome)))

;; ============================================================
;; Mutation oracles — broken classifiers
;; ============================================================

(defn- mut-always-body-ran
  "Ignores throws; pretends every binding succeeded.
   Catches: regression where strict throw is removed and non-Result
   bindings silently bind something."
  [_value]
  {:phase :body-ran :bound ::fake})

(defn- mut-swallow-throw
  "Reproduces the EXACT pre-2026-05-02 behavior — non-Result is
   silently treated as a terminator. If this mutation ever survives,
   the strict throw has been regressed."
  [value]
  (try
    (let [result (r/let-ok [x value] {:phase :body-ran :bound x})]
      (if (r/err? result)
        {:phase :err-short-circuit :error (:error result)}
        result))
    (catch clojure.lang.ExceptionInfo _
      ;; Bug: swallow the throw, fabricate a body-ran outcome
      {:phase :body-ran :bound ::fake})))

(defn- mut-ok-as-err
  "Misclassifies ok-bound values as errors."
  [value]
  (let [out (let-ok-outcome value)]
    (if (= :body-ran (:phase out))
      {:phase :err-short-circuit :error :fake}
      out)))

(defn- mut-empty-out
  "Returns garbage."
  [_] {})

;; ============================================================
;; Trifecta — golden + property + mutation
;; ============================================================

(deftrifecta let-ok-strict-trifecta
  hive-dsl.let-ok-strict-trifecta-test/let-ok-outcome
  {:golden-path "test/golden/hive-dsl/trifecta-let-ok-strict.edn"
   :cases       {:ok-int        (r/ok 7)
                 :ok-nil        (r/ok nil)
                 :err-keyword   (r/err :validation/bad)
                 :err-with-data (r/err :io/timeout {:retry 3})
                 :raw-int       42
                 :raw-map       {:a 1 :b 2}
                 :raw-vector    [1 2 3]
                 :raw-string    "hello"
                 :nil-val       nil}
   :gen         gen-input
   :pred        known-phase?
   :num-tests   200
   :mutations   [["always-body-ran"   mut-always-body-ran]
                 ["swallow-throw"     mut-swallow-throw]
                 ["ok-as-err"         mut-ok-as-err]
                 ["empty-out"         mut-empty-out]]})
