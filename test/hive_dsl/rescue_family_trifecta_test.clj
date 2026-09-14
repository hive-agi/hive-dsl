(ns hive-dsl.rescue-family-trifecta-test
  "Trifecta coverage for the rescue family: rescue, rescue-log, rescue-ex, rescue-ex-log."
  (:require [clojure.test :refer [use-fixtures]]
            [clojure.test.check.generators :as gen]
            [hive-dsl.result :as r]
            [hive-test.trifecta :refer [deftrifecta]]))

;; ============================================================
;; Test surface
;; ============================================================

(defn- throwable-sexp
  "Return an S-expression (as data) that constructs the given throwable kind."
  [kind]
  (case kind
    :runtime         '(RuntimeException. "boom")
    :ex-info         '(ex-info "boom" {:detail 42})
    :assertion-error '(AssertionError. "boom")
    :stack-overflow  '(StackOverflowError. "boom")))

(defn- build-form
  "Build a macro call form (as data) for evaluating with `eval`."
  [variant fallback throwable-kind]
  (let [tsexp (throwable-sexp throwable-kind)]
    (case variant
      :rescue        (list 'hive-dsl.result/rescue      fallback (list 'throw tsexp))
      :rescue-log    (list 'hive-dsl.result/rescue-log  "test"   fallback (list 'throw tsexp))
      :rescue-ex     (list 'hive-dsl.result/rescue-ex   fallback (list 'throw tsexp))
      :rescue-ex-log (list 'hive-dsl.result/rescue-ex-log "test" fallback (list 'throw tsexp)))))

(defn rescue-outcome
  "Evaluate the macro for VARIANT with a body that throws THROWABLE-KIND
   and returns FALLBACK-KIND as the fallback. Classify the result:

     {:caught? true  :result-class str :error-keys (sorted-set) or nil}
     {:caught? false :escaped str}"

  [[variant throwable-kind fallback-kind]]
  (let [fallback (case fallback-kind
                   :vector  []
                   :map     {}
                   :nil     nil
                   :keyword :fallback)
        form     (build-form variant fallback throwable-kind)]
    (try
      (let [result       (eval form)
            err-meta     (::r/error (meta result))
            error-keys   (when err-meta (into (sorted-set) (keys err-meta)))]
        {:caught? true
         :result-class (str (class result))
         :error-keys error-keys})
      (catch Throwable e
        {:caught? false
         :escaped (str (class e) ": " (ex-message e))}))))

;; ============================================================
;; Generators
;; ============================================================

(def ^:private all-variants
  [:rescue :rescue-log :rescue-ex :rescue-ex-log])

(def ^:private all-throwables
  [:runtime :ex-info :assertion-error :stack-overflow])

(def ^:private all-fallbacks
  [:vector :map :nil :keyword])

(def ^:private gen-variant
  (gen/elements all-variants))

(def ^:private gen-throwable
  (gen/elements all-throwables))

(def ^:private gen-fallback
  (gen/elements all-fallbacks))

(def ^:private gen-triple
  (gen/tuple gen-variant gen-throwable gen-fallback))

;; ============================================================
;; Cases — full matrix 4×4×4 = 64
;; ============================================================

(def ^:private generate-cases
  (into (sorted-map)
    (for [v all-variants
          t all-throwables
          f all-fallbacks]
      [(keyword (str (name v) "/" (name t) "/" (name f)))
       [v t f]])))

;; ============================================================
;; Invariant — every outcome has the correct shape
;; ============================================================

(defn- valid-outcome?
  "Property post-condition: outcome has :caught?, and either
   {:caught? true :result-class str :error-keys set-or-nil}
   or {:caught? false :escaped str}."
  [outcome]
  (and (map? outcome)
       (contains? outcome :caught?)
       (if (:caught? outcome)
         (and (string? (:result-class outcome))
              (or (nil? (:error-keys outcome))
                  (and (set? (:error-keys outcome))
                       (contains? (:error-keys outcome) :message)
                       (contains? (:error-keys outcome) :form))))
         (string? (:escaped outcome)))))

;; ============================================================
;; Mutation oracles — broken classifiers that represent
;; regression patterns in the macros
;; ============================================================

(defn- mut-ex-variants-catch-errors
  "Bug: rescue-ex and rescue-ex-log catch errors too (host-catch-all
   instead of Exception-only)."
  [[variant throwable-kind fallback-kind]]
  (let [effective-variant (case variant
                            :rescue-ex     :rescue
                            :rescue-ex-log :rescue-log
                            variant)]
    (rescue-outcome [effective-variant throwable-kind fallback-kind])))

(defn- mut-rescue-lets-errors-escape
  "Bug: rescue and rescue-log let errors escape (Exception-only
   instead of host-catch-all)."
  [[variant throwable-kind fallback-kind]]
  (let [effective-variant (case variant
                            :rescue     :rescue-ex
                            :rescue-log :rescue-ex-log
                            variant)]
    (rescue-outcome [effective-variant throwable-kind fallback-kind])))

(defn- mut-log-variants-drop-label
  "Bug: log variants omit :label from error metadata."
  [[variant throwable-kind fallback-kind]]
  (let [effective-variant (case variant
                            :rescue-log    :rescue
                            :rescue-ex-log :rescue-ex
                            variant)]
    (rescue-outcome [effective-variant throwable-kind fallback-kind])))

(defn- mut-metadata-on-nil-fallback
  "Bug: nil fallback still gets error metadata attached (meta-check
   is not working correctly for nil)."
  [[variant throwable-kind fallback-kind]]
  (let [effective-fallback (if (= :nil fallback-kind) :keyword fallback-kind)]
    (rescue-outcome [variant throwable-kind effective-fallback])))

;; ============================================================
;; Trifecta — golden + property + mutation
;; ============================================================

(deftrifecta rescue-family-trifecta
  hive-dsl.rescue-family-trifecta-test/rescue-outcome
  {:golden-path "test/golden/hive-dsl/trifecta-rescue-family.edn"
   :cases       generate-cases
   :gen         gen-triple
   :pred        valid-outcome?
   :num-tests   200
   :mutations   [["ex-variants-catch-errors"   mut-ex-variants-catch-errors]
                 ["rescue-lets-errors-escape"   mut-rescue-lets-errors-escape]
                 ["log-variants-drop-label"     mut-log-variants-drop-label]
                 ["metadata-on-nil-fallback"    mut-metadata-on-nil-fallback]]})
