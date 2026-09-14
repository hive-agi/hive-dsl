(ns hive-dsl.rescue-family-trifecta-test
  "Trifecta coverage for the rescue family: rescue, rescue-log, rescue-ex, rescue-ex-log."
  (:require [clojure.test :refer [use-fixtures deftest is]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :as tc]
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

;; ============================================================
;; Input-aware oracle — caught?/error-keys must match the
;; catch-policy, throwable-kind and fallback-kind
;; ============================================================

(defn- oracle-pass?
  "Input-aware oracle over the rescue-family outcome.
   Asserts catching semantics (caught? true for runtime/ex-info in all
   variants; false for Errors only under rescue-ex/rescue-ex-log). When
   caught, vector/map fallbacks carry exactly :message + :form (and :label
   for log variants); nil/keyword fallbacks carry no error metadata."
  [[variant throwable-kind fallback-kind :as triple]]
  (let [{:keys [caught? error-keys]} (rescue-outcome triple)
        error?   (#{:assertion-error :stack-overflow} throwable-kind)
        ex-only? (contains? #{:rescue-ex :rescue-ex-log} variant)
        log-var? (#{:rescue-log :rescue-ex-log} variant)]
    (and
     ;; Catching semantics
     (if error?
       (not= caught? ex-only?)     ;; errors escape ex variants only
       (true? caught?))             ;; non-errors always caught
     ;; Error metadata upon catch
     (if caught?
       (case fallback-kind
         (:vector :map)
         (if log-var?
           (= #{:message :form :label} error-keys)
           (= #{:message :form} error-keys))
         (:nil :keyword)
         (nil? error-keys))
       true))))

(deftest oracle-rejects-catch-all-expansion
  (let [catch-all (var-get #'r/rescue)
        catch-all-log (var-get #'r/rescue-log)
        original-meta (into {} (map (fn [v] [v (meta v)]) [#'r/rescue-ex #'r/rescue-ex-log]))]
    (try
      (with-redefs [r/rescue-ex catch-all r/rescue-ex-log catch-all-log]
        (doseq [[v m] original-meta] (reset-meta! v m))
        (doseq [variant [:rescue-ex :rescue-ex-log]
                throwable [:assertion-error :stack-overflow]
                fallback [:vector :map :nil :keyword]]
          (is (false? (oracle-pass? [variant throwable fallback]))
              (pr-str [variant throwable fallback]))))
      (finally (doseq [[v m] original-meta] (reset-meta! v m))))))

(tc/defspec rescue-family-caught-property 200
  (prop/for-all [triple gen-triple]
    (oracle-pass? triple)))

;; ============================================================
;; Nil/false label sentinel tests — explicit logging-variant
;; calls with literal nil and false labels must still use the
;; logging expansion and keep :label on metadata-capable fallbacks
;; ============================================================

(defn- label-test
  "Return complete metadata: absent :label differs from a present nil."
  [variant label]
  (let [macro-sym (case variant
                    :rescue-log 'hive-dsl.result/rescue-log
                    :rescue-ex-log 'hive-dsl.result/rescue-ex-log)
        result (eval (list macro-sym label {} '(throw (RuntimeException. "boom"))))]
    (::r/error (meta result))))

(deftest rescue-family-nil-false-labels
  (doseq [variant [:rescue-log :rescue-ex-log]
          label [nil false]]
    (let [error (label-test variant label)]
      (is (= #{:message :form :label} (set (keys error))))
      (is (contains? error :label))
      (is (= label (:label error))))))
