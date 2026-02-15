(ns hive-dsl.adt-property-test
  "Property-based tests for hive-dsl.adt — defadt macro.

   Properties tested:
   1. Round-trip: serialize∘deserialize = identity for all variants
   2. Exhaustiveness: case dispatch covers all variants (totality)
   3. Coercion: keyword→variant→keyword = identity
   4. Predicate correctness: type predicate is exact discriminator
   5. Registry consistency: all constructed values are registry-valid
   6. ADT laws: make-variant produces adt? values

   Convention: 200 iterations per defspec, *_property_test.clj naming."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-dsl.adt :as adt]))

;; =============================================================================
;; Test ADT Definitions
;; =============================================================================

;; Pure enum
(adt/defadt TestDirection
  :dir/north
  :dir/south
  :dir/east
  :dir/west)

;; Data-carrying
(adt/defadt TestExpr
  "Arithmetic expression ADT."
  [:expr/literal {:value number?}]
  [:expr/add     {:left number? :right number?}]
  [:expr/negate  {:operand number?}])

;; Mixed (some data, some enum)
(adt/defadt TestStatus
  [:status/running {:progress number?}]
  :status/idle
  :status/stopped
  [:status/error {:message string?}])

;; =============================================================================
;; Generators
;; =============================================================================

;; --- TestDirection generators ---

(def gen-direction-variant
  (gen/elements [:dir/north :dir/south :dir/east :dir/west]))

(def gen-direction
  (gen/fmap test-direction gen-direction-variant))

;; --- TestExpr generators ---

(def gen-expr-literal
  (gen/let [v gen/small-integer]
    (test-expr :expr/literal {:value v})))

(def gen-expr-add
  (gen/let [l gen/small-integer
            r gen/small-integer]
    (test-expr :expr/add {:left l :right r})))

(def gen-expr-negate
  (gen/let [n gen/small-integer]
    (test-expr :expr/negate {:operand n})))

(def gen-expr
  (gen/one-of [gen-expr-literal gen-expr-add gen-expr-negate]))

(def gen-expr-variant
  (gen/elements [:expr/literal :expr/add :expr/negate]))

;; --- TestStatus generators ---

(def gen-status-running
  (gen/let [p (gen/double* {:infinite? false :NaN? false :min 0.0 :max 100.0})]
    (test-status :status/running {:progress p})))

(def gen-status-error
  (gen/let [m gen/string-alphanumeric]
    (test-status :status/error {:message m})))

(def gen-status
  (gen/one-of [gen-status-running
               (gen/return (test-status :status/idle))
               (gen/return (test-status :status/stopped))
               gen-status-error]))

(def gen-status-variant
  (gen/elements [:status/running :status/idle :status/stopped :status/error]))

;; --- Arbitrary keyword generator (for negative testing) ---

(def gen-arbitrary-keyword
  (gen/fmap (fn [[ns n]] (keyword ns n))
            (gen/tuple gen/string-alphanumeric gen/string-alphanumeric)))

;; =============================================================================
;; Property 1: Round-trip serialization (serialize∘deserialize = identity)
;; =============================================================================

(defspec roundtrip-enum-serialize-deserialize 200
  (prop/for-all [v gen-direction]
                (= v (adt/deserialize (adt/serialize v)))))

(defspec roundtrip-data-serialize-deserialize 200
  (prop/for-all [v gen-expr]
                (= v (adt/deserialize (adt/serialize v)))))

(defspec roundtrip-mixed-serialize-deserialize 200
  (prop/for-all [v gen-status]
                (= v (adt/deserialize (adt/serialize v)))))

(defspec roundtrip-edn-print-read 200
  (prop/for-all [v gen-direction]
                (let [s (pr-str v)
                      v2 (clojure.edn/read-string s)]
                  (= v v2))))

(defspec roundtrip-edn-data-variant 200
  (prop/for-all [v gen-expr]
                (let [s (pr-str v)
                      v2 (clojure.edn/read-string s)]
                  (= v v2))))

;; =============================================================================
;; Property 2: Exhaustiveness (adt-case covers all variants = totality)
;; =============================================================================

(defspec exhaustive-dispatch-enum 200
  (prop/for-all [v gen-direction]
                (string?
                 (adt/adt-case TestDirection v
                               :dir/north "N"
                               :dir/south "S"
                               :dir/east  "E"
                               :dir/west  "W"))))

(defspec exhaustive-dispatch-data 200
  (prop/for-all [v gen-expr]
                (number?
                 (adt/adt-case TestExpr v
                               :expr/literal (:value v)
                               :expr/add     (+ (:left v) (:right v))
                               :expr/negate  (- (:operand v))))))

(defspec exhaustive-dispatch-mixed 200
  (prop/for-all [v gen-status]
                (string?
                 (adt/adt-case TestStatus v
                               :status/running (str "running: " (:progress v) "%")
                               :status/idle    "idle"
                               :status/stopped "stopped"
                               :status/error   (str "error: " (:message v))))))

;; =============================================================================
;; Property 3: Keyword coercion (keyword→variant→keyword = identity)
;; =============================================================================

(defspec coercion-roundtrip-enum 200
  (prop/for-all [kw gen-direction-variant]
                (= kw (adt/adt-variant (->test-direction kw)))))

(defspec coercion-roundtrip-data 200
  (prop/for-all [kw gen-expr-variant]
                (= kw (adt/adt-variant (->test-expr kw)))))

(defspec coercion-roundtrip-mixed 200
  (prop/for-all [kw gen-status-variant]
                (= kw (adt/adt-variant (->test-status kw)))))

(defspec coercion-produces-correct-type 200
  (prop/for-all [kw gen-direction-variant]
                (let [v (->test-direction kw)]
                  (and (test-direction? v)
                       (= :TestDirection (adt/adt-type v))))))

;; =============================================================================
;; Property 4: Predicate correctness (exact type discrimination)
;; =============================================================================

(defspec predicate-positive-enum 200
  (prop/for-all [v gen-direction]
                (test-direction? v)))

(defspec predicate-positive-data 200
  (prop/for-all [v gen-expr]
                (test-expr? v)))

(defspec predicate-positive-mixed 200
  (prop/for-all [v gen-status]
                (test-status? v)))

(defspec predicate-negative-cross-type 200
  (prop/for-all [v gen-direction]
                (and (not (test-expr? v))
                     (not (test-status? v)))))

(defspec predicate-negative-cross-type-2 200
  (prop/for-all [v gen-expr]
                (and (not (test-direction? v))
                     (not (test-status? v)))))

;; =============================================================================
;; Property 5: Registry consistency
;; =============================================================================

(defspec all-constructed-values-are-valid 200
  (prop/for-all [v gen-direction]
                (adt/adt-valid? v)))

(defspec all-data-values-are-valid 200
  (prop/for-all [v gen-expr]
                (adt/adt-valid? v)))

(defspec all-mixed-values-are-valid 200
  (prop/for-all [v gen-status]
                (adt/adt-valid? v)))

;; =============================================================================
;; Property 6: ADT structural laws
;; =============================================================================

(defspec adt-predicate-holds 200
  (prop/for-all [v gen-direction]
                (adt/adt? v)))

(defspec adt-has-type-and-variant 200
  (prop/for-all [v gen-expr]
                (and (keyword? (adt/adt-type v))
                     (keyword? (adt/adt-variant v)))))

(defspec make-variant-matches-constructor-enum 200
  (prop/for-all [kw gen-direction-variant]
                (= (test-direction kw)
                   (adt/make-variant :TestDirection kw))))

(defspec make-variant-matches-constructor-data 200
  (prop/for-all [n gen/small-integer]
                (= (test-expr :expr/literal {:value n})
                   (adt/make-variant :TestExpr :expr/literal {:value n}))))

;; =============================================================================
;; Property 7: Variant keyword is preserved through construction
;; =============================================================================

(defspec variant-keyword-preserved-enum 200
  (prop/for-all [kw gen-direction-variant]
                (= kw (adt/adt-variant (test-direction kw)))))

(defspec variant-keyword-preserved-data 200
  (prop/for-all [kw gen-expr-variant]
                (let [v (case kw
                          :expr/literal (test-expr kw {:value 1})
                          :expr/add     (test-expr kw {:left 1 :right 2})
                          :expr/negate  (test-expr kw {:operand 1}))]
                  (= kw (adt/adt-variant v)))))

;; =============================================================================
;; Property 8: Data field preservation
;; =============================================================================

(defspec data-fields-preserved-literal 200
  (prop/for-all [n gen/small-integer]
                (= n (:value (test-expr :expr/literal {:value n})))))

(defspec data-fields-preserved-add 200
  (prop/for-all [l gen/small-integer
                 r gen/small-integer]
                (let [v (test-expr :expr/add {:left l :right r})]
                  (and (= l (:left v))
                       (= r (:right v))))))

;; =============================================================================
;; Property 9: adt-data extracts only user fields
;; =============================================================================

(defspec adt-data-excludes-meta-keys 200
  (prop/for-all [v gen-expr]
                (let [data (adt/adt-data v)]
                  (and (not (contains? data :adt/type))
                       (not (contains? data :adt/variant))))))

(defspec adt-data-empty-for-enums 200
  (prop/for-all [v gen-direction]
                (= {} (adt/adt-data v))))

;; =============================================================================
;; Property 10: Deserialize rejects corrupted values
;; =============================================================================

(defspec deserialize-rejects-missing-type 200
  (prop/for-all [kw gen-direction-variant]
                (nil? (adt/deserialize {:adt/variant kw}))))

(defspec deserialize-rejects-missing-variant 200
  (prop/for-all [kw gen-direction-variant]
                (nil? (adt/deserialize {:adt/type :TestDirection}))))

(defspec deserialize-rejects-wrong-variant 200
  (prop/for-all [kw gen-arbitrary-keyword]
    ;; Arbitrary keywords are almost certainly not valid TestDirection variants
    ;; (probability of collision is negligible)
                (let [result (adt/deserialize {:adt/type :TestDirection :adt/variant kw})]
                  (or (nil? result)  ; rejected as expected
                      (contains? #{:dir/north :dir/south :dir/east :dir/west} kw)))))  ; lucky collision
