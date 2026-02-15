(ns hive-dsl.adt-test
  "Unit tests for hive-dsl.adt — defadt macro, constructors, predicates,
   coercion, serialization, validation, and exhaustive pattern matching."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-dsl.adt :as adt]))

;; =============================================================================
;; Test ADT Definitions
;; =============================================================================

;; Enum ADT (no data fields)
(adt/defadt Color
  "Primary colors."
  :color/red
  :color/green
  :color/blue)

;; Data-carrying ADT
(adt/defadt Shape
  "Geometric shapes with associated data."
  [:shape/circle {:radius number?}]
  [:shape/rect   {:w number? :h number?}]
  :shape/point)

;; Mixed ADT (some variants have data, some don't)
(adt/defadt Result
  "Test result type."
  [:result/ok   {:value any?}]
  [:result/err  {:message string?}]
  :result/pending)

;; =============================================================================
;; defadt Macro — Generated Vars
;; =============================================================================

(deftest defadt-generates-type-var
  (testing "Type var contains correct metadata"
    (is (map? Color))
    (is (= :Color (:type Color)))
    (is (= #{:color/red :color/green :color/blue} (:variants Color)))
    (is (= {} (:schemas Color))))

  (testing "Data-carrying type has schemas"
    (is (map? Shape))
    (is (= :Shape (:type Shape)))
    (is (= #{:shape/circle :shape/rect :shape/point} (:variants Shape)))
    (is (contains? (:schemas Shape) :shape/circle))
    (is (contains? (:schemas Shape) :shape/rect))
    (is (not (contains? (:schemas Shape) :shape/point)))))

(deftest defadt-registers-in-global-registry
  (testing "Types are registered"
    (is (adt/type-registered? :Color))
    (is (adt/type-registered? :Shape))
    (is (adt/type-registered? :Result)))

  (testing "Variants are accessible via registry"
    (is (= #{:color/red :color/green :color/blue}
           (adt/type-variants :Color)))
    (is (= #{:shape/circle :shape/rect :shape/point}
           (adt/type-variants :Shape)))))

;; =============================================================================
;; Constructor
;; =============================================================================

(deftest constructor-enum-variants
  (testing "Enum constructor produces correct ADT values"
    (let [c (color :color/red)]
      (is (= :Color (:adt/type c)))
      (is (= :color/red (:adt/variant c)))))

  (testing "Enum constructor rejects unknown variants"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Unknown Color variant"
                          (color :color/purple)))))

(deftest constructor-data-variants
  (testing "Data constructor merges data fields"
    (let [s (shape :shape/circle {:radius 5})]
      (is (= :Shape (:adt/type s)))
      (is (= :shape/circle (:adt/variant s)))
      (is (= 5 (:radius s)))))

  (testing "Data constructor with no data"
    (let [s (shape :shape/point)]
      (is (= :Shape (:adt/type s)))
      (is (= :shape/point (:adt/variant s)))))

  (testing "Data constructor rejects unknown variants"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Unknown Shape variant"
                          (shape :shape/triangle {:sides 3})))))

(deftest constructor-mixed-variants
  (testing "Mixed ADT with data and enum variants"
    (let [ok (result :result/ok {:value 42})
          pending (result :result/pending)]
      (is (= 42 (:value ok)))
      (is (= :result/pending (:adt/variant pending))))))

;; =============================================================================
;; Predicate
;; =============================================================================

(deftest predicate-positive
  (testing "Predicate returns true for matching types"
    (is (color? (color :color/red)))
    (is (shape? (shape :shape/circle {:radius 5})))
    (is (result? (result :result/pending)))))

(deftest predicate-negative
  (testing "Predicate returns false for non-matching types"
    (is (not (color? (shape :shape/point))))
    (is (not (shape? (color :color/red))))
    (is (not (color? {})))
    (is (not (color? nil)))
    (is (not (color? "red")))))

;; =============================================================================
;; Keyword Coercion
;; =============================================================================

(deftest coercion-valid-keywords
  (testing "Valid keywords coerce to ADT values"
    (let [c (->color :color/red)]
      (is (some? c))
      (is (= :Color (:adt/type c)))
      (is (= :color/red (:adt/variant c))))))

(deftest coercion-invalid-keywords
  (testing "Invalid keywords return nil"
    (is (nil? (->color :color/purple)))
    (is (nil? (->shape :shape/triangle)))))

(deftest coercion-roundtrip
  (testing "Keyword → ADT → keyword is identity"
    (is (= :color/red (adt/adt-variant (->color :color/red))))
    (is (= :color/green (adt/adt-variant (->color :color/green))))
    (is (= :color/blue (adt/adt-variant (->color :color/blue))))))

;; =============================================================================
;; ADT Value Operations
;; =============================================================================

(deftest adt-type-extraction
  (testing "adt-type extracts type keyword"
    (is (= :Color (adt/adt-type (color :color/red))))
    (is (= :Shape (adt/adt-type (shape :shape/point)))))

  (testing "adt-type returns nil for non-ADT"
    (is (nil? (adt/adt-type {})))
    (is (nil? (adt/adt-type nil)))))

(deftest adt-variant-extraction
  (testing "adt-variant extracts variant keyword"
    (is (= :color/red (adt/adt-variant (color :color/red))))
    (is (= :shape/circle (adt/adt-variant (shape :shape/circle {:radius 5})))))

  (testing "adt-variant returns nil for non-ADT"
    (is (nil? (adt/adt-variant {})))
    (is (nil? (adt/adt-variant nil)))))

(deftest adt-data-extraction
  (testing "adt-data extracts data fields"
    (is (= {:radius 5}
           (adt/adt-data (shape :shape/circle {:radius 5}))))
    (is (= {:w 10 :h 20}
           (adt/adt-data (shape :shape/rect {:w 10 :h 20})))))

  (testing "adt-data returns empty map for enum variants"
    (is (= {} (adt/adt-data (color :color/red))))
    (is (= {} (adt/adt-data (shape :shape/point))))))

(deftest adt-predicates
  (testing "adt? recognizes ADT values"
    (is (adt/adt? (color :color/red)))
    (is (adt/adt? (shape :shape/circle {:radius 5}))))

  (testing "adt? rejects non-ADT values"
    (is (not (adt/adt? {})))
    (is (not (adt/adt? nil)))
    (is (not (adt/adt? {:adt/type :Color})))  ; missing variant
    (is (not (adt/adt? {:adt/variant :color/red})))))  ; missing type

(deftest adt-valid-predicate
  (testing "adt-valid? for registered types"
    (is (adt/adt-valid? (color :color/red)))
    (is (adt/adt-valid? (shape :shape/circle {:radius 5}))))

  (testing "adt-valid? rejects unregistered types"
    (is (not (adt/adt-valid? {:adt/type :Unknown :adt/variant :x/y})))))

;; =============================================================================
;; Serialization Round-trip
;; =============================================================================

(deftest serialize-produces-plain-map
  (testing "Serialize returns a plain map"
    (let [s (adt/serialize (color :color/red))]
      (is (map? s))
      (is (= :Color (:adt/type s)))
      (is (= :color/red (:adt/variant s)))))

  (testing "Serialize includes data fields"
    (let [s (adt/serialize (shape :shape/circle {:radius 5}))]
      (is (= 5 (:radius s))))))

(deftest deserialize-validates
  (testing "Deserialize accepts valid maps"
    (is (some? (adt/deserialize {:adt/type :Color :adt/variant :color/red})))
    (is (some? (adt/deserialize {:adt/type :Shape :adt/variant :shape/circle :radius 5}))))

  (testing "Deserialize rejects invalid maps"
    (is (nil? (adt/deserialize {})))
    (is (nil? (adt/deserialize {:adt/type :Color})))
    (is (nil? (adt/deserialize {:adt/type :Unknown :adt/variant :x/y})))
    (is (nil? (adt/deserialize {:adt/type :Color :adt/variant :color/purple})))))

(deftest serialize-deserialize-roundtrip
  (testing "Serialize then deserialize is identity for enum"
    (let [v (color :color/red)]
      (is (= v (adt/deserialize (adt/serialize v))))))

  (testing "Serialize then deserialize preserves data"
    (let [v (shape :shape/circle {:radius 5})]
      (is (= v (adt/deserialize (adt/serialize v)))))))

(deftest edn-roundtrip
  (testing "ADT values survive EDN print/read"
    (let [v (shape :shape/rect {:w 10 :h 20})
          s (pr-str v)
          v2 (clojure.edn/read-string s)]
      (is (= v v2))
      (is (adt/adt-valid? v2)))))

;; =============================================================================
;; Schema Validation
;; =============================================================================

(deftest validate-accepts-valid-values
  (testing "Valid data-carrying variant passes"
    (is (= (shape :shape/circle {:radius 5})
           (adt/validate Shape (shape :shape/circle {:radius 5})))))

  (testing "Enum variant passes (no schema)"
    (is (= (color :color/red)
           (adt/validate Color (color :color/red))))))

(deftest validate-rejects-invalid-values
  (testing "Type mismatch throws"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Type mismatch"
                          (adt/validate Color (shape :shape/point)))))

  (testing "Unknown variant throws"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Unknown variant"
                          (adt/validate Color {:adt/type :Color
                                               :adt/variant :color/purple}))))

  (testing "Missing required field throws"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Schema validation failed"
                          (adt/validate Shape (shape :shape/circle)))))

  (testing "Invalid field type throws"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Schema validation failed"
                          (adt/validate Shape
                                        {:adt/type :Shape
                                         :adt/variant :shape/circle
                                         :radius "not-a-number"})))))

;; =============================================================================
;; adt-case: Exhaustive Pattern Matching
;; =============================================================================

(deftest adt-case-dispatches-correctly
  (testing "Dispatches to correct branch for enum"
    (is (= "red"
           (adt/adt-case Color (color :color/red)
                         :color/red "red"
                         :color/green "green"
                         :color/blue "blue")))
    (is (= "green"
           (adt/adt-case Color (color :color/green)
                         :color/red "red"
                         :color/green "green"
                         :color/blue "blue"))))

  (testing "Dispatches to correct branch for data variants"
    (is (= 5
           (adt/adt-case Shape (shape :shape/circle {:radius 5})
                         :shape/circle (:radius (shape :shape/circle {:radius 5}))
                         :shape/rect   0
                         :shape/point  0)))))

(deftest adt-case-with-expr-binding
  (testing "Expression is evaluated and accessible in branch"
    (let [s (shape :shape/rect {:w 10 :h 20})]
      (is (= 200
             (adt/adt-case Shape s
                           :shape/circle (* Math/PI (:radius s) (:radius s))
                           :shape/rect   (* (:w s) (:h s))
                           :shape/point  0))))))

;; =============================================================================
;; make-variant (low-level)
;; =============================================================================

(deftest make-variant-basic
  (testing "Creates ADT value without data"
    (let [v (adt/make-variant :Color :color/red)]
      (is (= :Color (:adt/type v)))
      (is (= :color/red (:adt/variant v)))))

  (testing "Creates ADT value with data"
    (let [v (adt/make-variant :Shape :shape/circle {:radius 5})]
      (is (= :Shape (:adt/type v)))
      (is (= :shape/circle (:adt/variant v)))
      (is (= 5 (:radius v)))))

  (testing "Empty data map produces clean value"
    (let [v (adt/make-variant :Color :color/red {})]
      (is (= {:adt/type :Color :adt/variant :color/red} v)))))

;; =============================================================================
;; coerce-keyword
;; =============================================================================

(deftest coerce-keyword-valid
  (testing "Valid variant returns ADT value"
    (let [v (adt/coerce-keyword :Color :color/red)]
      (is (some? v))
      (is (color? v)))))

(deftest coerce-keyword-invalid
  (testing "Invalid variant returns nil"
    (is (nil? (adt/coerce-keyword :Color :color/purple))))

  (testing "Unregistered type returns nil"
    (is (nil? (adt/coerce-keyword :Unknown :x/y)))))

;; =============================================================================
;; Edge Cases
;; =============================================================================

(deftest adt-value-works-with-standard-map-operations
  (testing "assoc works (returns new map, might break ADT contract)"
    (let [v (color :color/red)
          v2 (assoc v :extra "data")]
      (is (= "data" (:extra v2)))
      (is (color? v2))))  ; still passes type check

  (testing "dissoc can break ADT"
    (let [v (color :color/red)
          broken (dissoc v :adt/type)]
      (is (not (adt/adt? broken)))))

  (testing "merge works"
    (let [v (shape :shape/circle {:radius 5})
          v2 (merge v {:radius 10})]
      (is (= 10 (:radius v2)))
      (is (shape? v2)))))
