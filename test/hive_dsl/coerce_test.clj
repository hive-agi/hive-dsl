(ns hive-dsl.coerce-test
  (:require [clojure.test :refer [deftest is testing]]
            [hive-dsl.coerce :as c]
            [hive-dsl.result :as r]))

;; =============================================================================
;; ->int
;; =============================================================================

(deftest ->int-test
  (testing "valid coercions"
    (is (= {:ok 42}  (c/->int 42)))
    (is (= {:ok 42}  (c/->int "42")))
    (is (= {:ok -3}  (c/->int "-3")))
    (is (= {:ok 0}   (c/->int "0")))
    (is (= {:ok nil} (c/->int nil)))
    (is (= {:ok 3}   (c/->int 3.7))))

  (testing "invalid coercions"
    (is (r/err? (c/->int "abc")))
    (is (= :coerce/invalid-int (:error (c/->int "abc"))))
    (is (r/err? (c/->int "3.5")))
    (is (r/err? (c/->int [1 2])))))

;; =============================================================================
;; ->double
;; =============================================================================

(deftest ->double-test
  (testing "valid coercions"
    (is (= {:ok 0.9}  (c/->double 0.9)))
    (is (= {:ok 0.9}  (c/->double "0.9")))
    (is (= {:ok 42.0} (c/->double 42)))
    (is (= {:ok nil}  (c/->double nil))))

  (testing "invalid coercions"
    (is (r/err? (c/->double "abc")))
    (is (= :coerce/invalid-double (:error (c/->double "nope"))))))

;; =============================================================================
;; ->keyword
;; =============================================================================

(deftest ->keyword-test
  (testing "valid coercions"
    (is (= {:ok :both}  (c/->keyword "both")))
    (is (= {:ok :both}  (c/->keyword :both)))
    (is (= {:ok nil}    (c/->keyword nil)))
    (is (= {:ok nil}    (c/->keyword "")))
    (is (= {:ok nil}    (c/->keyword "  "))))

  (testing "invalid coercions"
    (is (r/err? (c/->keyword 42)))))

;; =============================================================================
;; ->boolean
;; =============================================================================

(deftest ->boolean-test
  (testing "valid coercions"
    (is (= {:ok true}  (c/->boolean "true")))
    (is (= {:ok true}  (c/->boolean "TRUE")))
    (is (= {:ok true}  (c/->boolean "1")))
    (is (= {:ok true}  (c/->boolean "yes")))
    (is (= {:ok false} (c/->boolean "false")))
    (is (= {:ok false} (c/->boolean "0")))
    (is (= {:ok false} (c/->boolean "no")))
    (is (= {:ok true}  (c/->boolean true)))
    (is (= {:ok false} (c/->boolean false)))
    (is (= {:ok nil}   (c/->boolean nil))))

  (testing "invalid coercions"
    (is (r/err? (c/->boolean "maybe")))
    (is (r/err? (c/->boolean 42)))))

;; =============================================================================
;; ->vec
;; =============================================================================

(deftest ->vec-test
  (testing "valid coercions"
    (is (= {:ok ["a" "b"]} (c/->vec ["a" "b"])))
    (is (= {:ok ["a" "b"]} (c/->vec '("a" "b"))))
    (is (= {:ok ["a" "b"]} (c/->vec "[\"a\",\"b\"]")))
    (is (= {:ok nil}        (c/->vec nil))))

  (testing "invalid coercions"
    (is (r/err? (c/->vec "not-json")))
    (is (r/err? (c/->vec "{\"a\":1}")))
    (is (r/err? (c/->vec 42)))))

;; =============================================================================
;; ->enum
;; =============================================================================

(deftest ->enum-test
  (let [allowed #{:outgoing :incoming :both}]
    (testing "valid coercions"
      (is (= {:ok :both}     (c/->enum "both" allowed)))
      (is (= {:ok :outgoing} (c/->enum :outgoing allowed)))
      (is (= {:ok nil}       (c/->enum nil allowed))))

    (testing "invalid coercions"
      (is (r/err? (c/->enum "nope" allowed)))
      (is (= :coerce/invalid-enum (:error (c/->enum "nope" allowed)))))))

;; =============================================================================
;; coerce-map
;; =============================================================================

(deftest coerce-map-test
  (testing "full coercion pipeline"
    (let [schema {:max_depth [:int]
                  :direction [:enum #{:outgoing :incoming :both}]
                  :confidence [:double]}
          result (c/coerce-map schema {:max_depth "3"
                                       :direction "both"
                                       :confidence "0.9"
                                       :scope "hive-mcp"})]
      (is (r/ok? result))
      (is (= {:max_depth 3
              :direction :both
              :confidence 0.9
              :scope "hive-mcp"}
             (:ok result)))))

  (testing "missing fields pass through"
    (let [schema {:max_depth [:int]}
          result (c/coerce-map schema {:scope "hive-mcp"})]
      (is (r/ok? result))
      (is (= {:scope "hive-mcp"} (:ok result)))))

  (testing "error includes field name"
    (let [schema {:max_depth [:int]}
          result (c/coerce-map schema {:max_depth "abc"})]
      (is (r/err? result))
      (is (= :max_depth (:field result)))))

  (testing "nil values are removed from result"
    (let [schema {:max_depth [:int]}
          result (c/coerce-map schema {:max_depth nil :scope "x"})]
      (is (r/ok? result))
      (is (= {:scope "x"} (:ok result)))))

  (testing "boolean coercion in map"
    (let [schema {:force [:boolean]}
          result (c/coerce-map schema {:force "true"})]
      (is (r/ok? result))
      (is (= {:force true} (:ok result)))))

  (testing "vec coercion in map"
    (let [schema {:relations [:vec]}
          result (c/coerce-map schema {:relations "[\"implements\",\"depends-on\"]"})]
      (is (r/ok? result))
      (is (= {:relations ["implements" "depends-on"]} (:ok result))))))
