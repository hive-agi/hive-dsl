(ns hive-dsl.batch-property-test
  "Property-based tests for normalize-tx-datum.
   Verifies totality, idempotency, and structural invariants."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [hive-dsl.batch :as batch]
            [hive-test.properties :as props]
            [hive-test.generators.kg :as gen-kg]))

;; =============================================================================
;; Generators
;; =============================================================================

(def gen-map-datum
  "Generator for entity-map tx-data."
  (gen/hash-map :id gen/pos-int :name gen/string-alphanumeric))

(def gen-keyword-vector-datum
  "Generator for [:db/add eid attr val] style tx-data."
  (gen/let [eid gen/pos-int
            attr (gen/elements [:kg-edge/from :kg-edge/to :db/ident])
            val gen/string-alphanumeric]
    [:db/add eid attr val]))

(def gen-datum-collection
  "Generator for collections of map datums."
  (gen/vector gen-map-datum 0 10))

(def gen-any-tx-data
  "Generator for any valid tx-datum input."
  (gen/one-of [gen-map-datum
               gen-keyword-vector-datum
               gen-datum-collection
               gen/string-alphanumeric
               gen/pos-int]))

;; =============================================================================
;; P1: Totality — never throws, always returns vector
;; =============================================================================

(props/defprop-total p1-normalize-totality
  batch/normalize-tx-datum
  gen-any-tx-data
  {:pred vector?})

;; =============================================================================
;; P2: Idempotent on well-formed data
;;     (mapcat normalize (normalize x)) == (normalize x)
;; =============================================================================

(defspec p2-normalize-idempotent 200
  (prop/for-all [x gen-any-tx-data]
                (let [once (batch/normalize-tx-datum x)
                      twice (vec (mapcat batch/normalize-tx-datum once))]
                  (= once twice))))

;; =============================================================================
;; P3: Map datum wraps to [map]
;; =============================================================================

(defspec p3-map-wraps-to-singleton 200
  (prop/for-all [m gen-map-datum]
                (= [m] (batch/normalize-tx-datum m))))

;; =============================================================================
;; P4: [:db/add ...] wraps to [[:db/add ...]]
;; =============================================================================

(defspec p4-keyword-vector-wraps-to-singleton 200
  (prop/for-all [v gen-keyword-vector-datum]
                (= [v] (batch/normalize-tx-datum v))))

;; =============================================================================
;; P5: Collection passes through as (vec coll)
;; =============================================================================

(defspec p5-collection-passes-through 200
  (prop/for-all [coll gen-datum-collection]
                (= (vec coll) (batch/normalize-tx-datum coll))))

;; =============================================================================
;; P6: Count preservation on collections
;; =============================================================================

(defspec p6-count-preservation 200
  (prop/for-all [coll gen-datum-collection]
                (= (count coll) (count (batch/normalize-tx-datum coll)))))
