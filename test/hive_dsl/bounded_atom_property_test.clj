(ns hive-dsl.bounded-atom-property-test
  "Property-based tests proving bounded-atom invariants hold
   under random operation sequences.

   Properties tested:
   - P1: Capacity invariant — bcount never exceeds max-entries
   - P2: Sweep idempotent — sweep!(sweep!(ba)) = sweep!(ba) in count
   - P3: Totality — no valid operation sequence throws
   - P4: TTL eviction — expired entries invisible to bget
   - P5: LRU preserves recent — most recently accessed survives eviction
   - P6: FIFO evicts oldest — first inserted evicted first on overflow
   - P7: Put/get roundtrip — bget after bput returns same data"
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [hive-dsl.bounded-atom :as ba]
            [hive-test.generators.bounded-atom :as gen-ba]
            [hive-test.properties.bounded-atom :as prop-ba]))

;; =============================================================================
;; API function map — bridges hive-test property helpers to hive-dsl.bounded-atom
;; =============================================================================

(def ba-api
  "Bounded-atom API fns for the operation interpreter."
  {:bput!   ba/bput!
   :bget    ba/bget
   :bclear! ba/bclear!
   :sweep!  ba/sweep!
   :bcount  ba/bcount})

;; =============================================================================
;; P1: CAPACITY INVARIANT — the critical mathematical proof
;;     After ANY sequence of operations, bcount <= max-entries
;;     (for :lru and :fifo policies that do capacity eviction)
;; =============================================================================

(defspec p1-bounded-atom-capacity-invariant 500
  (prop/for-all [[opts ops] (prop-ba/gen-batom-with-ops)]
    (let [batom (ba/bounded-atom opts)
          max-e (:max-entries opts)]
      ;; Execute all operations
      (prop-ba/run-ops ba-api batom ops)
      ;; THE INVARIANT: count must never exceed max-entries
      (<= (ba/bcount batom) max-e))))

;; Stress variant with larger sequences
(defspec p1-capacity-invariant-stress 100
  (prop/for-all [[opts ops] (gen/tuple gen-ba/gen-bounded-atom-opts-capacity
                                       gen-ba/gen-op-sequence-large)]
    (let [batom (ba/bounded-atom opts)
          max-e (:max-entries opts)]
      (prop-ba/run-ops ba-api batom ops)
      (<= (ba/bcount batom) max-e))))

;; =============================================================================
;; P2: SWEEP IDEMPOTENT — sweep!(sweep!(ba)) has same count as sweep!(ba)
;; =============================================================================

(defspec p2-sweep-idempotent 200
  (prop/for-all [[opts ops] (prop-ba/gen-batom-with-ops-all-policies)]
    (let [batom (ba/bounded-atom opts)]
      ;; Fill the atom with some operations
      (prop-ba/run-ops ba-api batom ops)
      ;; Sweep once
      (ba/sweep! batom :test)
      (let [count-after-first (ba/bcount batom)]
        ;; Sweep again — count should not change
        (ba/sweep! batom :test)
        (= count-after-first (ba/bcount batom))))))

;; =============================================================================
;; P3: TOTALITY — no sequence of valid operations throws an exception
;; =============================================================================

(defspec p3-bounded-atom-total 200
  (prop/for-all [[opts ops] (prop-ba/gen-batom-with-ops-all-policies)]
    (let [batom (ba/bounded-atom opts)]
      (try
        (prop-ba/run-ops ba-api batom ops)
        true
        (catch Exception _
          false)))))

;; =============================================================================
;; P4: TTL EVICTION — expired entries are invisible to bget
;; =============================================================================

(defspec p4-ttl-eviction-correct 200
  (prop/for-all [max-entries (gen/choose 5 50)
                 n-entries (gen/choose 1 20)
                 values (gen/vector gen/any-printable 20)]
    (let [;; Use very short TTL so entries expire quickly in test
          ttl-ms 1
          batom (ba/bounded-atom {:max-entries max-entries
                                  :ttl-ms ttl-ms
                                  :eviction-policy :lru})
          keys (mapv #(keyword (str "ttl-k" %)) (range n-entries))]
      ;; Put entries
      (doseq [[k v] (map vector keys values)]
        (ba/bput! batom k v))
      ;; Wait for TTL to expire
      (Thread/sleep 5)
      ;; All entries should now be invisible
      (every? nil? (map #(ba/bget batom %) keys)))))

;; =============================================================================
;; P5: LRU PRESERVES RECENT — most recently accessed entry survives eviction
;; =============================================================================

(defspec p5-lru-preserves-recent 200
  (prop/for-all [max-entries (gen/choose 2 10)
                 extra-values (gen/vector gen/any-printable 1 5)]
    (let [batom (ba/bounded-atom {:max-entries max-entries
                                  :eviction-policy :lru})
          ;; Fill exactly to capacity with distinct keys
          fill-keys (mapv #(keyword (str "fill-" %)) (range max-entries))]
      ;; Insert entries with slight delays for timestamp ordering
      (doseq [k fill-keys]
        (ba/bput! batom k {:key k})
        (Thread/sleep 1))
      ;; Access the first key to make it "recently used"
      (let [protected-key (first fill-keys)]
        (ba/bget batom protected-key)
        (Thread/sleep 1)
        ;; Now overflow with new entries — should evict least recently accessed
        (doseq [[i v] (map-indexed vector extra-values)]
          (ba/bput! batom (keyword (str "overflow-" i)) v)
          (Thread/sleep 1))
        ;; The protected key (most recently accessed) should survive
        (some? (ba/bget batom protected-key))))))

;; =============================================================================
;; P6: FIFO EVICTS OLDEST — first-inserted entry is evicted first on overflow
;; =============================================================================

(defspec p6-fifo-evicts-oldest 200
  (prop/for-all [max-entries (gen/choose 2 10)]
    (let [batom (ba/bounded-atom {:max-entries max-entries
                                  :eviction-policy :fifo})
          ;; Fill to capacity
          fill-keys (mapv #(keyword (str "fifo-" %)) (range max-entries))]
      ;; Insert entries — first key is the oldest
      (doseq [k fill-keys]
        (ba/bput! batom k {:key k})
        (Thread/sleep 1))
      (let [oldest-key (first fill-keys)]
        ;; Access oldest key — should NOT protect it under FIFO
        (ba/bget batom oldest-key)
        (Thread/sleep 1)
        ;; Overflow by one — oldest created should be evicted
        (ba/bput! batom :overflow-trigger {:new true})
        ;; The oldest key should have been evicted
        (nil? (ba/bget batom oldest-key))))))

;; =============================================================================
;; P7: PUT/GET ROUNDTRIP — bget after bput returns same data
;; =============================================================================

(defspec p7-put-get-roundtrip 200
  (prop/for-all [k gen-ba/gen-entry-key
                 v gen-ba/gen-entry-value]
    (let [;; Use large capacity so no eviction
          batom (ba/bounded-atom {:max-entries 1000})]
      (ba/bput! batom k v)
      (= v (ba/bget batom k)))))

;; Roundtrip with overwrite — latest value wins
(defspec p7-put-overwrite-roundtrip 200
  (prop/for-all [k gen-ba/gen-entry-key
                 v1 gen-ba/gen-entry-value
                 v2 gen-ba/gen-entry-value]
    (let [batom (ba/bounded-atom {:max-entries 1000})]
      (ba/bput! batom k v1)
      (ba/bput! batom k v2)
      (= v2 (ba/bget batom k)))))

;; =============================================================================
;; Additional: constructor totality — valid opts never throw
;; =============================================================================

(defspec constructor-totality 200
  (prop/for-all [opts gen-ba/gen-bounded-atom-opts]
    (try
      (ba/bounded-atom opts)
      true
      (catch Exception _
        false))))

;; =============================================================================
;; Additional: bclear! makes bcount zero
;; =============================================================================

(defspec clear-zeroes-count 200
  (prop/for-all [[opts ops] (prop-ba/gen-batom-with-ops-all-policies)]
    (let [batom (ba/bounded-atom opts)]
      (prop-ba/run-ops ba-api batom ops)
      (ba/bclear! batom)
      (= 0 (ba/bcount batom)))))

;; =============================================================================
;; Additional: bcount is always non-negative
;; =============================================================================

(defspec bcount-non-negative 200
  (prop/for-all [[opts ops] (prop-ba/gen-batom-with-ops-all-policies)]
    (let [batom (ba/bounded-atom opts)]
      (prop-ba/run-ops ba-api batom ops)
      (>= (ba/bcount batom) 0))))
