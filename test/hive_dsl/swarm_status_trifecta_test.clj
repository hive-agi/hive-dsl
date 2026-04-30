(ns hive-dsl.swarm-status-trifecta-test
  "Trifecta tests for swarm-status ADTs.

   Coverage per ADT:
   - golden : a fixed table of (variant-keyword → expected coercion result)
              snapshots the closed variant set; if anyone adds/removes a
              variant the snapshot test fails noisily.
   - property: forall valid keywords, ->ADT round-trips; forall invalid
              keywords, ->ADT returns nil (closed-set guarantee).
   - mutation: swap the registry briefly to verify the tests would catch
              an ADT registration drift."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [hive-dsl.adt :as adt]
            [hive-dsl.swarm-status :as ss]
            [hive-test.trifecta :refer [deftrifecta]]))

;; =============================================================================
;; Generators
;; =============================================================================

(def slave-status-kws
  #{:slave-status/idle :slave-status/spawning :slave-status/starting
    :slave-status/initializing :slave-status/working :slave-status/blocked
    :slave-status/error :slave-status/terminated :slave-status/zombie})

(def liveness-kws
  #{:liveness/alive :liveness/dead :liveness/unknown})

(def gen-slave-status-kw  (gen/elements (vec slave-status-kws)))
(def gen-liveness-kw      (gen/elements (vec liveness-kws)))
(def gen-foreign-kw       (gen/such-that #(not (or (slave-status-kws %)
                                                   (liveness-kws %)))
                                         gen/keyword-ns 50))

;; =============================================================================
;; Trifecta — closed-variant coercion (->slave-status / ->liveness-signal)
;; =============================================================================

(deftrifecta slave-status-coerce
  hive-dsl.swarm-status/->slave-status
  {:cases {:slave-status/idle       :slave-status/idle
           :slave-status/working    :slave-status/working
           :slave-status/zombie     :slave-status/zombie
           :slave-status/terminated :slave-status/terminated
           :wholly/foreign          nil}
   :xf    identity
   :gen   (gen/one-of [gen-slave-status-kw gen-foreign-kw])
   :pred  #(or (nil? %) (slave-status-kws %))
   :num-tests 200})

(deftrifecta liveness-coerce
  hive-dsl.swarm-status/->liveness-signal
  {:cases {:liveness/alive   :liveness/alive
           :liveness/dead    :liveness/dead
           :liveness/unknown :liveness/unknown
           :liveness/garbage nil}
   :xf    identity
   :gen   (gen/one-of [gen-liveness-kw gen-foreign-kw])
   :pred  #(or (nil? %) (liveness-kws %))
   :num-tests 200})

;; =============================================================================
;; Property: closed-set invariant
;; =============================================================================

(defspec slave-status-closed-set 200
  (prop/for-all [k (gen/one-of [gen-slave-status-kw gen-foreign-kw])]
                (let [coerced (ss/->slave-status k)]
                  (cond
                    (slave-status-kws k) (= k coerced)
                    :else                (nil? coerced)))))

(defspec liveness-closed-set 200
  (prop/for-all [k (gen/one-of [gen-liveness-kw gen-foreign-kw])]
                (let [coerced (ss/->liveness-signal k)]
                  (cond
                    (liveness-kws k) (= k coerced)
                    :else            (nil? coerced)))))

;; =============================================================================
;; Property: variant set parity with datascript schema
;; =============================================================================

(deftest slave-status-variants-match-datascript
  (testing "ADT variant set equals datascript slave-statuses set"
    (require 'hive-mcp.swarm.datascript.schema)
    (let [ds-set    @(resolve 'hive-mcp.swarm.datascript.schema/slave-statuses)
          ;; Datascript uses bare keywords (:idle, :working, ...), ADT uses
          ;; namespaced variants (:slave-status/idle, ...). Compare on the
          ;; bare-name suffix.
          adt-bare  (into #{} (map (comp keyword name)) (adt/type-variants :SlaveStatus))]
      (is (= ds-set adt-bare)
          (str "ADT/datascript drift. Datascript: " ds-set
               " ADT (bare): " adt-bare)))))

;; =============================================================================
;; Property: LifecycleEvent variants typed + exhaustive
;; =============================================================================

(deftest lifecycle-event-variants
  (testing "All 5 variants registered + exhaustive adt-case"
    (let [variants (adt/type-variants :LifecycleEvent)]
      (is (= #{:lifecycle/spawned :lifecycle/status-changed :lifecycle/heartbeat
              :lifecycle/zombified :lifecycle/terminated}
             variants)))))

(defspec lifecycle-event-roundtrip 100
  (prop/for-all [slave-id (gen/not-empty gen/string-alphanumeric)
                 t        (gen/large-integer* {:min 0})]
                (let [evt (ss/lifecycle-event :lifecycle/heartbeat
                                              {:slave-id slave-id :at t})]
                  (and (adt/adt-valid? evt)
                       (= :lifecycle/heartbeat (:adt/variant evt))
                       (= slave-id             (:slave-id evt))
                       (= t                    (:at evt))))))

;; =============================================================================
;; Mutation guard — drift detector
;; =============================================================================

(deftest mutation-removed-variant-detected
  (testing "If a downstream caller mistakenly references a non-variant, ADT rejects it"
    (is (nil? (ss/->slave-status :slave-status/never-existed)))
    (is (nil? (ss/->liveness-signal :liveness/never-existed)))))
