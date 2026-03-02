(ns hive-dsl.resource-property-test
  "Property-based tests for resource lifecycle macros.
   Verifies cleanup totality and LIFO ordering invariants."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [hive-dsl.result :as r]
            [hive-dsl.resource :as res]))

;; =============================================================================
;; P1: Cleanup totality — release ALWAYS runs for ok? acquires
;; =============================================================================

(defspec p1-cleanup-totality 100
  (prop/for-all [value gen/any-printable
                 throw? gen/boolean]
                (let [released (atom false)
                      release-fn (fn [_] (reset! released true))
                      result (try
                               (res/with-resource [r (r/ok value)]
                                 :release release-fn
                                 (if throw?
                                   (throw (Exception. "test"))
                                   (r/ok :done)))
                               (catch Exception _ :threw))]
      ;; Release must have run regardless of body outcome
                  @released)))

;; =============================================================================
;; P2: No release on acquire failure
;; =============================================================================

(defspec p2-no-release-on-acquire-failure 100
  (prop/for-all [category gen/keyword]
                (let [released (atom false)
                      result (res/with-resource [r (r/err category)]
                               :release (fn [_] (reset! released true))
                               (r/ok :unreachable))]
                  (and (r/err? result)
                       (not @released)))))

;; =============================================================================
;; P3: LIFO ordering — N resources released in reverse acquisition order
;; =============================================================================

(defspec p3-lifo-ordering 100
  (prop/for-all [n (gen/choose 1 20)]
                (let [cleanup-log (atom [])
                      scope (res/new-scope)]
      ;; Acquire N resources
                  (doseq [i (range n)]
                    (res/scope-acquire! scope
                                        (fn [_] (swap! cleanup-log conj i))
                                        (fn [x] (r/ok x))
                                        [i]))
      ;; Release
                  (res/scope-release! scope)
      ;; Verify LIFO: last acquired (n-1) should be first released
                  (= (reverse (range n)) @cleanup-log))))

;; =============================================================================
;; P4: Failed acquires don't affect scope size
;; =============================================================================

(defspec p4-failed-acquires-transparent 100
  (prop/for-all [n-ok (gen/choose 0 10)
                 n-fail (gen/choose 0 10)]
                (let [scope (res/new-scope)]
      ;; Mix ok and fail acquires
                  (doseq [_ (range n-ok)]
                    (res/scope-acquire! scope identity
                                        (fn [] (r/ok :resource))
                                        []))
                  (doseq [_ (range n-fail)]
                    (res/scope-acquire! scope identity
                                        (fn [] (r/err :resource/acquire-failed))
                                        []))
      ;; Scope should only contain successful acquires
                  (= n-ok (count @scope)))))

;; =============================================================================
;; P5: Scope cleanup is total — all resources released even with errors
;; =============================================================================

(defspec p5-scope-cleanup-total 100
  (prop/for-all [n (gen/choose 1 10)
                 fail-indices (gen/vector (gen/choose 0 9) 0 5)]
                (let [attempted (atom #{})
                      fail-set (set fail-indices)
                      scope (res/new-scope)]
                  (doseq [i (range n)]
                    (res/scope-acquire! scope
                                        (fn [r]
                                          (swap! attempted conj r)
                                          (when (fail-set r)
                                            (throw (Exception. (str "fail-" r)))))
                                        (fn [x] (r/ok x))
                                        [i]))
                  (res/scope-release! scope)
      ;; Every resource should have had cleanup attempted
                  (= (set (range n)) @attempted))))
