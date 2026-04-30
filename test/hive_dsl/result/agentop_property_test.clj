(ns hive-dsl.result.agentop-property-test
  "Property-based tests for hive-dsl.result.agentop."
  (:require [clojure.test :refer [deftest is]]
            [clojure.test.check :as tc]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-dsl.result :as r]
            [hive-dsl.result.agentop :as a]))

;; -----------------------------------------------------------------------------
;; Generators
;; -----------------------------------------------------------------------------

(def gen-result
  "Any Result value with simple ok/err payload."
  (gen/one-of
   [(gen/fmap r/ok gen/int)
    (gen/fmap #(r/err % {}) (gen/elements [:a :b :c]))]))

(def gen-results-vec
  (gen/vector gen-result 0 10))

;; -----------------------------------------------------------------------------
;; tap: identity over Result value
;; -----------------------------------------------------------------------------

(defspec tap-identity-law 100
  (prop/for-all [result gen-result]
    (= result (a/tap result (constantly :ignored)))))

(defspec tap-invokes-only-on-ok 100
  (prop/for-all [result gen-result]
    (let [counter (atom 0)]
      (a/tap result (fn [_] (swap! counter inc)))
      (if (r/ok? result)
        (= 1 @counter)
        (= 0 @counter)))))

;; -----------------------------------------------------------------------------
;; recover: identity on Ok, replaces on Err
;; -----------------------------------------------------------------------------

(defspec recover-on-ok-is-identity 100
  (prop/for-all [v gen/int]
    (let [r1 (r/ok v)]
      (= r1 (a/recover r1 (fn [_] (r/ok :different)))))))

(defspec recover-on-err-uses-f 100
  (prop/for-all [err-cat (gen/elements [:a :b :c])
                 replacement gen/int]
    (= (r/ok replacement)
       (a/recover (r/err err-cat) (fn [_] (r/ok replacement))))))

;; -----------------------------------------------------------------------------
;; retry-on: exhaustion count invariant
;; -----------------------------------------------------------------------------

(defspec retry-on-exhausts-max-attempts 50
  (prop/for-all [max-attempts (gen/choose 1 10)]
    (let [calls (atom 0)
          thunk #(do (swap! calls inc) (r/err :always))]
      (a/retry-on thunk {:max max-attempts :backoff-ms (constantly 0)})
      (= max-attempts @calls))))

(defspec retry-on-stops-at-first-ok 50
  (prop/for-all [max-attempts (gen/choose 2 10)
                 succeed-at (gen/choose 1 5)]
    (let [calls (atom 0)
          thunk #(let [n (swap! calls inc)]
                   (if (>= n succeed-at) (r/ok n) (r/err :transient)))
          result (a/retry-on thunk {:max max-attempts :backoff-ms (constantly 0)})]
      (if (<= succeed-at max-attempts)
        (and (r/ok? result) (= succeed-at @calls))
        (and (r/err? result) (= max-attempts @calls))))))

;; -----------------------------------------------------------------------------
;; fan-in: order + all-ok ↔ no-err
;; -----------------------------------------------------------------------------

(defspec fan-in-all-ok-preserves-order 100
  (prop/for-all [xs (gen/vector gen/int 0 20)]
    (= (r/ok (vec xs))
       (a/fan-in (mapv r/ok xs)))))

(defspec fan-in-any-err-is-err 100
  (prop/for-all [results gen-results-vec]
    (let [result (a/fan-in results)]
      (if (some r/err? results)
        (r/err? result)
        (r/ok? result)))))

(defspec fan-in-err-collects-all-partials 100
  (prop/for-all [results gen-results-vec]
    (let [errs (filter r/err? results)
          result (a/fan-in results)]
      (if (seq errs)
        (= (count errs) (count (:err/partials result)))
        true))))

;; -----------------------------------------------------------------------------
;; fan-out: commutes with fan-in over synchronous thunks
;; -----------------------------------------------------------------------------

(defspec fan-out-matches-fan-in-on-pure-thunks 30
  (prop/for-all [vs (gen/vector gen/int 0 8)]
    (let [thunks (mapv (fn [v] #(r/ok v)) vs)]
      (= (a/fan-in (mapv #(%) thunks))
         (a/fan-out thunks)))))

;; -----------------------------------------------------------------------------
;; with-budget: concurrent deduction monotonicity
;; -----------------------------------------------------------------------------

(deftest with-budget-concurrent-monotonic
  (let [budget 10000
        ba (atom budget)
        n-threads 16
        ops-per-thread 100
        cost 5
        successes (atom 0)
        futs (vec
              (for [_ (range n-threads)]
                (future
                  (dotimes [_ ops-per-thread]
                    (let [r (a/with-budget ba cost #(r/ok :done))]
                      (when (r/ok? r) (swap! successes inc)))))))]
    (run! deref futs)
    ;; Invariant 1: remaining is non-negative
    (is (>= @ba 0))
    ;; Invariant 2: budget accounting is exact
    (is (= budget (+ @ba (* cost @successes)))
        "remaining + sum(deductions) must equal original budget")))

(defspec with-budget-never-over-deducts 30
  (prop/for-all [cost (gen/choose 1 50)
                 initial (gen/choose 0 200)
                 attempts (gen/choose 1 20)]
    (let [ba (atom initial)]
      (dotimes [_ attempts]
        (a/with-budget ba cost #(r/ok :done)))
      (>= @ba 0))))

;; -----------------------------------------------------------------------------
;; with-persona: binding is thread-local
;; -----------------------------------------------------------------------------

(deftest with-persona-thread-local
  (let [outer-seen (atom nil)
        a-seen (atom nil)
        b-seen (atom nil)]
    (is (nil? a/*persona*))
    (a/with-persona {:persona/id :A}
      #(do
         (reset! a-seen a/*persona*)
         (a/with-persona {:persona/id :B}
           (fn [] (reset! b-seen a/*persona*) (r/ok nil)))
         (r/ok nil)))
    (reset! outer-seen a/*persona*)
    (is (= {:persona/id :A} @a-seen))
    (is (= {:persona/id :B} @b-seen))
    (is (nil? @outer-seen))))
