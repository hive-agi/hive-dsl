(ns hive-dsl.result.agentop-test
  "Unit tests for hive-dsl.result.agentop combinators."
  (:require [clojure.test :refer [deftest testing is]]
            [hive-dsl.result :as r]
            [hive-dsl.result.agentop :as a]))

;; -----------------------------------------------------------------------------
;; tap
;; -----------------------------------------------------------------------------

(deftest tap-invokes-on-ok
  (let [sink (atom nil)]
    (a/tap (r/ok 42) #(reset! sink %))
    (is (= 42 @sink))))

(deftest tap-skips-on-err
  (let [sink (atom :untouched)]
    (a/tap (r/err :boom) #(reset! sink %))
    (is (= :untouched @sink))))

(deftest tap-returns-result-unchanged
  (is (= (r/ok 1) (a/tap (r/ok 1) identity)))
  (is (= (r/err :boom) (a/tap (r/err :boom) identity))))

;; -----------------------------------------------------------------------------
;; recover
;; -----------------------------------------------------------------------------

(deftest recover-ok-passthrough
  (is (= (r/ok 1) (a/recover (r/ok 1) (fn [_] (r/ok :nope))))))

(deftest recover-err-replaces
  (is (= (r/ok :rescued)
         (a/recover (r/err :boom) (fn [_] (r/ok :rescued))))))

(deftest recover-wraps-plain-return-as-ok
  (is (= (r/ok :plain)
         (a/recover (r/err :boom) (fn [_] :plain)))))

;; -----------------------------------------------------------------------------
;; retry-on
;; -----------------------------------------------------------------------------

(deftest retry-on-returns-first-ok
  (let [calls (atom 0)
        thunk #(do (swap! calls inc) (r/ok @calls))]
    (is (= (r/ok 1) (a/retry-on thunk {:max 5})))
    (is (= 1 @calls))))

(deftest retry-on-exhausts-max
  (let [calls (atom 0)
        thunk #(do (swap! calls inc) (r/err :always-fails))]
    (is (r/err? (a/retry-on thunk {:max 3})))
    (is (= 3 @calls))))

(deftest retry-on-respects-pred
  (let [calls (atom 0)
        thunk #(do (swap! calls inc) (r/err :fatal))]
    (a/retry-on thunk {:max 5 :pred #(not= :fatal (:error %))})
    ;; pred is false for :fatal, so we stop after first call
    (is (= 1 @calls))))

(deftest retry-on-recovers-on-nth-attempt
  (let [calls (atom 0)
        thunk #(let [n (swap! calls inc)]
                 (if (< n 3) (r/err :transient) (r/ok n)))]
    (is (= (r/ok 3) (a/retry-on thunk {:max 5})))
    (is (= 3 @calls))))

;; -----------------------------------------------------------------------------
;; fan-in
;; -----------------------------------------------------------------------------

(deftest fan-in-all-ok-preserves-order
  (is (= (r/ok [1 2 3])
         (a/fan-in [(r/ok 1) (r/ok 2) (r/ok 3)]))))

(deftest fan-in-empty-is-ok-empty
  (is (= (r/ok []) (a/fan-in []))))

(deftest fan-in-err-collects-partials
  (let [result (a/fan-in [(r/ok 1) (r/err :a) (r/ok 2) (r/err :b)])]
    (is (r/err? result))
    (is (= :partial (:error result)))
    (is (= [1 2] (:err/oks result)))
    (is (= 2 (count (:err/partials result))))))

;; -----------------------------------------------------------------------------
;; fan-out
;; -----------------------------------------------------------------------------

(deftest fan-out-parallel-all-ok
  (let [thunks [#(r/ok :a) #(r/ok :b) #(r/ok :c)]]
    (is (= (r/ok [:a :b :c]) (a/fan-out thunks)))))

(deftest fan-out-exception-becomes-err
  (let [result (a/fan-out [#(r/ok :fine) #(throw (ex-info "boom" {}))])]
    (is (r/err? result))
    (is (some #(= :thunk-threw (:error %)) (:err/partials result)))))

;; -----------------------------------------------------------------------------
;; with-budget
;; -----------------------------------------------------------------------------

(deftest with-budget-happy-path
  (let [ba (atom 100)
        result (a/with-budget ba 30 #(r/ok :done))]
    (is (= (r/ok :done) result))
    (is (= 70 @ba))))

(deftest with-budget-exhausted-does-not-deduct
  (let [ba (atom 10)
        result (a/with-budget ba 30 #(r/ok :should-not-run))]
    (is (r/err? result))
    (is (= :budget-exhausted (:error result)))
    (is (= 10 @ba) "budget MUST not change when op rejected")
    (is (= 30 (:budget/needed result)))
    (is (= 10 (:budget/remaining result)))))

(deftest with-budget-consumes-even-on-err
  (let [ba (atom 100)
        result (a/with-budget ba 25 #(r/err :op-failed))]
    (is (r/err? result))
    (is (= 75 @ba) "budget consumed regardless of op outcome")))

(deftest with-budget-zero-cost
  (let [ba (atom 0)
        result (a/with-budget ba 0 #(r/ok :free))]
    (is (= (r/ok :free) result))
    (is (= 0 @ba))))

;; -----------------------------------------------------------------------------
;; with-persona
;; -----------------------------------------------------------------------------

(deftest with-persona-binds-dynamic-var
  (let [seen (atom nil)]
    (a/with-persona {:persona/id :explorer}
      #(do (reset! seen a/*persona*) (r/ok nil)))
    (is (= {:persona/id :explorer} @seen))))

(deftest with-persona-unbinds-after
  (a/with-persona {:persona/id :explorer} #(r/ok nil))
  (is (nil? a/*persona*)))

(deftest with-persona-enriches-err
  (let [result (a/with-persona {:persona/id :critic}
                 #(r/err :op-failed))]
    (is (r/err? result))
    (is (= :critic (:persona/id result)))))

(deftest with-persona-passes-ok-unchanged
  (let [result (a/with-persona {:persona/id :critic} #(r/ok 42))]
    (is (= (r/ok 42) result))))

;; -----------------------------------------------------------------------------
;; with-crdt (T2 stub)
;; -----------------------------------------------------------------------------

(deftest with-crdt-binds-dynamic-var
  (let [seen (atom nil)
        crdt-ref ::fake-ref]
    (a/with-crdt crdt-ref #(do (reset! seen a/*crdt*) (r/ok nil)))
    (is (= ::fake-ref @seen))))

(deftest with-crdt-unbinds-after
  (a/with-crdt ::fake #(r/ok nil))
  (is (nil? a/*crdt*)))

;; -----------------------------------------------------------------------------
;; Integration: the algebra threads cleanly
;; -----------------------------------------------------------------------------

(deftest composed-pipeline
  (testing "tap + recover + with-budget compose"
    (let [ba (atom 100)
          log (atom [])
          result (-> (r/ok 10)
                     (a/tap #(swap! log conj [:ok %]))
                     (a/recover (fn [_] (r/ok :rescued))))]
      (is (= (r/ok 10) result))
      (is (= [[:ok 10]] @log))
      ;; use with-budget outside
      (is (= (r/ok :done)
             (a/with-budget ba 50 #(r/ok :done))))
      (is (= 50 @ba)))))
