(ns hive-dsl.resource-test
  (:require [clojure.test :refer [deftest testing is]]
            [hive-dsl.result :as r]
            [hive-dsl.resource :as res]))

;; =============================================================================
;; with-resource tests
;; =============================================================================

(deftest with-resource-ok-path-test
  (testing "with-resource returns body result on success"
    (let [released (atom false)
          result (res/with-resource [conn (r/ok :my-conn)]
                   :release (fn [_] (reset! released true))
                   (r/ok {:data "processed"}))]
      (is (r/ok? result))
      (is (= {:data "processed"} (:ok result)))
      (is @released "release should have been called"))))

(deftest with-resource-acquire-failure-test
  (testing "with-resource short-circuits on acquire error"
    (let [released (atom false)
          result (res/with-resource [conn (r/err :resource/acquire-failed {:msg "no db"})]
                   :release (fn [_] (reset! released true))
                   (r/ok :should-not-reach))]
      (is (r/err? result))
      (is (= :resource/acquire-failed (:error result)))
      (is (false? @released) "release should NOT be called on acquire failure"))))

(deftest with-resource-cleanup-on-exception-test
  (testing "with-resource runs release even when body throws"
    (let [released (atom false)]
      (is (thrown? Exception
                   (res/with-resource [conn (r/ok :my-conn)]
                     :release (fn [_] (reset! released true))
                     (throw (Exception. "boom")))))
      (is @released "release must run even on exception"))))

(deftest with-resource-release-error-swallowed-test
  (testing "with-resource swallows release errors"
    (let [result (res/with-resource [conn (r/ok :my-conn)]
                   :release (fn [_] (throw (Exception. "release failed")))
                   (r/ok :done))]
      (is (r/ok? result))
      (is (= :done (:ok result))))))

(deftest with-resource-composes-with-let-ok-test
  (testing "with-resource composes naturally with let-ok"
    (let [released (atom false)
          result (res/with-resource [conn (r/ok {:db "test"})]
                   :release (fn [_] (reset! released true))
                   (r/let-ok [data (r/ok "raw-data")
                              processed (r/ok (str data "-processed"))]
                             (r/ok {:conn conn :result processed})))]
      (is (r/ok? result))
      (is (= {:conn {:db "test"} :result "raw-data-processed"} (:ok result)))
      (is @released))))

(deftest with-resource-let-ok-short-circuit-test
  (testing "with-resource cleans up when let-ok short-circuits"
    (let [released (atom false)
          result (res/with-resource [conn (r/ok :conn)]
                   :release (fn [_] (reset! released true))
                   (r/let-ok [x (r/err :io/timeout {:msg "slow"})]
                             (r/ok :unreachable)))]
      (is (r/err? result))
      (is (= :io/timeout (:error result)))
      (is @released "release must run even on let-ok short-circuit"))))

;; =============================================================================
;; with-scope tests
;; =============================================================================

(deftest with-scope-basic-test
  (testing "with-scope creates scope and cleans up"
    (let [cleanup-log (atom [])
          result (res/with-scope [scope]
                   (res/scope-acquire! scope
                                       (fn [r] (swap! cleanup-log conj :db-closed) nil)
                                       (fn [config] (r/ok {:db config}))
                                       ["test-config"])
                   (r/ok :done))]
      (is (= :done (:ok result)))
      (is (= [:db-closed] @cleanup-log)))))

(deftest with-scope-lifo-order-test
  (testing "with-scope releases resources in LIFO order"
    (let [cleanup-log (atom [])]
      (res/with-scope [scope]
        (res/scope-acquire! scope
                            (fn [_] (swap! cleanup-log conj :first))
                            (fn [] (r/ok :resource-a))
                            [])
        (res/scope-acquire! scope
                            (fn [_] (swap! cleanup-log conj :second))
                            (fn [] (r/ok :resource-b))
                            [])
        (res/scope-acquire! scope
                            (fn [_] (swap! cleanup-log conj :third))
                            (fn [] (r/ok :resource-c))
                            []))
      ;; LIFO: third acquired = first released
      (is (= [:third :second :first] @cleanup-log)))))

(deftest with-scope-acquire-failure-test
  (testing "scope-acquire! returns error without adding to scope"
    (let [cleanup-log (atom [])
          scope (res/new-scope)
          result (res/scope-acquire! scope
                                     (fn [_] (swap! cleanup-log conj :released))
                                     (fn [] (r/err :resource/acquire-failed))
                                     [])]
      (is (r/err? result))
      (is (empty? @scope) "failed acquire should not add to scope")
      ;; Manually release to confirm nothing was registered
      (res/scope-release! scope)
      (is (empty? @cleanup-log)))))

(deftest with-scope-cleanup-errors-collected-test
  (testing "scope-release! collects cleanup errors but continues"
    (let [cleanup-log (atom [])
          scope (res/new-scope)]
      (res/scope-acquire! scope
                          (fn [_] (swap! cleanup-log conj :ok-release))
                          (fn [] (r/ok :a))
                          [])
      (res/scope-acquire! scope
                          (fn [_] (throw (Exception. "cleanup boom")))
                          (fn [] (r/ok :b))
                          [])
      (res/scope-acquire! scope
                          (fn [_] (swap! cleanup-log conj :ok-release-2))
                          (fn [] (r/ok :c))
                          [])
      (let [errors (res/scope-release! scope)]
        ;; All three should have been attempted (LIFO)
        (is (= [:ok-release-2 :ok-release] @cleanup-log))
        (is (= 1 (count errors)))
        (is (= "cleanup boom" (:error (first errors))))))))

(deftest with-scope-exception-in-body-test
  (testing "with-scope cleans up even when body throws"
    (let [cleanup-log (atom [])]
      (is (thrown? Exception
                   (res/with-scope [scope]
                     (res/scope-acquire! scope
                                         (fn [_] (swap! cleanup-log conj :cleaned))
                                         (fn [] (r/ok :resource))
                                         [])
                     (throw (Exception. "body failed")))))
      (is (= [:cleaned] @cleanup-log)))))
