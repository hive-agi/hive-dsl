(ns hive-dsl.lifecycle-test
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.core.async :as async]
            [hive-dsl.lifecycle :as lc]))

;; =============================================================================
;; Lifecycle protocol + state helpers
;; =============================================================================

(deftest lifecycle-state-helpers-test
  (testing "lifecycle-state reads atom-backed state"
    (let [exec (lc/->managed-executor 2)]
      (is (= :stopped (lc/lifecycle-state exec)))
      (is (lc/stopped? exec))
      (is (not (lc/started? exec))))))

(deftest state-transitions-test
  (testing "stopped -> started -> stopped roundtrip"
    (let [exec (lc/->managed-executor 2)]
      (is (lc/stopped? exec))
      (lc/start! exec)
      (is (lc/started? exec))
      (is (not (lc/stopped? exec)))
      (lc/stop! exec)
      (is (lc/stopped? exec))
      (is (not (lc/started? exec))))))

;; =============================================================================
;; ManagedExecutor tests
;; =============================================================================

(deftest managed-executor-start-stop-test
  (testing "ManagedExecutor start creates pool, stop shuts it down"
    (let [exec (lc/->managed-executor 2)]
      (lc/start! exec)
      (is (lc/started? exec))
      (is (some? @(:executor exec)))
      (lc/stop! exec)
      (is (lc/stopped? exec))
      (is (nil? @(:executor exec))))))

(deftest managed-executor-submit-test
  (testing "submit! executes a fn and returns a Future"
    (let [exec (lc/->managed-executor 2)
          _ (lc/start! exec)
          result (promise)]
      (try
        (let [fut (lc/submit! exec (fn [] (deliver result 42)))]
          (.get fut)
          (is (= 42 @result)))
        (finally
          (lc/stop! exec))))))

(deftest managed-executor-schedule-test
  (testing "schedule! runs fn after a delay"
    (let [exec (lc/->managed-executor 2)
          _ (lc/start! exec)
          result (promise)]
      (try
        (lc/schedule! exec (fn [] (deliver result :scheduled)) 10)
        (is (= :scheduled (deref result 2000 :timeout)))
        (finally
          (lc/stop! exec))))))

(deftest managed-executor-schedule-at-fixed-rate-test
  (testing "schedule-at-fixed-rate! runs fn repeatedly"
    (let [exec (lc/->managed-executor 2)
          _ (lc/start! exec)
          counter (atom 0)]
      (try
        (let [fut (lc/schedule-at-fixed-rate! exec
                                              (fn [] (swap! counter inc))
                                              0 20)]
          (Thread/sleep 100)
          (.cancel fut true)
          (is (>= @counter 2) "should have run at least twice"))
        (finally
          (lc/stop! exec))))))

(deftest managed-executor-submit-when-stopped-test
  (testing "submit! throws when executor is stopped"
    (let [exec (lc/->managed-executor 2)]
      (is (thrown? clojure.lang.ExceptionInfo
                   (lc/submit! exec (fn [] :nope)))))))

(deftest managed-executor-double-start-throws-test
  (testing "start! throws when already started"
    (let [exec (lc/->managed-executor 2)]
      (lc/start! exec)
      (try
        (is (thrown? clojure.lang.ExceptionInfo
                     (lc/start! exec)))
        (finally
          (lc/stop! exec))))))

(deftest managed-executor-double-stop-idempotent-test
  (testing "stop! is idempotent — calling twice does not error"
    (let [exec (lc/->managed-executor 2)]
      (lc/start! exec)
      (lc/stop! exec)
      (is (lc/stopped? exec))
      ;; Second stop should be a no-op, no exception
      (lc/stop! exec)
      (is (lc/stopped? exec)))))

(deftest managed-executor-stop-kills-threads-test
  (testing "stop! kills running tasks"
    (let [exec (lc/->managed-executor 2)
          _ (lc/start! exec)
          interrupted (promise)]
      ;; Submit a long-running task
      (lc/submit! exec (fn []
                         (try
                           (Thread/sleep 30000)
                           (catch InterruptedException _
                             (deliver interrupted true)))))
      (Thread/sleep 50) ;; let task start
      (lc/stop! exec)
      (is (= true (deref interrupted 3000 :timeout))
          "running task should be interrupted on stop"))))

;; =============================================================================
;; ManagedChannel tests
;; =============================================================================

(deftest managed-channel-start-stop-test
  (testing "ManagedChannel start creates channel, stop closes it"
    (let [ch (lc/->managed-channel 10)]
      (lc/start! ch)
      (is (lc/started? ch))
      (is (some? @(:channel ch)))
      (lc/stop! ch)
      (is (lc/stopped? ch))
      (is (nil? @(:channel ch))))))

(deftest managed-channel-put-take-test
  (testing "put! and take! work for basic messaging"
    (let [ch (lc/->managed-channel 10)]
      (lc/start! ch)
      (try
        (lc/put! ch :hello)
        (lc/put! ch :world)
        (is (= :hello (lc/take! ch)))
        (is (= :world (lc/take! ch)))
        (finally
          (lc/stop! ch))))))

(deftest managed-channel-put-when-stopped-test
  (testing "put! throws when channel is stopped"
    (let [ch (lc/->managed-channel 10)]
      (is (thrown? clojure.lang.ExceptionInfo
                   (lc/put! ch :nope))))))

(deftest managed-channel-take-when-stopped-test
  (testing "take! throws when channel is stopped"
    (let [ch (lc/->managed-channel 10)]
      (is (thrown? clojure.lang.ExceptionInfo
                   (lc/take! ch))))))

(deftest managed-channel-double-start-throws-test
  (testing "start! throws when already started"
    (let [ch (lc/->managed-channel 10)]
      (lc/start! ch)
      (try
        (is (thrown? clojure.lang.ExceptionInfo
                     (lc/start! ch)))
        (finally
          (lc/stop! ch))))))

(deftest managed-channel-double-stop-idempotent-test
  (testing "stop! is idempotent — calling twice does not error"
    (let [ch (lc/->managed-channel 10)]
      (lc/start! ch)
      (lc/stop! ch)
      (is (lc/stopped? ch))
      ;; Second stop should be a no-op, no exception
      (lc/stop! ch)
      (is (lc/stopped? ch)))))

(deftest managed-channel-stop-returns-nil-on-take-test
  (testing "take from underlying channel returns nil after stop (closed)"
    (let [ch (lc/->managed-channel 10)]
      (lc/start! ch)
      (lc/put! ch :before-close)
      (let [raw-ch @(:channel ch)]
        (lc/stop! ch)
        ;; After close, buffered items drain, then nil
        (is (= :before-close (async/<!! raw-ch)))
        (is (nil? (async/<!! raw-ch)))))))

;; =============================================================================
;; with-lifecycle macro tests
;; =============================================================================

(deftest with-lifecycle-normal-exit-test
  (testing "with-lifecycle starts resource, runs body, stops on exit"
    (let [exec-ref (atom nil)
          result (lc/with-lifecycle [exec (lc/->managed-executor 2)]
                   (reset! exec-ref exec)
                   (is (lc/started? exec))
                   :body-result)]
      (is (= :body-result result))
      (is (lc/stopped? @exec-ref)))))

(deftest with-lifecycle-auto-stops-on-exception-test
  (testing "with-lifecycle stops resource even when body throws"
    (let [exec-ref (atom nil)]
      (is (thrown? Exception
                   (lc/with-lifecycle [exec (lc/->managed-executor 2)]
                     (reset! exec-ref exec)
                     (throw (Exception. "boom")))))
      (is (lc/stopped? @exec-ref)))))

(deftest with-lifecycle-returns-body-value-test
  (testing "with-lifecycle returns the body's value"
    (let [result (lc/with-lifecycle [ch (lc/->managed-channel 10)]
                   (lc/put! ch :msg)
                   (lc/take! ch))]
      (is (= :msg result)))))

(deftest with-lifecycle-stop-error-does-not-mask-exception-test
  (testing "stop! error during cleanup does not mask the original exception"
    ;; Create a resource whose stop! will throw
    (let [bad-resource (reify lc/Lifecycle
                         (start! [this] this)
                         (stop! [_this]
                           (throw (Exception. "stop failed"))))]
      ;; The body exception should propagate, not be masked by stop! failure
      (is (thrown-with-msg?
            Exception #"body error"
            (lc/with-lifecycle [r bad-resource]
              (throw (Exception. "body error"))))))))

;; =============================================================================
;; with-lifecycle-scope macro tests
;; =============================================================================

(deftest with-lifecycle-scope-basic-test
  (testing "with-lifecycle-scope starts and stops resources"
    (let [exec-ref (atom nil)
          ch-ref (atom nil)
          result (lc/with-lifecycle-scope [scope]
                   (let [exec (lc/scope-start! scope (lc/->managed-executor 2))
                         ch   (lc/scope-start! scope (lc/->managed-channel 10))]
                     (reset! exec-ref exec)
                     (reset! ch-ref ch)
                     (is (lc/started? exec))
                     (is (lc/started? ch))
                     :scope-result))]
      (is (= :scope-result result))
      (is (lc/stopped? @exec-ref))
      (is (lc/stopped? @ch-ref)))))

(deftest with-lifecycle-scope-lifo-order-test
  (testing "with-lifecycle-scope stops resources in LIFO order"
    (let [stop-log (atom [])]
      (lc/with-lifecycle-scope [scope]
        ;; Use reified resources that log stop order
        (lc/scope-start! scope
                         (reify lc/Lifecycle
                           (start! [this] this)
                           (stop! [_this] (swap! stop-log conj :first))))
        (lc/scope-start! scope
                         (reify lc/Lifecycle
                           (start! [this] this)
                           (stop! [_this] (swap! stop-log conj :second))))
        (lc/scope-start! scope
                         (reify lc/Lifecycle
                           (start! [this] this)
                           (stop! [_this] (swap! stop-log conj :third)))))
      ;; LIFO: third started = first stopped
      (is (= [:third :second :first] @stop-log)))))

(deftest with-lifecycle-scope-cleanup-on-exception-test
  (testing "with-lifecycle-scope cleans up even when body throws"
    (let [exec-ref (atom nil)]
      (is (thrown? Exception
                   (lc/with-lifecycle-scope [scope]
                     (let [exec (lc/scope-start! scope (lc/->managed-executor 2))]
                       (reset! exec-ref exec)
                       (throw (Exception. "scope body failed"))))))
      (is (lc/stopped? @exec-ref)))))

(deftest with-lifecycle-scope-stop-error-collected-test
  (testing "scope-stop! collects errors but continues cleanup"
    (let [stop-log (atom [])
          scope (lc/new-lifecycle-scope)]
      ;; First resource — stops ok
      (lc/scope-start! scope
                       (reify lc/Lifecycle
                         (start! [this] this)
                         (stop! [_this] (swap! stop-log conj :ok-1))))
      ;; Second resource — stop throws
      (lc/scope-start! scope
                       (reify lc/Lifecycle
                         (start! [this] this)
                         (stop! [_this]
                           (swap! stop-log conj :error-stop)
                           (throw (Exception. "stop boom")))))
      ;; Third resource — stops ok
      (lc/scope-start! scope
                       (reify lc/Lifecycle
                         (start! [this] this)
                         (stop! [_this] (swap! stop-log conj :ok-3))))
      (let [errors (lc/scope-stop! scope)]
        ;; All three attempted in LIFO order
        (is (= [:ok-3 :error-stop :ok-1] @stop-log))
        (is (= 1 (count errors)))
        (is (= "stop boom" (:error (first errors))))))))

;; =============================================================================
;; Integration: executor + channel together in a scope
;; =============================================================================

(deftest scope-executor-and-channel-integration-test
  (testing "executor and channel work together inside a scope"
    (let [result (promise)]
      (lc/with-lifecycle-scope [scope]
        (let [exec (lc/scope-start! scope (lc/->managed-executor 2))
              ch   (lc/scope-start! scope (lc/->managed-channel 10))]
          ;; Submit a task that puts a value on the channel
          (lc/submit! exec (fn [] (lc/put! ch :from-executor)))
          ;; Take the value
          (deliver result (lc/take! ch))))
      (is (= :from-executor @result)))))

;; =============================================================================
;; Edge cases
;; =============================================================================

(deftest stop-unstarted-executor-is-noop-test
  (testing "stopping an executor that was never started is a no-op"
    (let [exec (lc/->managed-executor 2)]
      (is (lc/stopped? exec))
      ;; Stop without start should be idempotent no-op
      (lc/stop! exec)
      (is (lc/stopped? exec)))))

(deftest stop-unstarted-channel-is-noop-test
  (testing "stopping a channel that was never started is a no-op"
    (let [ch (lc/->managed-channel 10)]
      (is (lc/stopped? ch))
      ;; Stop without start should be idempotent no-op
      (lc/stop! ch)
      (is (lc/stopped? ch)))))
