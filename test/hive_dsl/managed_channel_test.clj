(ns hive-dsl.managed-channel-test
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.core.async :as async]
            [hive-dsl.lifecycle :as lc]
            [hive-dsl.managed-channel :as mc]))

;; =============================================================================
;; ManagedGoLoop lifecycle tests
;; =============================================================================

(deftest managed-go-loop-start-stop-test
  (testing "ManagedGoLoop start creates channel, stop closes it"
    (let [gl (mc/managed-go-loop (fn [_msg]) {:name "test-loop" :buf-size 10})]
      (is (lc/stopped? gl))
      (lc/start! gl)
      (is (lc/started? gl))
      (is (some? @(:channel gl)))
      (lc/stop! gl)
      (is (lc/stopped? gl))
      (is (nil? @(:channel gl))))))

(deftest managed-go-loop-double-start-throws-test
  (testing "start! throws when already started"
    (let [gl (mc/managed-go-loop (fn [_msg]) {:name "test-loop"})]
      (lc/start! gl)
      (try
        (is (thrown? clojure.lang.ExceptionInfo
                     (lc/start! gl)))
        (finally
          (lc/stop! gl))))))

(deftest managed-go-loop-double-stop-idempotent-test
  (testing "stop! is idempotent -- calling twice does not error"
    (let [gl (mc/managed-go-loop (fn [_msg]) {:name "test-loop"})]
      (lc/start! gl)
      (lc/stop! gl)
      (is (lc/stopped? gl))
      ;; Second stop should be a no-op
      (lc/stop! gl)
      (is (lc/stopped? gl)))))

(deftest managed-go-loop-stop-never-started-test
  (testing "stop! on never-started go-loop is a no-op"
    (let [gl (mc/managed-go-loop (fn [_msg]) {:name "test-loop"})]
      (is (lc/stopped? gl))
      (lc/stop! gl)
      (is (lc/stopped? gl)))))

;; =============================================================================
;; ManagedGoLoop message processing tests
;; =============================================================================

(deftest managed-go-loop-processes-messages-test
  (testing "go-loop processes messages sent via put!"
    (let [received (atom [])
          gl (mc/managed-go-loop
               (fn [msg] (swap! received conj msg))
               {:name "test-loop" :buf-size 10})]
      (lc/start! gl)
      (try
        (lc/put! gl :msg-1)
        (lc/put! gl :msg-2)
        (lc/put! gl :msg-3)
        ;; Give the go-loop time to process
        (Thread/sleep 100)
        (is (= [:msg-1 :msg-2 :msg-3] @received))
        (finally
          (lc/stop! gl))))))

(deftest managed-go-loop-stop-terminates-loop-test
  (testing "stop! closes channel causing go-loop to exit (not hang)"
    (let [loop-exited (promise)
          gl (mc/managed-go-loop
               (fn [_msg] nil)
               {:name "exit-test" :buf-size 10})]
      (lc/start! gl)
      ;; Watch the channel -- when it closes, the go-loop will read nil and exit
      (let [ch @(:channel gl)]
        ;; Start a watcher that checks channel closure
        (async/go
          (loop []
            (if (nil? (async/<! ch))
              (deliver loop-exited true)
              (recur)))))
      ;; Stop should close the channel
      (lc/stop! gl)
      ;; The loop should exit quickly, not hang
      (is (= true (deref loop-exited 2000 :timeout))
          "go-loop should terminate when channel is closed"))))

(deftest managed-go-loop-error-does-not-kill-loop-test
  (testing "error in handler doesn't kill the go-loop -- it continues processing"
    (let [received (atom [])
          error-log (atom [])
          gl (mc/managed-go-loop
               (fn [msg]
                 (if (= msg :bomb)
                   (throw (ex-info "handler exploded" {:msg msg}))
                   (swap! received conj msg)))
               {:name "error-test"
                :buf-size 10
                :on-error (fn [e msg] (swap! error-log conj {:error (.getMessage e) :msg msg}))})]
      (lc/start! gl)
      (try
        (lc/put! gl :before)
        (lc/put! gl :bomb)
        (lc/put! gl :after)
        (Thread/sleep 100)
        ;; :before and :after processed, :bomb caused error
        (is (= [:before :after] @received))
        (is (= 1 (count @error-log)))
        (is (= :bomb (:msg (first @error-log))))
        (finally
          (lc/stop! gl))))))

(deftest managed-go-loop-put-when-stopped-throws-test
  (testing "put! throws when go-loop is stopped"
    (let [gl (mc/managed-go-loop (fn [_msg]) {:name "test-loop"})]
      (is (thrown? clojure.lang.ExceptionInfo
                   (lc/put! gl :nope))))))

(deftest managed-go-loop-take-when-stopped-throws-test
  (testing "take! throws when go-loop is stopped"
    (let [gl (mc/managed-go-loop (fn [_msg]) {:name "test-loop"})]
      (is (thrown? clojure.lang.ExceptionInfo
                   (lc/take! gl))))))

;; =============================================================================
;; ManagedPubSub tests
;; =============================================================================

(deftest managed-pub-sub-start-stop-test
  (testing "ManagedPubSub start creates pub channel, stop closes it"
    (let [ps (mc/managed-pub-sub :type {:name "events" :buf-size 10})]
      (is (lc/stopped? ps))
      (lc/start! ps)
      (is (lc/started? ps))
      (is (some? @(:pub-channel ps)))
      (is (some? @(:pub ps)))
      (lc/stop! ps)
      (is (lc/stopped? ps))
      (is (nil? @(:pub-channel ps)))
      (is (nil? @(:pub ps))))))

(deftest managed-pub-sub-double-stop-idempotent-test
  (testing "stop! is idempotent"
    (let [ps (mc/managed-pub-sub :type {:name "events"})]
      (lc/start! ps)
      (lc/stop! ps)
      (is (lc/stopped? ps))
      (lc/stop! ps)
      (is (lc/stopped? ps)))))

(deftest managed-pub-sub-subscribe-publish-test
  (testing "publish routes messages to correct topic subscribers"
    (let [ps (mc/managed-pub-sub :type {:name "events" :buf-size 100})]
      (lc/start! ps)
      (try
        (let [foo-ch (mc/subscribe! ps :foo 10)
              bar-ch (mc/subscribe! ps :bar 10)]
          ;; Publish to :foo topic
          (mc/publish! ps {:type :foo :data 1})
          (mc/publish! ps {:type :foo :data 2})
          ;; Publish to :bar topic
          (mc/publish! ps {:type :bar :data 3})
          ;; Read from subscribers with timeout
          (let [foo1 (first (async/alts!! [foo-ch (async/timeout 1000)]))]
            (is (= {:type :foo :data 1} foo1)))
          (let [foo2 (first (async/alts!! [foo-ch (async/timeout 1000)]))]
            (is (= {:type :foo :data 2} foo2)))
          (let [bar1 (first (async/alts!! [bar-ch (async/timeout 1000)]))]
            (is (= {:type :bar :data 3} bar1))))
        (finally
          (lc/stop! ps))))))

(deftest managed-pub-sub-stop-closes-sub-channels-test
  (testing "stop! closes all subscriber channels"
    (let [ps (mc/managed-pub-sub :type {:name "events" :buf-size 10})]
      (lc/start! ps)
      (let [foo-ch (mc/subscribe! ps :foo 10)
            bar-ch (mc/subscribe! ps :bar 10)]
        (lc/stop! ps)
        ;; All sub channels should be closed (read nil)
        (is (nil? (async/<!! foo-ch)))
        (is (nil? (async/<!! bar-ch)))))))

(deftest managed-pub-sub-publish-when-stopped-throws-test
  (testing "publish! throws when pub/sub is stopped"
    (let [ps (mc/managed-pub-sub :type {:name "events"})]
      (is (thrown? clojure.lang.ExceptionInfo
                   (mc/publish! ps {:type :foo}))))))

(deftest managed-pub-sub-subscribe-when-stopped-throws-test
  (testing "subscribe! throws when pub/sub is stopped"
    (let [ps (mc/managed-pub-sub :type {:name "events"})]
      (is (thrown? clojure.lang.ExceptionInfo
                   (mc/subscribe! ps :foo))))))

;; =============================================================================
;; with-go-loop macro tests
;; =============================================================================

(deftest with-go-loop-auto-cleanup-test
  (testing "with-go-loop starts loop, runs body, auto-stops on exit"
    (let [loop-ref (atom nil)
          received (atom [])]
      (mc/with-go-loop [gl (fn [msg] (swap! received conj msg)) {:buf-size 10}]
        (reset! loop-ref gl)
        (is (lc/started? gl))
        (lc/put! gl :hello)
        (Thread/sleep 50))
      ;; After scope exit, loop should be stopped
      (is (lc/stopped? @loop-ref))
      (is (= [:hello] @received)))))

(deftest with-go-loop-stops-on-exception-test
  (testing "with-go-loop stops loop even when body throws"
    (let [loop-ref (atom nil)]
      (is (thrown? Exception
                   (mc/with-go-loop [gl (fn [_msg]) {:buf-size 10}]
                     (reset! loop-ref gl)
                     (throw (Exception. "boom")))))
      (is (lc/stopped? @loop-ref)))))

(deftest with-go-loop-returns-body-value-test
  (testing "with-go-loop returns the body's value"
    (let [result (mc/with-go-loop [gl (fn [_msg]) {:buf-size 10}]
                   :body-result)]
      (is (= :body-result result)))))

;; =============================================================================
;; fresh-channels! tests
;; =============================================================================

(deftest fresh-channels-creates-channels-test
  (testing "fresh-channels! creates a map of named channels"
    (let [chans (mc/fresh-channels! {:tx-chan {:buf 100}
                                     :ctrl-chan {:buf 1}})]
      (try
        (is (= #{:tx-chan :ctrl-chan} (set (keys chans))))
        (is (some? (:tx-chan chans)))
        (is (some? (:ctrl-chan chans)))
        ;; Channels should work
        (async/>!! (:tx-chan chans) :hello)
        (is (= :hello (async/<!! (:tx-chan chans))))
        (finally
          (mc/close-channels! chans))))))

(deftest fresh-channels-buf-size-test
  (testing "fresh-channels! uses specified buf size"
    (let [chans (mc/fresh-channels! {:ch {:buf 5}})]
      (try
        ;; Should be able to put 5 items without blocking
        (dotimes [i 5]
          (async/>!! (:ch chans) i))
        ;; Verify they come back
        (is (= 0 (async/<!! (:ch chans))))
        (finally
          (mc/close-channels! chans))))))

(deftest close-channels-closes-all-test
  (testing "close-channels! closes all channels in the map"
    (let [chans (mc/fresh-channels! {:a {:buf 10} :b {:buf 10}})]
      (mc/close-channels! chans)
      ;; Reads from closed channels return nil
      (is (nil? (async/<!! (:a chans))))
      (is (nil? (async/<!! (:b chans)))))))

(deftest close-channels-idempotent-test
  (testing "close-channels! is idempotent -- calling twice is safe"
    (let [chans (mc/fresh-channels! {:a {:buf 10}})]
      (mc/close-channels! chans)
      ;; Second close should not throw
      (mc/close-channels! chans)
      (is (nil? (async/<!! (:a chans)))))))

;; =============================================================================
;; Integration: lifecycle scope + managed-go-loop
;; =============================================================================

(deftest scope-managed-go-loop-integration-test
  (testing "managed-go-loop works with lifecycle scope for LIFO cleanup"
    (let [received (atom [])
          gl-ref (atom nil)]
      (lc/with-lifecycle-scope [scope]
        (let [gl (lc/scope-start! scope
                   (mc/managed-go-loop
                     (fn [msg] (swap! received conj msg))
                     {:name "scoped" :buf-size 10}))]
          (reset! gl-ref gl)
          (is (lc/started? gl))
          (lc/put! gl :scoped-msg)
          (Thread/sleep 50)))
      ;; After scope exit, go-loop should be stopped
      (is (lc/stopped? @gl-ref))
      (is (= [:scoped-msg] @received)))))

(deftest scope-managed-pub-sub-integration-test
  (testing "managed-pub-sub works with lifecycle scope"
    (let [ps-ref (atom nil)]
      (lc/with-lifecycle-scope [scope]
        (let [ps (lc/scope-start! scope
                   (mc/managed-pub-sub :type {:name "scoped-events" :buf-size 10}))]
          (reset! ps-ref ps)
          (is (lc/started? ps))
          (let [ch (mc/subscribe! ps :test 10)]
            (mc/publish! ps {:type :test :data 42})
            (let [[val _] (async/alts!! [ch (async/timeout 1000)])]
              (is (= {:type :test :data 42} val))))))
      ;; After scope exit, pub/sub should be stopped
      (is (lc/stopped? @ps-ref)))))
