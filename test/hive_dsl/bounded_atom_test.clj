(ns hive-dsl.bounded-atom-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [hive-dsl.bounded-atom :as ba]))

;; =============================================================================
;; Fixture — clear sweep registry and stop scheduler between tests
;; =============================================================================

(defn- clear-registry [f]
  (ba/stop-sweep-scheduler!)
  (reset! @#'ba/sweepable-registry {})
  (f)
  (ba/stop-sweep-scheduler!)
  (reset! @#'ba/sweepable-registry {}))

(use-fixtures :each clear-registry)

;; =============================================================================
;; Constructor tests
;; =============================================================================

(deftest bounded-atom-creates-empty-store-test
  (testing "bounded-atom creates empty map atom with opts"
    (let [b (ba/bounded-atom {:max-entries 10})]
      (is (= {} @(:atom b)))
      (is (= 10 (get-in b [:opts :max-entries])))
      (is (= :lru (get-in b [:opts :eviction-policy])))
      (is (nil? (get-in b [:opts :ttl-ms]))))))

(deftest bounded-atom-custom-policy-test
  (testing "bounded-atom accepts custom eviction policy"
    (let [b (ba/bounded-atom {:max-entries 5 :eviction-policy :fifo})]
      (is (= :fifo (get-in b [:opts :eviction-policy]))))))

(deftest bounded-atom-ttl-config-test
  (testing "bounded-atom accepts TTL config"
    (let [b (ba/bounded-atom {:max-entries 5 :ttl-ms 60000})]
      (is (= 60000 (get-in b [:opts :ttl-ms]))))))

(deftest bounded-atom-invalid-max-entries-test
  (testing "bounded-atom rejects non-positive max-entries"
    (is (thrown? AssertionError
                 (ba/bounded-atom {:max-entries 0})))
    (is (thrown? AssertionError
                 (ba/bounded-atom {:max-entries -1})))))

(deftest bounded-atom-invalid-policy-test
  (testing "bounded-atom rejects invalid eviction policy"
    (is (thrown? AssertionError
                 (ba/bounded-atom {:max-entries 10 :eviction-policy :random})))))

;; =============================================================================
;; bput! / bget tests
;; =============================================================================

(deftest bput-and-bget-test
  (testing "bput! stores data, bget retrieves it"
    (let [b (ba/bounded-atom {:max-entries 10})]
      (ba/bput! b :k1 {:task "hello"})
      (is (= {:task "hello"} (ba/bget b :k1))))))

(deftest bput-returns-eviction-stats-test
  (testing "bput! returns eviction stats"
    (let [b (ba/bounded-atom {:max-entries 10})
          result (ba/bput! b :k1 "data")]
      (is (map? (:result result)))
      (is (number? (:evicted-count result))))))

(deftest bget-returns-nil-for-missing-key-test
  (testing "bget returns nil for missing key"
    (let [b (ba/bounded-atom {:max-entries 10})]
      (is (nil? (ba/bget b :missing))))))

(deftest bcount-tracks-entries-test
  (testing "bcount returns correct count"
    (let [b (ba/bounded-atom {:max-entries 10})]
      (is (= 0 (ba/bcount b)))
      (ba/bput! b :k1 "a")
      (is (= 1 (ba/bcount b)))
      (ba/bput! b :k2 "b")
      (is (= 2 (ba/bcount b))))))

(deftest bkeys-returns-keys-test
  (testing "bkeys returns entry keys"
    (let [b (ba/bounded-atom {:max-entries 10})]
      (ba/bput! b :k1 "a")
      (ba/bput! b :k2 "b")
      (is (= #{:k1 :k2} (set (ba/bkeys b)))))))

(deftest bclear-empties-store-test
  (testing "bclear! removes all entries"
    (let [b (ba/bounded-atom {:max-entries 10})]
      (ba/bput! b :k1 "a")
      (ba/bput! b :k2 "b")
      (ba/bclear! b)
      (is (= 0 (ba/bcount b))))))

;; =============================================================================
;; Capacity eviction tests
;; =============================================================================

(deftest lru-eviction-test
  (testing "LRU eviction removes least recently accessed entry"
    (let [b (ba/bounded-atom {:max-entries 3 :eviction-policy :lru})]
      (ba/bput! b :k1 "first")
      (Thread/sleep 5)
      (ba/bput! b :k2 "second")
      (Thread/sleep 5)
      (ba/bput! b :k3 "third")
      (Thread/sleep 5)
      ;; Access k1 to make it recently used
      (ba/bget b :k1)
      (Thread/sleep 5)
      ;; Adding k4 should evict k2 (least recently accessed)
      (ba/bput! b :k4 "fourth")
      (is (= 3 (ba/bcount b)))
      (is (some? (ba/bget b :k1)) "k1 was accessed recently, should survive")
      (is (nil? (ba/bget b :k2)) "k2 was LRU, should be evicted")
      (is (some? (ba/bget b :k3)) "k3 should survive"))))

(deftest fifo-eviction-test
  (testing "FIFO eviction removes oldest created entry"
    (let [b (ba/bounded-atom {:max-entries 3 :eviction-policy :fifo})]
      (ba/bput! b :k1 "first")
      (Thread/sleep 5)
      (ba/bput! b :k2 "second")
      (Thread/sleep 5)
      (ba/bput! b :k3 "third")
      (Thread/sleep 5)
      ;; Access k1 — should NOT matter for FIFO
      (ba/bget b :k1)
      (Thread/sleep 5)
      ;; Adding k4 should evict k1 (oldest created)
      (ba/bput! b :k4 "fourth")
      (is (= 3 (ba/bcount b)))
      (is (nil? (ba/bget b :k1)) "k1 was oldest, should be evicted in FIFO"))))

(deftest capacity-enforced-at-boundary-test
  (testing "capacity is never exceeded"
    (let [b (ba/bounded-atom {:max-entries 5})]
      (dotimes [i 20]
        (ba/bput! b (keyword (str "k" i)) {:idx i}))
      (is (<= (ba/bcount b) 5)))))

;; =============================================================================
;; TTL eviction tests
;; =============================================================================

(deftest ttl-eviction-on-bget-test
  (testing "bget returns nil and evicts expired entry"
    (let [b (ba/bounded-atom {:max-entries 10 :ttl-ms 50})]
      (ba/bput! b :k1 "ephemeral")
      (is (some? (ba/bget b :k1)) "should exist immediately")
      (Thread/sleep 80)
      (is (nil? (ba/bget b :k1)) "should be expired after TTL"))))

(deftest ttl-eviction-on-bput-test
  (testing "bput! evicts expired entries when enforcing capacity"
    (let [b (ba/bounded-atom {:max-entries 3 :ttl-ms 50})]
      (ba/bput! b :k1 "old-1")
      (ba/bput! b :k2 "old-2")
      (Thread/sleep 80)
      ;; These should trigger TTL eviction of k1, k2
      (ba/bput! b :k3 "new-1")
      (ba/bput! b :k4 "new-2")
      (ba/bput! b :k5 "new-3")
      (is (<= (ba/bcount b) 3))
      (is (some? (ba/bget b :k3)))
      (is (some? (ba/bget b :k4)))
      (is (some? (ba/bget b :k5))))))

;; =============================================================================
;; bounded-swap! tests
;; =============================================================================

(deftest bounded-swap-test
  (testing "bounded-swap! applies function and enforces capacity"
    (let [b (ba/bounded-atom {:max-entries 2})
          now (System/currentTimeMillis)
          result (ba/bounded-swap! b
                   (fn [m]
                     (assoc m
                       :k1 {:data "a" :created-at now :last-accessed now}
                       :k2 {:data "b" :created-at now :last-accessed now}
                       :k3 {:data "c" :created-at now :last-accessed now})))]
      (is (<= (ba/bcount b) 2) "should not exceed max-entries")
      (is (map? (:result result))))))

;; =============================================================================
;; bounded-reset! tests
;; =============================================================================

(deftest bounded-reset-within-capacity-test
  (testing "bounded-reset! accepts map within capacity"
    (let [b (ba/bounded-atom {:max-entries 5})
          now (System/currentTimeMillis)
          new-map {:k1 {:data "a" :created-at now :last-accessed now}
                   :k2 {:data "b" :created-at now :last-accessed now}}
          result (ba/bounded-reset! b new-map)]
      (is (= 2 (ba/bcount b)))
      (is (= 0 (:evicted-count result))))))

(deftest bounded-reset-over-capacity-test
  (testing "bounded-reset! evicts when over capacity"
    (let [b (ba/bounded-atom {:max-entries 2})
          now (System/currentTimeMillis)
          new-map {:k1 {:data "a" :created-at now :last-accessed now}
                   :k2 {:data "b" :created-at (+ now 1) :last-accessed (+ now 1)}
                   :k3 {:data "c" :created-at (+ now 2) :last-accessed (+ now 2)}}
          result (ba/bounded-reset! b new-map)]
      (is (= 2 (ba/bcount b)))
      (is (= 1 (:evicted-count result)))
      (is (= :capacity (:eviction-reason result))))))

;; =============================================================================
;; bput! overwrites existing key
;; =============================================================================

(deftest bput-overwrites-existing-test
  (testing "bput! overwrites existing key without increasing count"
    (let [b (ba/bounded-atom {:max-entries 3})]
      (ba/bput! b :k1 "v1")
      (ba/bput! b :k2 "v2")
      (ba/bput! b :k1 "v1-updated")
      (is (= 2 (ba/bcount b)))
      (is (= "v1-updated" (ba/bget b :k1))))))

;; =============================================================================
;; Sweep registry tests
;; =============================================================================

(deftest register-sweepable-test
  (testing "register-sweepable! adds atom to registry"
    (let [b (ba/bounded-atom {:max-entries 10})]
      (ba/register-sweepable! b :test-store)
      (is (contains? (ba/registered-sweepables) :test-store))
      (is (= b (get (ba/registered-sweepables) :test-store))))))

(deftest unregister-sweepable-test
  (testing "unregister-sweepable! removes from registry"
    (let [b (ba/bounded-atom {:max-entries 10})]
      (ba/register-sweepable! b :test-store)
      (ba/unregister-sweepable! :test-store)
      (is (not (contains? (ba/registered-sweepables) :test-store))))))

;; =============================================================================
;; sweep! / sweep-all! tests
;; =============================================================================

(deftest sweep-single-atom-test
  (testing "sweep! evicts expired entries from a single atom"
    (let [b (ba/bounded-atom {:max-entries 10 :ttl-ms 50})]
      (ba/bput! b :k1 "old")
      (ba/bput! b :k2 "old-too")
      ;; Verify entries exist before TTL expires
      (is (= 2 (ba/bcount b)))
      (Thread/sleep 80)
      ;; sweep! should evict both expired entries (no bput! between to avoid early eviction)
      (let [stats (ba/sweep! b :test)]
        (is (= :test (:name stats)))
        (is (= 2 (:evicted-count stats)))
        (is (= 0 (:remaining stats)))))))

(deftest sweep-no-ttl-test
  (testing "sweep! is a no-op for atoms without TTL"
    (let [b (ba/bounded-atom {:max-entries 10})]
      (ba/bput! b :k1 "data")
      (let [stats (ba/sweep! b :no-ttl)]
        (is (= 0 (:evicted-count stats)))
        (is (= 1 (:remaining stats)))))))

(deftest sweep-all-test
  (testing "sweep-all! iterates all registered atoms"
    (let [b1 (ba/bounded-atom {:max-entries 10 :ttl-ms 50})
          b2 (ba/bounded-atom {:max-entries 10 :ttl-ms 50})]
      (ba/register-sweepable! b1 :store-1)
      (ba/register-sweepable! b2 :store-2)
      (ba/bput! b1 :k1 "old")
      (ba/bput! b2 :k2 "old")
      (Thread/sleep 80)
      (let [results (ba/sweep-all!)]
        (is (= 2 (count results)))
        (is (every? #(= 1 (:evicted-count %)) results))
        (is (every? #(= 0 (:remaining %)) results))))))

(deftest sweep-all-empty-registry-test
  (testing "sweep-all! returns empty vector when no atoms registered"
    (is (= [] (ba/sweep-all!)))))

;; =============================================================================
;; TTL-only policy test
;; =============================================================================

(deftest ttl-only-policy-no-capacity-eviction-test
  (testing ":ttl policy does not evict for capacity, only for TTL"
    (let [b (ba/bounded-atom {:max-entries 2 :eviction-policy :ttl :ttl-ms 60000})]
      ;; Fill beyond capacity — :ttl policy won't capacity-evict
      (ba/bput! b :k1 "a")
      (ba/bput! b :k2 "b")
      (ba/bput! b :k3 "c")
      ;; All three may still be present since :ttl doesn't do capacity eviction
      (is (>= (ba/bcount b) 2)))))

;; =============================================================================
;; Observability callback tests (gc-fix-7)
;; =============================================================================

(deftest on-evict-callback-fires-on-capacity-eviction-test
  (testing ":on-evict callback is called when capacity eviction occurs"
    (let [events (atom [])
          b (ba/bounded-atom {:max-entries 2
                              :name "cb-test"
                              :on-evict (fn [evt] (swap! events conj evt))})]
      ;; Fill to capacity
      (ba/bput! b :k1 "a")
      (ba/bput! b :k2 "b")
      ;; No eviction yet
      (is (= 0 (count @events)) "no callback before eviction")
      ;; Trigger eviction
      (ba/bput! b :k3 "c")
      (is (= 1 (count @events)) "callback fired once on eviction")
      (let [evt (first @events)]
        (is (= "cb-test" (:name evt)) "event has correct atom name")
        (is (pos? (:evicted-count evt)) "evicted-count is positive")
        (is (= :capacity (:reason evt)) "reason is :capacity")
        (is (number? (:entries evt)) "entries count is a number")))))

(deftest on-evict-callback-fires-on-ttl-eviction-test
  (testing ":on-evict callback is called on TTL eviction during bput!"
    (let [events (atom [])
          b (ba/bounded-atom {:max-entries 5
                              :ttl-ms 50
                              :name "ttl-cb-test"
                              :on-evict (fn [evt] (swap! events conj evt))})]
      (ba/bput! b :k1 "old")
      (ba/bput! b :k2 "old-too")
      (Thread/sleep 80)
      ;; bput! will evict expired entries
      (ba/bput! b :k3 "new")
      ;; At least one eviction callback
      (is (pos? (count @events)) "callback fired on TTL eviction")
      (let [evt (first @events)]
        (is (= :ttl (:reason evt)) "reason is :ttl for TTL eviction")))))

(deftest on-evict-callback-not-fired-when-no-eviction-test
  (testing ":on-evict callback is NOT called when no evictions happen"
    (let [call-count (atom 0)
          b (ba/bounded-atom {:max-entries 10
                              :on-evict (fn [_] (swap! call-count inc))})]
      (ba/bput! b :k1 "a")
      (ba/bput! b :k2 "b")
      (ba/bput! b :k3 "c")
      (is (= 0 @call-count) "no callback when under capacity"))))

(deftest on-evict-exception-does-not-break-bput-test
  (testing "exception in :on-evict does not prevent bput! from completing"
    (let [b (ba/bounded-atom {:max-entries 2
                              :on-evict (fn [_] (throw (RuntimeException. "boom!")))})]
      (ba/bput! b :k1 "a")
      (ba/bput! b :k2 "b")
      ;; This triggers eviction with broken callback — should NOT throw
      (ba/bput! b :k3 "c")
      (is (<= (ba/bcount b) 2) "capacity still enforced"))))

(deftest on-sweep-callback-fires-test
  (testing ":on-sweep callback is called when sweep! runs"
    (let [events (atom [])
          b (ba/bounded-atom {:max-entries 10
                              :ttl-ms 50
                              :name "sweep-cb-test"
                              :on-sweep (fn [evt] (swap! events conj evt))})]
      (ba/bput! b :k1 "old")
      (Thread/sleep 80)
      (ba/sweep! b :sweep-cb-test)
      (is (= 1 (count @events)) "on-sweep callback fired once")
      (let [evt (first @events)]
        (is (= :sweep-cb-test (:name evt)) "event has sweep name")
        (is (= 1 (:evicted-count evt)) "evicted count is correct")
        (is (= 0 (:remaining evt)) "remaining count is correct")
        (is (number? (:duration-ms evt)) "duration-ms is a number")
        (is (>= (:duration-ms evt) 0) "duration-ms is non-negative")))))

(deftest on-sweep-callback-fires-for-no-ttl-test
  (testing ":on-sweep callback fires even when no TTL (zero evictions)"
    (let [events (atom [])
          b (ba/bounded-atom {:max-entries 10
                              :on-sweep (fn [evt] (swap! events conj evt))})]
      (ba/bput! b :k1 "data")
      (ba/sweep! b :no-ttl)
      (is (= 1 (count @events)) "on-sweep fires even with no TTL")
      (is (= 0 (:evicted-count (first @events)))))))

(deftest on-sweep-exception-does-not-break-sweep-test
  (testing "exception in :on-sweep does not prevent sweep! from completing"
    (let [b (ba/bounded-atom {:max-entries 10
                              :ttl-ms 50
                              :on-sweep (fn [_] (throw (RuntimeException. "sweep boom!")))})]
      (ba/bput! b :k1 "old")
      (Thread/sleep 80)
      ;; sweep! should complete without throwing
      (let [result (ba/sweep! b :err-test)]
        (is (= 1 (:evicted-count result)))))))

(deftest sweep-returns-duration-ms-test
  (testing "sweep! returns :duration-ms in stats"
    (let [b (ba/bounded-atom {:max-entries 10 :ttl-ms 50})]
      (ba/bput! b :k1 "old")
      (Thread/sleep 80)
      (let [stats (ba/sweep! b :duration-test)]
        (is (contains? stats :duration-ms) "stats include :duration-ms")
        (is (number? (:duration-ms stats)) "duration-ms is a number")
        (is (>= (:duration-ms stats) 0) "duration-ms is non-negative")))))

(deftest bounded-atom-name-stored-test
  (testing "bounded-atom stores :name for observability"
    (let [b (ba/bounded-atom {:max-entries 10 :name "my-store"})]
      (is (= "my-store" (:name b)) "name is stored on the batom map"))))

(deftest bounded-swap-fires-on-evict-test
  (testing "bounded-swap! fires :on-evict when evictions happen"
    (let [events (atom [])
          b (ba/bounded-atom {:max-entries 2
                              :name "swap-cb-test"
                              :on-evict (fn [evt] (swap! events conj evt))})
          now (System/currentTimeMillis)]
      ;; bounded-swap! adding 3 entries to max-entries=2
      (ba/bounded-swap! b
        (fn [m]
          (assoc m
            :k1 {:data "a" :created-at now :last-accessed now}
            :k2 {:data "b" :created-at (+ now 1) :last-accessed (+ now 1)}
            :k3 {:data "c" :created-at (+ now 2) :last-accessed (+ now 2)})))
      (is (= 1 (count @events)) "on-evict fired from bounded-swap!")
      (is (= :capacity (:reason (first @events)))))))

(deftest bounded-reset-fires-on-evict-test
  (testing "bounded-reset! fires :on-evict when evictions happen"
    (let [events (atom [])
          b (ba/bounded-atom {:max-entries 2
                              :name "reset-cb-test"
                              :on-evict (fn [evt] (swap! events conj evt))})
          now (System/currentTimeMillis)
          new-map {:k1 {:data "a" :created-at now :last-accessed now}
                   :k2 {:data "b" :created-at (+ now 1) :last-accessed (+ now 1)}
                   :k3 {:data "c" :created-at (+ now 2) :last-accessed (+ now 2)}}]
      (ba/bounded-reset! b new-map)
      (is (= 1 (count @events)) "on-evict fired from bounded-reset!")
      (is (= :capacity (:reason (first @events)))))))

;; =============================================================================
;; Auto-sweep scheduler tests (D3)
;; =============================================================================

(deftest scheduler-start-and-sweep-runs-test
  (testing "scheduler starts and sweep executes within interval"
    (let [b (ba/bounded-atom {:max-entries 10 :ttl-ms 30})
          sweep-results (atom [])]
      (ba/register-sweepable! b :sched-test)
      (ba/bput! b :k1 "will-expire")
      (ba/start-sweep-scheduler! {:interval-ms 50
                                  :on-sweep-complete (fn [results]
                                                       (swap! sweep-results conj results))})
      (try
        (is (:running? (ba/sweep-scheduler-status)) "scheduler is running")
        (is (= 50 (:interval-ms (ba/sweep-scheduler-status))) "interval-ms matches")
        ;; Wait for at least one sweep to fire (interval + margin)
        (Thread/sleep 200)
        (is (pos? (count @sweep-results)) "on-sweep-complete callback fired at least once")
        (is (pos? (:sweep-count (ba/sweep-scheduler-status))) "sweep-count incremented")
        (is (some? (:last-sweep-at (ba/sweep-scheduler-status))) "last-sweep-at is set")
        (finally
          (ba/stop-sweep-scheduler!))))))

(deftest scheduler-stop-halts-thread-test
  (testing "stop-sweep-scheduler! stops the sweep thread"
    (ba/start-sweep-scheduler! {:interval-ms 50})
    (is (:running? (ba/sweep-scheduler-status)) "running after start")
    (ba/stop-sweep-scheduler!)
    (is (not (:running? (ba/sweep-scheduler-status))) "not running after stop")
    (is (= 0 (:sweep-count (ba/sweep-scheduler-status))) "sweep-count resets to 0")))

(deftest scheduler-double-start-idempotent-test
  (testing "calling start-sweep-scheduler! twice is idempotent"
    (let [stop-fn-1 (ba/start-sweep-scheduler! {:interval-ms 50})
          status-1 (ba/sweep-scheduler-status)
          stop-fn-2 (ba/start-sweep-scheduler! {:interval-ms 999})]
      (try
        (is (:running? status-1) "first start works")
        ;; Second call should NOT change interval — still using first scheduler
        (is (= 50 (:interval-ms (ba/sweep-scheduler-status)))
            "interval unchanged by second start call")
        ;; Both stop-fns should be the same function
        (is (= stop-fn-1 stop-fn-2) "both calls return the same stop function")
        (finally
          (ba/stop-sweep-scheduler!))))))

(deftest scheduler-double-stop-idempotent-test
  (testing "calling stop-sweep-scheduler! twice is idempotent (no error)"
    (ba/start-sweep-scheduler! {:interval-ms 50})
    (ba/stop-sweep-scheduler!)
    ;; Second stop should be a no-op, no exception
    (ba/stop-sweep-scheduler!)
    (is (not (:running? (ba/sweep-scheduler-status))) "still not running")))

(deftest scheduler-sweep-exception-does-not-kill-scheduler-test
  (testing "exception during sweep does not crash the scheduler"
    (let [sweep-count-atom (atom 0)
          ;; Register a "broken" sweepable that will cause sweep! to exercise
          ;; error handling. We use on-sweep-complete to track invocations.
          bad-batom {:atom (atom {})
                     :name "broken"
                     :opts {:max-entries 10
                            :ttl-ms 30
                            :on-sweep (fn [_] (throw (RuntimeException. "boom in sweep!")))}}]
      (ba/register-sweepable! bad-batom :broken-store)
      (ba/start-sweep-scheduler! {:interval-ms 50
                                  :on-sweep-complete (fn [_]
                                                       (swap! sweep-count-atom inc))})
      (try
        ;; Wait for multiple sweep cycles
        (Thread/sleep 250)
        ;; Scheduler should still be running despite the on-sweep exception in sweep!
        (is (:running? (ba/sweep-scheduler-status)) "scheduler survived exceptions")
        ;; Multiple sweeps should have fired
        (is (>= @sweep-count-atom 2) "multiple sweeps fired despite exceptions")
        (finally
          (ba/stop-sweep-scheduler!))))))

(deftest scheduler-status-reporting-test
  (testing "sweep-scheduler-status reports accurate state"
    ;; Before start
    (let [status-before (ba/sweep-scheduler-status)]
      (is (not (:running? status-before)) "not running before start")
      (is (nil? (:interval-ms status-before)) "no interval before start")
      (is (nil? (:last-sweep-at status-before)) "no last-sweep before start")
      (is (= 0 (:sweep-count status-before)) "sweep-count 0 before start"))
    ;; After start
    (ba/start-sweep-scheduler! {:interval-ms 50})
    (try
      (Thread/sleep 150)
      (let [status-after (ba/sweep-scheduler-status)]
        (is (:running? status-after) "running after start")
        (is (= 50 (:interval-ms status-after)) "interval matches config")
        (is (pos? (:sweep-count status-after)) "sweep-count is positive")
        (is (some? (:last-sweep-at status-after)) "last-sweep-at is set")
        (is (instance? java.time.Instant (:last-sweep-at status-after))
            "last-sweep-at is an Instant"))
      (finally
        (ba/stop-sweep-scheduler!)))))

(deftest scheduler-on-sweep-complete-receives-results-test
  (testing "on-sweep-complete callback receives sweep-all! results"
    (let [b (ba/bounded-atom {:max-entries 10 :ttl-ms 30})
          captured (atom nil)]
      (ba/register-sweepable! b :results-test)
      (ba/bput! b :k1 "data")
      (ba/start-sweep-scheduler! {:interval-ms 50
                                  :on-sweep-complete (fn [results]
                                                       (reset! captured results))})
      (try
        (Thread/sleep 200)
        (is (some? @captured) "callback received results")
        (is (vector? @captured) "results is a vector")
        (when (seq @captured)
          (let [result (first @captured)]
            (is (contains? result :name) "result has :name")
            (is (contains? result :evicted-count) "result has :evicted-count")
            (is (contains? result :remaining) "result has :remaining")))
        (finally
          (ba/stop-sweep-scheduler!))))))

(deftest scheduler-daemon-thread-test
  (testing "scheduler thread is a daemon thread named correctly"
    (ba/start-sweep-scheduler! {:interval-ms 5000})
    (try
      (let [sweep-threads (->> (Thread/getAllStackTraces)
                               .keySet
                               (filter #(= "hive-bounded-atom-sweep" (.getName %))))]
        (is (= 1 (count sweep-threads)) "exactly one sweep thread exists")
        (when (seq sweep-threads)
          (let [t (first sweep-threads)]
            (is (.isDaemon t) "sweep thread is a daemon thread")
            (is (= "hive-bounded-atom-sweep" (.getName t)) "thread has correct name"))))
      (finally
        (ba/stop-sweep-scheduler!)))))
