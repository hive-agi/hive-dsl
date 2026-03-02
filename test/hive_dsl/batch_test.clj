(ns hive-dsl.batch-test
  (:require [clojure.test :refer [deftest testing is]]
            [hive-dsl.result :as r]
            [hive-dsl.batch :as batch]))

;; =============================================================================
;; tx-batch / batch-add! / batch-flush! unit tests
;; =============================================================================

(deftest tx-batch-creates-empty-batch-test
  (testing "tx-batch creates batch with empty tx-data"
    (let [b (batch/tx-batch :conn (fn [_ _]))]
      (is (= :conn (:conn b)))
      (is (= [] @(:tx-data b)))
      (is (fn? (:flush-fn b))))))

(deftest batch-add-single-map-test
  (testing "batch-add! accumulates map tx-data"
    (let [b (batch/tx-batch :conn (fn [_ _]))]
      (batch/batch-add! b {:kg-edge/from "a" :kg-edge/to "b"})
      (batch/batch-add! b {:kg-edge/from "c" :kg-edge/to "d"})
      (is (= [{:kg-edge/from "a" :kg-edge/to "b"}
              {:kg-edge/from "c" :kg-edge/to "d"}]
             @(:tx-data b))))))

(deftest batch-add-vector-tx-datum-test
  (testing "batch-add! handles vector tx-data (e.g. [:db/add ...])"
    (let [b (batch/tx-batch :conn (fn [_ _]))]
      (batch/batch-add! b [:db/add 1 :attr "val"])
      (is (= [[:db/add 1 :attr "val"]] @(:tx-data b))))))

(deftest batch-add-collection-test
  (testing "batch-add! handles collection of tx-data"
    (let [b (batch/tx-batch :conn (fn [_ _]))]
      (batch/batch-add! b [{:id 1} {:id 2}])
      (is (= [{:id 1} {:id 2}] @(:tx-data b))))))

(deftest batch-flush-success-test
  (testing "batch-flush! calls flush-fn with accumulated data"
    (let [captured (atom nil)
          b (batch/tx-batch :test-conn
                            (fn [conn data]
                              (reset! captured {:conn conn :data data})
                              {:tx-id 42}))]
      (batch/batch-add! b {:a 1})
      (batch/batch-add! b {:b 2})
      (let [result (batch/batch-flush! b)]
        (is (r/ok? result))
        (is (= {:tx-id 42} (:ok result)))
        (is (= :test-conn (:conn @captured)))
        (is (= [{:a 1} {:b 2}] (:data @captured)))
        ;; tx-data should be cleared after flush
        (is (empty? @(:tx-data b)))))))

(deftest batch-flush-empty-test
  (testing "batch-flush! returns :batch/empty when no data"
    (let [b (batch/tx-batch :conn (fn [_ _]))]
      (let [result (batch/batch-flush! b)]
        (is (r/err? result))
        (is (= :batch/empty (:error result)))))))

(deftest batch-flush-exception-test
  (testing "batch-flush! catches exceptions and returns err"
    (let [b (batch/tx-batch :conn
                            (fn [_ _] (throw (Exception. "tx failed"))))]
      (batch/batch-add! b {:a 1})
      (let [result (batch/batch-flush! b)]
        (is (r/err? result))
        (is (= :batch/flush-failed (:error result)))
        (is (= "tx failed" (:message result)))))))

(deftest batch-count-test
  (testing "batch-count tracks accumulated items"
    (let [b (batch/tx-batch :conn (fn [_ _]))]
      (is (= 0 (batch/batch-count b)))
      (batch/batch-add! b {:a 1})
      (is (= 1 (batch/batch-count b)))
      (batch/batch-add! b [{:b 2} {:c 3}])
      (is (= 3 (batch/batch-count b))))))

;; =============================================================================
;; with-batch macro tests
;; =============================================================================

(deftest with-batch-single-flush-test
  (testing "with-batch flushes all accumulated data as single transaction"
    (let [flush-calls (atom 0)
          captured-data (atom nil)
          result (batch/with-batch [b (batch/tx-batch :conn
                                                      (fn [_ data]
                                                        (swap! flush-calls inc)
                                                        (reset! captured-data data)
                                                        {:tx-id 1}))]
                   (batch/batch-add! b {:edge 1})
                   (batch/batch-add! b {:edge 2})
                   (batch/batch-add! b {:edge 3}))]
      (is (r/ok? result))
      (is (= 1 @flush-calls) "flush should be called exactly once")
      (is (= [{:edge 1} {:edge 2} {:edge 3}] @captured-data)))))

(deftest with-batch-no-flush-on-exception-test
  (testing "with-batch does NOT flush when body throws"
    (let [flush-calls (atom 0)
          result (batch/with-batch [b (batch/tx-batch :conn
                                                      (fn [_ _]
                                                        (swap! flush-calls inc)
                                                        {:tx-id 1}))]
                   (batch/batch-add! b {:edge 1})
                   (throw (Exception. "body failed")))]
      (is (r/err? result))
      (is (= :batch/flush-failed (:error result)))
      (is (= :body (:phase result)))
      (is (zero? @flush-calls) "flush should NOT be called when body throws"))))

(deftest with-batch-flush-failure-test
  (testing "with-batch returns err when flush fails"
    (let [result (batch/with-batch [b (batch/tx-batch :conn
                                                      (fn [_ _]
                                                        (throw (Exception. "flush boom"))))]
                   (batch/batch-add! b {:edge 1}))]
      (is (r/err? result))
      (is (= :batch/flush-failed (:error result))))))

(deftest with-batch-empty-body-test
  (testing "with-batch with no adds returns :batch/empty"
    (let [result (batch/with-batch [b (batch/tx-batch :conn (fn [_ _]))]
                   nil)]
      (is (r/err? result))
      (is (= :batch/empty (:error result))))))

(deftest with-batch-atomicity-test
  (testing "with-batch guarantees atomicity — all or nothing"
    (let [committed (atom [])
          result (batch/with-batch [b (batch/tx-batch :conn
                                                      (fn [_ data]
                                                        (reset! committed data)
                                                        :ok))]
                   (dotimes [i 100]
                     (batch/batch-add! b {:idx i})))]
      (is (r/ok? result))
      (is (= 100 (count @committed))
          "all 100 items should be committed in single transaction"))))

;; =============================================================================
;; normalize-tx-datum unit tests (edge cases)
;; =============================================================================

(deftest normalize-nil-test
  (testing "nil wraps to [nil]"
    (is (= [nil] (batch/normalize-tx-datum nil)))))

(deftest normalize-empty-vector-test
  (testing "empty vector normalizes to empty vector"
    (is (= [] (batch/normalize-tx-datum [])))))

(deftest normalize-map-test
  (testing "map wraps to [map]"
    (is (= [{:a 1}] (batch/normalize-tx-datum {:a 1})))))

(deftest normalize-db-add-vector-test
  (testing "[:db/add ...] wraps to single-element vector"
    (is (= [[:db/add 1 :attr "val"]]
           (batch/normalize-tx-datum [:db/add 1 :attr "val"])))))

(deftest normalize-db-retract-vector-test
  (testing "[:db/retract ...] wraps to single-element vector"
    (is (= [[:db/retract 1 :attr "val"]]
           (batch/normalize-tx-datum [:db/retract 1 :attr "val"])))))

(deftest normalize-db-retractEntity-test
  (testing "[:db.fn/retractEntity ...] wraps to single-element vector"
    (is (= [[:db.fn/retractEntity 42]]
           (batch/normalize-tx-datum [:db.fn/retractEntity 42])))))

(deftest normalize-collection-of-maps-test
  (testing "collection of maps passes through as vec"
    (is (= [{:a 1} {:b 2}]
           (batch/normalize-tx-datum [{:a 1} {:b 2}])))))

(deftest normalize-mixed-collection-test
  (testing "mixed vector collection (not keyword-first) passes through"
    (is (= [{:a 1} [:db/add 1 :b 2]]
           (batch/normalize-tx-datum [{:a 1} [:db/add 1 :b 2]])))))

(deftest normalize-list-collection-test
  (testing "list (sequential but not vector) normalizes to vec"
    (is (= [{:a 1} {:b 2}]
           (batch/normalize-tx-datum (list {:a 1} {:b 2}))))))
