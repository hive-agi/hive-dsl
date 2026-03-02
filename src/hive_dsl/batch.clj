(ns hive-dsl.batch
  "Transaction batching primitives for write coalescing.

   Accumulates transaction data in an atom, then flushes as a single
   operation on scope exit. Designed for KG/Datahike workloads where
   N individual transact! calls create lock contention.

   Usage:
     (with-batch [batch (tx-batch conn)]
       (batch-add! batch {:kg-edge/from \"a\" :kg-edge/to \"b\"})
       (batch-add! batch {:kg-edge/from \"c\" :kg-edge/to \"d\"}))
     ;; Single transact! on scope exit with all accumulated tx-data"
  (:require [hive-dsl.result :as r]))

;; =============================================================================
;; Batch primitives
;; =============================================================================

(defn tx-batch
  "Create a new transaction batch tied to a connection.

   Returns a batch map:
     {:conn     conn        ;; connection/store for flushing
      :tx-data  (atom [])   ;; accumulated transaction data
      :flush-fn flush-fn}   ;; function (fn [conn tx-data]) to execute

   flush-fn receives the connection and the accumulated tx-data vector.
   It should perform the actual transact! call."
  [conn flush-fn]
  {:conn conn
   :tx-data (atom [])
   :flush-fn flush-fn})

(defn normalize-tx-datum
  "Normalize tx-datum into flat vector of individual datoms.
   Maps -> [map], [:db/add ...] -> [[:db/add ...]], colls -> (vec coll).
   Pure function — no side effects."
  [tx-datum]
  (cond
    (map? tx-datum)                                      [tx-datum]
    (and (vector? tx-datum) (keyword? (first tx-datum))) [tx-datum]
    (sequential? tx-datum)                               (vec tx-datum)
    :else                                                [tx-datum]))

(defn batch-add!
  "Add transaction data to a batch. Accepts a single tx-datum (map or vector)
   or a collection of tx-data.

   Examples:
     (batch-add! batch {:kg-edge/from \"a\" :kg-edge/to \"b\"})
     (batch-add! batch [:db/add eid :attr val])
     (batch-add! batch [{:entity/id 1} {:entity/id 2}])"
  [batch tx-datum]
  (swap! (:tx-data batch) into (normalize-tx-datum tx-datum))
  batch)

(defn batch-flush!
  "Flush accumulated tx-data as a single transaction.

   Returns:
     (r/ok result) on success
     (r/err :batch/empty) if no data accumulated
     (r/err :batch/flush-failed {...}) on exception"
  [batch]
  (let [data @(:tx-data batch)]
    (if (empty? data)
      (r/err :batch/empty {:message "No transaction data accumulated"})
      (try
        (let [result ((:flush-fn batch) (:conn batch) data)]
          (reset! (:tx-data batch) [])
          (r/ok result))
        (catch Throwable t
          (r/err :batch/flush-failed {:message (.getMessage t)
                                      :class (str (class t))
                                      :tx-count (count data)}))))))

(defn batch-count
  "Return the number of accumulated tx-data items in the batch."
  [batch]
  (count @(:tx-data batch)))

(defmacro with-batch
  "Transaction batching scope. Accumulates tx-data, flushes once on exit.

   Normal path: body executes, then batch-flush! runs. Returns flush Result.
   Exception path: body throws, data is NOT flushed (no partial writes),
   returns err with exception info.

   Usage:
     (with-batch [batch (tx-batch conn transact-fn!)]
       (batch-add! batch {:kg-edge/from \"a\" :kg-edge/to \"b\"})
       (batch-add! batch {:kg-edge/from \"c\" :kg-edge/to \"d\"}))
     ;; => (r/ok tx-result) or (r/err :batch/flush-failed ...)"
  [[sym batch-expr] & body]
  `(let [~sym ~batch-expr]
     (try
       (do ~@body)
       (batch-flush! ~sym)
       (catch Throwable t#
         (r/err :batch/flush-failed {:message (.getMessage t#)
                                     :class (str (class t#))
                                     :tx-count (batch-count ~sym)
                                     :phase :body})))))

;; =============================================================================
;; Transparent batch scope (dynamic-var-based)
;; =============================================================================

(defn transparent-batch-scope
  "Create a reusable transparent-batch scope function bound to a dynamic var.

   Arguments:
     batch-var - the Var of a dynamic binding (e.g. #'*tx-batch*)
     flush-fn  - (fn [accumulated-data]) called once with all batched data

   Returns a function (fn [thunk]) that:
   1. If batch-var is already bound (nested scope), just calls thunk —
      data accumulates into the outer scope
   2. Otherwise, binds batch-var to a fresh atom, runs thunk, flushes

   Transparent: existing code that checks @batch-var will accumulate
   data instead of writing immediately, with zero code changes.
   Nestable: inner scopes delegate to the outermost scope.

   Usage:
     ;; Define once (at namespace level):
     (def with-tx-batch-fn
       (transparent-batch-scope #'*tx-batch*
         (fn [data] (proto/transact! store data))))

     ;; Call from anywhere (including cross-namespace resolution):
     (with-tx-batch-fn (fn [] (transact! a) (transact! b) (transact! c)))
     ;; => single flush with [a b c]"
  [batch-var flush-fn]
  (fn [thunk]
    (if (deref batch-var)
      ;; Already inside a batch scope — delegate to outer
      (thunk)
      ;; Outermost scope — collect and flush
      (let [batch (atom [])]
        (push-thread-bindings {batch-var batch})
        (try
          (let [result (thunk)]
            (when (seq @batch)
              (flush-fn @batch))
            result)
          (finally
            (pop-thread-bindings)))))))
