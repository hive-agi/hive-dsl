(ns hive-dsl.gate
  "Concurrency gate — bounded-permit execution with timeout.

   A gate wraps a java.util.concurrent.Semaphore with:
   - Lifecycle management (start!/stop!)
   - Timeout-aware permit acquisition
   - Result integration (gate-run returns ok/err instead of throwing)
   - Diagnostics (available permits, queue length)

   Three ways to use:

   1. `with-gate` — bracket macro, throws on timeout (for call sites
      that already have try/catch)

   2. `gate-run` — function, returns Result (for railway-oriented pipelines)

   3. `deref-gate` — gated deref for promises/futures with timeout
      (replaces bare @ on blocking calls)

   Usage:
     (def db-read (gate {:permits 4 :timeout-ms 15000 :name \"db-read\"}))
     (def db-write (gate {:permits 1 :timeout-ms 30000 :name \"db-write\"}))

     ;; Bracket — throws on timeout
     (with-gate db-read
       (query-database ...))

     ;; Result — returns err on timeout
     (gate-run db-write #(insert-record! ...))

     ;; Gated deref — replaces bare @
     (deref-gate db-read (chroma/query coll embedding))

     ;; Diagnostics
     (gate-stats db-read)
     ;; => {:name \"db-read\" :permits 4 :available 3 :queued 1}"
  (:require [hive-dsl.result :as r]
            [hive-dsl.lifecycle :as lc])
  (:import [java.util.concurrent Semaphore TimeUnit]))

;; =============================================================================
;; Gate Record
;; =============================================================================

(defrecord Gate [name permits timeout-ms ^Semaphore semaphore state]
  lc/Lifecycle
  (start! [this]
    (reset! state :started)
    this)
  (stop! [this]
    (reset! state :stopped)
    this))

;; =============================================================================
;; Constructor
;; =============================================================================

(defn gate
  "Create a concurrency gate.

   Options:
     :permits    — max concurrent executions (default 1, i.e. serialized)
     :timeout-ms — max wait for permit acquisition (default 30000)
     :name       — diagnostic name (default \"gate\")
     :fair?      — FIFO ordering for waiters (default true)

   Returns a started Gate. No need to call start! separately."
  [{:keys [permits timeout-ms name fair?]
    :or   {permits 1 timeout-ms 30000 name "gate" fair? true}}]
  (let [g (->Gate name permits timeout-ms
                  (Semaphore. (int permits) (boolean fair?))
                  (atom :started))]
    g))

;; =============================================================================
;; Core Execution
;; =============================================================================

(defn gate-run
  "Execute f under the gate's permit. Returns Result.

   On success: (ok <return-value>)
   On timeout: (err :gate/timeout {:name ... :timeout-ms ... :permits ...})
   On error:   (err :gate/execution-failed {:name ... :message ...})"
  [^Gate g f]
  (let [^Semaphore sem (:semaphore g)
        tms             (:timeout-ms g)]
    (if (.tryAcquire sem tms TimeUnit/MILLISECONDS)
      (try
        (r/ok (f))
        (catch Exception e
          (r/err :gate/execution-failed
                 {:name    (:name g)
                  :message (.getMessage e)
                  :class   (str (class e))}))
        (finally
          (.release sem)))
      (r/err :gate/timeout
             {:name       (:name g)
              :timeout-ms tms
              :permits    (:permits g)
              :available  (.availablePermits sem)
              :hint       "Too many concurrent operations or downstream is locked"}))))

(defn gate-run!
  "Execute f under the gate's permit. Throws on timeout.
   For call sites that prefer exceptions over Result."
  [^Gate g f]
  (let [^Semaphore sem (:semaphore g)
        tms             (:timeout-ms g)]
    (if (.tryAcquire sem tms TimeUnit/MILLISECONDS)
      (try
        (f)
        (finally
          (.release sem)))
      (throw (ex-info (str "Gate '" (:name g) "' timed out after " tms "ms")
                      {:name       (:name g)
                       :timeout-ms tms
                       :permits    (:permits g)
                       :available  (.availablePermits sem)})))))

;; =============================================================================
;; Sugar Macros
;; =============================================================================

(defmacro with-gate
  "Bracket macro — execute body under gate permit. Throws on timeout.

   (with-gate db-read
     (query-database ...))"
  [g & body]
  `(gate-run! ~g (fn [] ~@body)))

(defmacro with-gate-result
  "Bracket macro — execute body under gate permit. Returns Result.

   (with-gate-result db-write
     (insert-record! ...))"
  [g & body]
  `(gate-run ~g (fn [] ~@body)))

;; =============================================================================
;; Gated Deref — replaces bare @ on promises/futures
;; =============================================================================

(defn deref-gate
  "Deref a promise/future under the gate's permit with timeout.
   Replaces bare `@(chroma/query ...)` with bounded concurrency + timeout.

   Returns the deref'd value or throws on timeout/error.

   Usage:
     (deref-gate read-gate (chroma/query coll embedding))"
  ([^Gate g derefable]
   (deref-gate g derefable (:timeout-ms g)))
  ([^Gate g derefable timeout-ms]
   (let [^Semaphore sem (:semaphore g)
         gname           (:name g)]
     (if (.tryAcquire sem timeout-ms TimeUnit/MILLISECONDS)
       (try
         (let [result (deref derefable timeout-ms ::timeout)]
           (if (= result ::timeout)
             (throw (ex-info (str "Gate '" gname "' deref timed out after " timeout-ms "ms")
                             {:name       gname
                              :timeout-ms timeout-ms
                              :hint       "Downstream may be locked or unresponsive"}))
             result))
         (finally
           (.release sem)))
       (throw (ex-info (str "Gate '" gname "' full — too many concurrent operations")
                       {:name       gname
                        :timeout-ms timeout-ms
                        :permits    (:permits g)
                        :available  (.availablePermits sem)}))))))

;; =============================================================================
;; Diagnostics
;; =============================================================================

(defn gate-stats
  "Current gate state for diagnostics.

   Returns:
     {:name \"db-read\"
      :permits 4
      :available 3
      :queue-length 1
      :state :started}"
  [^Gate g]
  (let [^Semaphore sem (:semaphore g)]
    {:name         (:name g)
     :permits      (:permits g)
     :available    (.availablePermits sem)
     :queue-length (.getQueueLength sem)
     :state        @(:state g)}))
