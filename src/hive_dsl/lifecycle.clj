(ns hive-dsl.lifecycle
  "Lifecycle protocol and bracket macros for composable resource management.

   Provides:

   1. `Lifecycle` protocol — `start!`/`stop!` for any managed resource.

   2. `with-lifecycle` — Single-resource bracket pattern.
      Calls start! on entry, stop! in finally (always).

   3. `with-lifecycle-scope` / `scope-start!` — Multi-resource LIFO cleanup.
      Resources stopped in reverse start order on scope exit.

   4. Reference implementations:
      - `ManagedExecutor` — wraps ScheduledThreadPoolExecutor
      - `ManagedChannel` — wraps core.async channel

   Designed to prevent leaked executors, unclosed channels, and other
   resource leaks that cause GC death spirals."
  (:require [clojure.core.async :as async])
  (:import [java.util.concurrent
            ScheduledThreadPoolExecutor
            TimeUnit]))

;; =============================================================================
;; Lifecycle Protocol
;; =============================================================================

(defprotocol Lifecycle
  (start! [this] "Start resource. Returns started resource or throws.")
  (stop! [this] "Stop/close resource. Idempotent. Returns stopped resource or throws."))

;; =============================================================================
;; Lifecycle State Helpers
;; =============================================================================

(defn lifecycle-state
  "Return the current lifecycle state of a resource.
   Convention: state is stored in an atom at key :state.
   Returns :stopped, :started, or :error."
  [resource]
  (let [s (:state resource)]
    (if (instance? clojure.lang.IDeref s)
      @s
      s)))

(defn started?
  "True if resource is in :started state."
  [resource]
  (= :started (lifecycle-state resource)))

(defn stopped?
  "True if resource is in :stopped state."
  [resource]
  (= :stopped (lifecycle-state resource)))

;; =============================================================================
;; with-lifecycle — Single resource bracket pattern
;; =============================================================================

(defmacro with-lifecycle
  "Bracket pattern for a single Lifecycle resource.

   Calls start! on entry, stop! in finally (always — even on exception).
   Stop! errors are caught and printed as warnings; they do not mask
   the original exception.

   Usage:
     (with-lifecycle [executor (->managed-executor 4)]
       (submit! executor task))

   Expansion:
     (let [raw (->managed-executor 4)
           executor (start! raw)]
       (try
         (submit! executor task)
         (finally
           (try (stop! executor) (catch Throwable t ...)))))"
  [[sym init-expr] & body]
  `(let [raw#  ~init-expr
         ~sym  (start! raw#)]
     (try
       (do ~@body)
       (finally
         (try
           (stop! ~sym)
           (catch Throwable t#
             (binding [*out* *err*]
               (println "WARN: stop! failed during with-lifecycle cleanup:"
                        (.getMessage t#)))))))))

;; =============================================================================
;; with-lifecycle-scope — Multi-resource LIFO cleanup
;; =============================================================================

(defn new-lifecycle-scope
  "Create a new lifecycle scope. Returns an atom holding a stack of
   started Lifecycle resources."
  []
  (atom []))

(defn scope-start!
  "Start a Lifecycle resource within a scope.
   Calls start! on the resource, pushes the started resource onto the
   scope stack, and returns the started resource.

   Usage:
     (scope-start! scope (->managed-executor 4))"
  [scope resource]
  (let [started (start! resource)]
    (swap! scope conj started)
    started))

(defn scope-stop!
  "Stop all resources in a scope in LIFO order.
   Errors during stop are collected but do not halt the sweep.
   Returns vector of any stop errors (empty on success)."
  [scope]
  (let [stack @scope
        errors (volatile! [])]
    (doseq [resource (reverse stack)]
      (try
        (stop! resource)
        (catch Throwable t
          (vswap! errors conj {:resource resource
                               :error    (.getMessage t)}))))
    (reset! scope [])
    @errors))

(defmacro with-lifecycle-scope
  "Multi-resource scope with LIFO cleanup on exit.

   Usage:
     (with-lifecycle-scope [scope]
       (let [exec (scope-start! scope (->managed-executor 4))
             ch   (scope-start! scope (->managed-channel 100))]
         ;; both auto-stopped in LIFO order on exit
         (submit! exec task)))
     ;; ch stopped first, then exec (LIFO)"
  [[scope-sym] & body]
  `(let [~scope-sym (new-lifecycle-scope)]
     (try
       (do ~@body)
       (finally
         (let [errors# (scope-stop! ~scope-sym)]
           (when (seq errors#)
             (binding [*out* *err*]
               (println "WARN: lifecycle scope cleanup had"
                        (count errors#) "error(s)"))))))))

;; =============================================================================
;; Convenience protocols for ManagedExecutor / ManagedChannel
;; =============================================================================

(defprotocol Submittable
  (submit! [this f] "Submit a zero-arg fn for execution. Returns a Future.")
  (schedule! [this f delay-ms]
    "Schedule a zero-arg fn after delay-ms. Returns a ScheduledFuture.")
  (schedule-at-fixed-rate! [this f initial-delay-ms period-ms]
    "Schedule a zero-arg fn at fixed rate. Returns a ScheduledFuture."))

(defprotocol Channeled
  (put! [this val] "Put a value onto the channel. Returns true/false via core.async.")
  (take! [this] "Take a value from the channel. Returns a channel that yields the value."))

;; =============================================================================
;; ManagedExecutor — ScheduledThreadPoolExecutor wrapper
;; =============================================================================

(defrecord ManagedExecutor [pool-size state executor]
  ;; state: atom of :stopped | :started | :error
  ;; executor: atom of nil | ScheduledThreadPoolExecutor

  Lifecycle
  (start! [this]
    (let [current @state]
      (when (= current :started)
        (throw (ex-info "ManagedExecutor already started"
                        {:pool-size pool-size :state current})))
      (try
        (let [exec (ScheduledThreadPoolExecutor. pool-size)]
          (reset! executor exec)
          (reset! state :started)
          this)
        (catch Throwable t
          (reset! state :error)
          (throw t)))))

  (stop! [this]
    (when-not (= :stopped @state)
      (try
        (when-let [exec @executor]
          (.shutdownNow exec)
          (.awaitTermination exec 5 TimeUnit/SECONDS))
        (reset! executor nil)
        (reset! state :stopped)
        (catch Throwable t
          (reset! state :error)
          (throw t))))
    this)

  Submittable
  (submit! [_this f]
    (when-not (= :started @state)
      (throw (ex-info "Cannot submit to stopped executor"
                      {:state @state})))
    (.submit ^ScheduledThreadPoolExecutor @executor ^Callable f))

  (schedule! [_this f delay-ms]
    (when-not (= :started @state)
      (throw (ex-info "Cannot schedule on stopped executor"
                      {:state @state})))
    (.schedule ^ScheduledThreadPoolExecutor @executor
               ^Callable f
               (long delay-ms)
               TimeUnit/MILLISECONDS))

  (schedule-at-fixed-rate! [_this f initial-delay-ms period-ms]
    (when-not (= :started @state)
      (throw (ex-info "Cannot schedule on stopped executor"
                      {:state @state})))
    (.scheduleAtFixedRate ^ScheduledThreadPoolExecutor @executor
                          ^Runnable f
                          (long initial-delay-ms)
                          (long period-ms)
                          TimeUnit/MILLISECONDS)))

(defn ->managed-executor
  "Create an unstarted ManagedExecutor wrapping a ScheduledThreadPoolExecutor.
   Call start! to create the thread pool."
  [pool-size]
  (assert (pos-int? pool-size) "pool-size must be a positive integer")
  (->ManagedExecutor pool-size (atom :stopped) (atom nil)))

;; =============================================================================
;; ManagedChannel — core.async channel wrapper
;; =============================================================================

(defrecord ManagedChannel [buf-size state channel]
  ;; state: atom of :stopped | :started | :error
  ;; channel: atom of nil | core.async channel

  Lifecycle
  (start! [this]
    (let [current @state]
      (when (= current :started)
        (throw (ex-info "ManagedChannel already started"
                        {:buf-size buf-size :state current})))
      (try
        (let [ch (async/chan buf-size)]
          (reset! channel ch)
          (reset! state :started)
          this)
        (catch Throwable t
          (reset! state :error)
          (throw t)))))

  (stop! [this]
    (when-not (= :stopped @state)
      (try
        (when-let [ch @channel]
          (async/close! ch))
        (reset! channel nil)
        (reset! state :stopped)
        (catch Throwable t
          (reset! state :error)
          (throw t))))
    this)

  Channeled
  (put! [_this val]
    (when-not (= :started @state)
      (throw (ex-info "Cannot put to stopped channel"
                      {:state @state})))
    (async/>!! @channel val))

  (take! [_this]
    (when-not (= :started @state)
      (throw (ex-info "Cannot take from stopped channel"
                      {:state @state})))
    (async/<!! @channel)))

(defn ->managed-channel
  "Create an unstarted ManagedChannel wrapping a core.async buffered channel.
   Call start! to create the channel."
  [buf-size]
  (assert (pos-int? buf-size) "buf-size must be a positive integer")
  (->ManagedChannel buf-size (atom :stopped) (atom nil)))
