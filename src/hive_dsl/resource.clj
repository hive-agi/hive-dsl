(ns hive-dsl.resource
  "Resource lifecycle macros for bracket-pattern and scoped cleanup.

   Provides two complementary patterns:

   1. `with-resource` — Single resource bracket pattern.
      Acquire returns Result; release ALWAYS runs in finally.

   2. `with-scope` / `scope-acquire!` — Multi-resource LIFO cleanup.
      Resources released in reverse acquisition order on scope exit.

   Both compose naturally with `let-ok` and the Result monad."
  (:require [hive-dsl.result :as r]))

;; =============================================================================
;; with-resource — Bracket pattern returning Result
;; =============================================================================

(defmacro with-resource
  "Bracket pattern: acquire resource, use it, always release.

   Acquire-expr must return a Result. On ok, binds the value to sym
   and executes body. On err, short-circuits. Release fn ALWAYS runs
   in finally (even on exception). Release errors are swallowed.

   Usage:
     (with-resource [conn (acquire-db config)]
       :release close-db!
       (let-ok [data (query conn \"...\")]
         (ok (process data))))

   Expansion:
     (let [acquired (acquire-db config)]
       (if (ok? acquired)
         (let [conn (:ok acquired)]
           (try
             (let-ok [data (query conn \"...\")]
               (ok (process data)))
             (finally
               (try (close-db! conn) (catch Throwable _)))))
         acquired))"
  [[sym acquire-expr] & body]
  (let [[release-kw release-fn & rest-body]
        (if (= :release (first body))
          body
          (throw (ex-info "with-resource requires :release keyword after binding"
                          {:got (first body)})))]
    `(let [acquired# ~acquire-expr]
       (if (r/ok? acquired#)
         (let [~sym (:ok acquired#)]
           (try
             (do ~@rest-body)
             (finally
               (try (~release-fn ~sym)
                    (catch Throwable _#)))))
         acquired#))))

;; =============================================================================
;; with-scope — Multi-resource LIFO cleanup
;; =============================================================================

(defn new-scope
  "Create a new resource scope. Returns an atom holding a stack of
   [resource cleanup-fn] pairs."
  []
  (atom []))

(defn scope-acquire!
  "Acquire a resource within a scope. Calls acquire-fn with args,
   which must return a Result. On ok, pushes [resource cleanup-fn]
   onto the scope stack and returns the Result. On err, returns the error.

   Usage:
     (scope-acquire! scope open-db config)
     ;; acquire-fn is (open-db config), must return Result
     ;; cleanup-fn is (constantly nil) if not provided"
  ([scope acquire-fn & args]
   (scope-acquire! scope nil acquire-fn args))
  ([scope cleanup-fn acquire-fn args]
   (let [result (apply acquire-fn args)]
     (if (r/ok? result)
       (do
         (swap! scope conj [(:ok result) (or cleanup-fn identity)])
         result)
       result))))

(defn scope-release!
  "Release all resources in a scope in LIFO order.
   Errors during cleanup are collected but do not stop the sweep.
   Returns vector of any cleanup errors (empty on success)."
  [scope]
  (let [stack @scope
        errors (volatile! [])]
    (doseq [[resource cleanup-fn] (reverse stack)]
      (try
        (cleanup-fn resource)
        (catch Throwable t
          (vswap! errors conj {:resource resource
                               :error (.getMessage t)}))))
    (reset! scope [])
    @errors))

(defmacro with-scope
  "Multi-resource scope with LIFO cleanup on exit.

   Usage:
     (with-scope [scope]
       (let-ok [db   (scope-acquire! scope close-db! open-db [config])
                conn (scope-acquire! scope close-nrepl! open-nrepl [port])]
         (ok (process db conn))))
     ;; conn released first, then db (LIFO)"
  [[scope-sym] & body]
  `(let [~scope-sym (new-scope)]
     (try
       (do ~@body)
       (finally
         (let [errors# (scope-release! ~scope-sym)]
           (when (seq errors#)
             ;; Log is not available in hive-dsl — errors are silent
             ;; Callers can check scope post-finally if they need error info
             nil))))))
