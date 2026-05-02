(ns hive-dsl.result
  "Lightweight Result monad for railway-oriented error handling.

   ok  results: {:ok value}
   err results: {:error category ...extra-data}")

(defn ok
  "Wrap a value in a success Result."
  [value]
  {:ok value})

(defn err
  "Create an error Result with category keyword and optional data map."
  ([category]
   {:error category})
  ([category data]
   (merge {:error category} data)))

(defn ok?
  "True if result is a success."
  [r]
  (and (map? r) (contains? r :ok)))

(defn err?
  "True if result is an error."
  [r]
  (and (map? r) (contains? r :error)))

(defn bind
  "Monadic bind. If result is ok, applies f to the unwrapped value.
   If result is err, short-circuits and returns the error unchanged.
   f must return a Result."
  [result f]
  (if (ok? result)
    (f (:ok result))
    result))

(defn map-ok
  "Functor map over ok values. Applies f to the unwrapped value and
   re-wraps in ok. Errors pass through unchanged.
   Unlike bind, f returns a plain value (not a Result)."
  [result f]
  (if (ok? result)
    (ok (f (:ok result)))
    result))

(defn map-err
  "Map over error values. Applies f to the error map (without :error key)
   and merges result back. Ok values pass through unchanged.
   f receives the full error result map."
  [result f]
  (if (err? result)
    (f result)
    result))

(defmacro let-ok
  "Monadic let for Results. Binds :ok values; short-circuits on first error.

   (let-ok [x (may-fail)
            y (use x)]
     (ok (+ x y)))

   Strict: every binding RHS MUST evaluate to a Result map (`{:ok ..}` or
   `{:error ..}`). A non-Result value throws `ex-info` with category
   `:result/non-result-binding` and the offending binding symbol + form.

   Why strict: the loose pre-2026-05 form silently treated non-Result
   values as terminators, returning them as the chain's final value
   without executing the body. That semantic produced one of the worst
   failure modes (silent no-ops, `relocate-update` 2026-05-02). For
   pure transformations between Result-returning steps, exit `let-ok`
   into a plain `let`, then re-enter."
  [bindings & body]
  (if (empty? bindings)
    `(do ~@body)
    (let [[sym expr & rest-bindings] bindings]
      `(let [r# ~expr]
         (cond
           (ok? r#)  (let [~sym (:ok r#)]
                       (let-ok ~(vec rest-bindings) ~@body))
           (err? r#) r#
           :else
           (throw (ex-info
                    (str "let-ok binding `" '~sym
                         "` produced a non-Result value. "
                         "Wrap pure transformations in r/ok or move them "
                         "outside let-ok.")
                    {:category :result/non-result-binding
                     :binding  '~sym
                     :form     '~expr
                     :type     (some-> r# class .getName)
                     :value    r#})))))))

(defn ensure-result
  "If x is already a Result (ok or err), return as-is. Otherwise wrap in ok.
   Used by ok-> and ok->> for smart-wrapping step results."
  [x]
  (if (or (ok? x) (err? x))
    x
    (ok x)))

(defmacro ok->
  "Thread-first through Results with smart-wrap. Each step receives the
   unwrapped :ok value. If a step returns a Result, use it directly (bind).
   If it returns a plain value, auto-wrap in ok (fmap). Short-circuits on
   first error.

   Like some-> but with Result-based error info instead of nil.

   (ok-> (validate-order order catalog)   ;; switch: may return err
         price-order                       ;; pure: auto-wrapped in ok
         (acknowledge create-letter)       ;; switch: may return err
         log-order)                        ;; side-effect: auto-wrapped"
  [expr & forms]
  (let [g (gensym "ok__")
        steps (map (fn [step] `(if (err? ~g) ~g
                                   (ensure-result (-> (:ok ~g) ~step))))
                   forms)]
    `(let [~g (ensure-result ~expr)
           ~@(interleave (repeat g) steps)]
       ~g)))

(defmacro ok->>
  "Thread-last through Results with smart-wrap. Like ok-> but threads the
   unwrapped value in last position.

   (ok->> (ok [1 2 3 4])
          (map inc)
          (reduce +))"
  [expr & forms]
  (let [g (gensym "ok__")
        steps (map (fn [step] `(if (err? ~g) ~g
                                   (ensure-result (->> (:ok ~g) ~step))))
                   forms)]
    `(let [~g (ensure-result ~expr)
           ~@(interleave (repeat g) steps)]
       ~g)))

(defmacro try-effect
  "Execute body in try/catch, returning ok on success or err on exception.
   Category defaults to :effect/exception.

   (try-effect (do-side-effect!))
   => (ok result) or (err :effect/exception {:message \"...\"})"
  [& body]
  `(try
     (ok (do ~@body))
     (catch Exception e#
       (err :effect/exception {:message (.getMessage e#)
                               :class  (str (class e#))}))))

(defmacro try-effect*
  "Like try-effect but with a custom error category.

   (try-effect* :io/read-failure (slurp path))
   => (ok content) or (err :io/read-failure {:message \"...\"})"
  [category & body]
  `(try
     (ok (do ~@body))
     (catch Exception e#
       (err ~category {:message (.getMessage e#)
                       :class  (str (class e#))}))))

;; --- rescue / guard: Erlang-inspired two-tier error handling -----------------
;;
;; Erlang's `catch Expr`  → rescue  (supervision boundary, catches everything)
;; Erlang's `try-catch`   → guard   (selective, catches specific class)
;;
;; Both return fallback with error info as metadata. No logging, no strings.

(defmacro rescue
  "Supervision boundary — catch ANY throwable, return fallback.
   Like Erlang's `catch Expr`: never let a failure propagate.
   Error context attached via Clojure metadata, not logging.

   (rescue []  (traverse ids))        ;; => [] on failure, error in ^{::error {...}}
   (rescue nil (get-entry id))        ;; => nil on failure (nil can't carry meta)
   (rescue {}  (compute-stats data))  ;; => {} on failure, (::error (meta result)) for details

   Error data shape: {::error {:message \"...\" :form \"(traverse ids)\"}}"
  [fallback & body]
  `(try ~@body
        (catch Throwable e#
          (let [fb# ~fallback]
            (if (instance? clojure.lang.IObj fb#)
              (with-meta fb# {::error {:message (.getMessage e#)
                                       :form    ~(str (first body))}})
              fb#)))))

(defmacro guard
  "Selective catch — like rescue but for a specific Throwable subclass.
   Like Erlang's `try ... catch Class:Reason`: structured error handling
   where you care WHAT failed.

   (guard Exception [] (risky-call))            ;; catch Exception only
   (guard java.io.IOException nil (slurp path)) ;; catch IOException only
   (guard AssertionError nil (validated-call x)) ;; catch :pre/:post failures

   Error data shape: {::error {:message \"...\" :form \"(risky-call)\"}}"
  [catch-class fallback & body]
  `(try ~@body
        (catch ~catch-class e#
          (let [fb# ~fallback]
            (if (instance? clojure.lang.IObj fb#)
              (with-meta fb# {::error {:message (.getMessage e#)
                                       :form    ~(str (first body))}})
              fb#)))))

(defn on-error
  "Middleware: inspect rescue result, call handler-fn if error metadata present.
   Composes with rescue — rescue captures, on-error reacts.

   (on-error (fn [{:keys [message form]}] (log/error \"failed:\" message))
             (rescue {} (risky-call)))

   handler-fn receives: {:message \"...\" :form \"(risky-call)\"}
   Returns the rescue result unchanged (transparent middleware)."
  [handler-fn result]
  (when-let [err (::error (meta result))]
    (handler-fn err))
  result)

(defn with-error-handler
  "HOF middleware: wrap a rescue-fn with an error callback.
   Creates a composed fn that calls handler-fn on rescue failures.

   (def safe-parse (with-error-handler
                     (rescue-fn #(parse %) nil)
                     (fn [{:keys [message]}] (log/warn \"parse failed:\" message))))
   (safe-parse input)"
  [rescue-wrapped-fn handler-fn]
  (fn [& args]
    (let [result (apply rescue-wrapped-fn args)]
      (on-error handler-fn result))))

(defn rescue-fn
  "Wrap f: on throwable return fallback (default nil). For keep/map pipelines.
   Eliminates (keep (fn [x] (try (f x) (catch Exception _ nil))) coll).

   (keep (rescue-fn #(parse %)) items)           ;; nil on error → filtered by keep
   (map  (rescue-fn #(parse %) :not-found) items) ;; :not-found on error"
  ([f] (rescue-fn f nil))
  ([f fallback]
   (fn [& args]
     (try (apply f args)
          (catch Throwable _ fallback)))))

(defn guard-fn
  "Like rescue-fn but catches a specific class. For selective pipelines.

   (keep (guard-fn Exception #(parse %)) items)
   (map  (guard-fn IOException #(read %) :missing) items)"
  ([catch-class f] (guard-fn catch-class f nil))
  ([catch-class f fallback]
   (fn [& args]
     (try (apply f args)
          (catch Throwable t
            (if (instance? catch-class t)
              fallback
              (throw t)))))))

;; --- rescue-log / rescue-interrupt: logging and cancellation sugar ----------
;;
;; Base primitives alongside `rescue`/`guard`. They have direct try/catch
;; in the expansion because that is the canonical definition of the
;; sugar — callers then use `rescue-log` / `rescue-interrupt` INSTEAD of
;; raw try/catch at their own call sites.
;;
;; `rescue` + manual `on-error` + log + error-map reshape is 6+ lines of
;; boilerplate (see hive-knowledge.structural.core/ensure-require for the
;; motivating pattern). These combinators compress it to one form.
;;
;; Dependency note: `clojure.tools.logging/warn` is resolved at runtime
;; via `requiring-resolve` so hive-dsl itself does not hard-depend on
;; c.t.l. If the logger is absent, logging is a silent no-op.

(defn resolve-warn-fn
  "Best-effort lookup of clojure.tools.logging/warn. Returns fn or nil.
   Public because rescue-log/rescue-interrupt macros expand to call sites
   that resolve this var in the caller's namespace."
  []
  (rescue nil (requiring-resolve 'clojure.tools.logging/warn)))

(defmacro rescue-log
  "Like `rescue`, but logs the exception via `clojure.tools.logging/warn`
   under `label` before returning `fallback`. Combines rescue + log +
   error-map reshape into one form. Replaces the common pattern of
   `(try body (catch Exception e (log/warn e ...) fallback))`.

   (rescue-log \"ensure-require\" nil (risky-op))
   ;; => (risky-op) result on success, nil + warn-log on failure

   (rescue-log \"parse-config\" {:error :config/parse-failed}
     (edn/read-string (slurp path)))

   `label` — short call-site identifier, appears in log output
   `fallback` — returned on any throwable caught
   Logger degrades silently when clojure.tools.logging is absent."
  [label fallback & body]
  `(try ~@body
        (catch Throwable e#
          (when-let [warn-fn# (resolve-warn-fn)]
            (warn-fn# e# (str ~label " failed: " (.getMessage e#))))
          (let [fb# ~fallback]
            (if (instance? clojure.lang.IObj fb#)
              (with-meta fb# {::error {:message (.getMessage e#)
                                       :label   ~label
                                       :form    ~(str (first body))}})
              fb#)))))

(defmacro rescue-interrupt
  "Like `rescue-log`, but treats `InterruptedException` as a silent
   cancellation signal: re-interrupts the current thread and returns
   `fallback` WITHOUT logging. Other throwables fall through to
   `rescue-log` semantics (log + fallback).

   Use inside futures / worker tasks that run on a bounded pool and can
   be cancelled by a deadline watchdog — cancellation is expected, not
   an error, so the log would be noise.

   (rescue-interrupt \"query-axioms-worker\" []
     (query-scoped-entries store q))"
  [label fallback & body]
  `(try ~@body
        (catch InterruptedException _#
          (.interrupt (Thread/currentThread))
          ~fallback)
        (catch Throwable e#
          (when-let [warn-fn# (resolve-warn-fn)]
            (warn-fn# e# (str ~label " failed: " (.getMessage e#))))
          (let [fb# ~fallback]
            (if (instance? clojure.lang.IObj fb#)
              (with-meta fb# {::error {:message (.getMessage e#)
                                       :label   ~label
                                       :form    ~(str (first body))}})
              fb#)))))
