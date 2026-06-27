(ns hive-dsl.result
  "Lightweight Result monad for railway-oriented error handling.

   ok  results: {:ok value}
   err results: {:error category ...extra-data}"
  #?(:cljs (:require-macros [hive-dsl.result])))

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

   Strict: every Result-bound RHS MUST evaluate to a Result map
   (`{:ok ..}` or `{:error ..}`). A non-Result value throws `ex-info`
   with category `:result/non-result-binding` and the offending binding
   symbol + form.

   Pure-binding escape hatch: use `:let [v expr ...]` to bind plain
   (non-Result) values inline, mirroring re-frame's `let-fx` and Clojure's
   `for`/`doseq` :let support."
  [bindings & body]
  (cond
    (empty? bindings)
    `(do ~@body)

    (= :let (first bindings))
    (let [[_ let-bindings & rest-bindings] bindings]
      (when-not (vector? let-bindings)
        (throw (ex-info "let-ok :let requires a vector of bindings"
                        {:category :result/let-ok-malformed-let
                         :got      let-bindings})))
      `(let ~let-bindings
         (let-ok ~(vec rest-bindings) ~@body)))

    :else
    (let [[sym expr & rest-bindings] bindings
          r         (gensym "r")
          type-expr (if (:ns &env)
                      (list 'some-> r 'type 'pr-str)
                      (list 'some-> r 'class '.getName))]
      `(let [~r ~expr]
         (cond
           (ok? ~r)  (let [~sym (:ok ~r)]
                       (let-ok ~(vec rest-bindings) ~@body))
           (err? ~r) ~r
           :else
           (throw (ex-info
                    (str "let-ok binding `" '~sym
                         "` produced a non-Result value. "
                         "Wrap pure transformations in r/ok, use :let for "
                         "pure bindings, or move them outside let-ok.")
                    {:category :result/non-result-binding
                     :binding  '~sym
                     :form     '~expr
                     :type     ~type-expr
                     :value    ~r})))))))

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
  (let [e         (gensym "e")
        catch-sym (if (:ns &env) :default 'Exception)]
    `(try
       (ok (do ~@body))
       (catch ~catch-sym ~e
         (err :effect/exception {:message (ex-message ~e)
                                 :class  (str (type ~e))})))))

(defmacro try-effect*
  "Like try-effect but with a custom error category.

   (try-effect* :io/read-failure (slurp path))
   => (ok content) or (err :io/read-failure {:message \"...\"})"
  [category & body]
  (let [e         (gensym "e")
        catch-sym (if (:ns &env) :default 'Exception)]
    `(try
       (ok (do ~@body))
       (catch ~catch-sym ~e
         (err ~category {:message (ex-message ~e)
                         :class  (str (type ~e))})))))

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
  (let [cljs?      (boolean (:ns &env))
        e          (gensym "e")
        fb         (gensym "fb")
        catch-sym  (if cljs? :default 'Throwable)
        meta-check (if cljs?
                     (list 'satisfies? 'cljs.core/IWithMeta fb)
                     (list 'instance? 'clojure.lang.IObj fb))]
    `(try ~@body
          (catch ~catch-sym ~e
            (let [~fb ~fallback]
              (if ~meta-check
                (with-meta ~fb {::error {:message (ex-message ~e)
                                         :form    ~(str (first body))}})
                ~fb))))))

(defmacro guard
  "Selective catch — like rescue but for a specific Throwable subclass.
   Like Erlang's `try ... catch Class:Reason`: structured error handling
   where you care WHAT failed.

   (guard Exception [] (risky-call))            ;; catch Exception only
   (guard java.io.IOException nil (slurp path)) ;; catch IOException only
   (guard AssertionError nil (validated-call x)) ;; catch :pre/:post failures

   Error data shape: {::error {:message \"...\" :form \"(risky-call)\"}}"
  [catch-class fallback & body]
  (let [cljs?      (boolean (:ns &env))
        e          (gensym "e")
        fb         (gensym "fb")
        meta-check (if cljs?
                     (list 'satisfies? 'cljs.core/IWithMeta fb)
                     (list 'instance? 'clojure.lang.IObj fb))]
    `(try ~@body
          (catch ~catch-class ~e
            (let [~fb ~fallback]
              (if ~meta-check
                (with-meta ~fb {::error {:message (ex-message ~e)
                                         :form    ~(str (first body))}})
                ~fb))))))

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
          (catch #?(:clj Throwable :cljs :default) _ fallback)))))

(defn guard-fn
  "Like rescue-fn but catches a specific class. For selective pipelines.

   (keep (guard-fn Exception #(parse %)) items)
   (map  (guard-fn IOException #(read %) :missing) items)"
  ([catch-class f] (guard-fn catch-class f nil))
  ([catch-class f fallback]
   (fn [& args]
     (try (apply f args)
          (catch #?(:clj Throwable :cljs :default) t
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
   that resolve this var in the caller's namespace. On cljs there is no
   runtime var resolution; returns nil (logger degrades silently)."
  []
  #?(:clj  (rescue nil (requiring-resolve 'clojure.tools.logging/warn))
     :cljs nil))

(defmacro rescue-log
  "Like `rescue`, but logs the exception via `clojure.tools.logging/warn`
   under `label` before returning `fallback`. Combines rescue + log +
   error-map reshape into one form. Replaces the common pattern of
   `(try body (catch Exception e (log/warn e ...) fallback))`.

   (rescue-log \"ensure-require\" nil (risky-op))
   ;; => (risky-op) result on success, nil + warn-log on failure

   `label` — short call-site identifier, appears in log output
   `fallback` — returned on any throwable caught
   Logger degrades silently when clojure.tools.logging is absent (always on cljs)."
  [label fallback & body]
  (let [cljs?      (boolean (:ns &env))
        e          (gensym "e")
        fb         (gensym "fb")
        wf         (gensym "warn-fn")
        catch-sym  (if cljs? :default 'Throwable)
        meta-check (if cljs?
                     (list 'satisfies? 'cljs.core/IWithMeta fb)
                     (list 'instance? 'clojure.lang.IObj fb))]
    `(try ~@body
          (catch ~catch-sym ~e
            (when-let [~wf (resolve-warn-fn)]
              (~wf ~e (str ~label " failed: " (ex-message ~e))))
            (let [~fb ~fallback]
              (if ~meta-check
                (with-meta ~fb {::error {:message (ex-message ~e)
                                         :label   ~label
                                         :form    ~(str (first body))}})
                ~fb))))))

(defmacro rescue-interrupt
  "Like `rescue-log`, but treats `InterruptedException` as a silent
   cancellation signal: re-interrupts the current thread and returns
   `fallback` WITHOUT logging. Other throwables fall through to
   `rescue-log` semantics (log + fallback).

   On cljs there is no thread interruption — degrades to `rescue-log`.

   (rescue-interrupt \"query-axioms-worker\" []
     (query-scoped-entries store q))"
  [label fallback & body]
  (if (:ns &env)
    `(rescue-log ~label ~fallback ~@body)
    (let [e  (gensym "e")
          fb (gensym "fb")
          wf (gensym "warn-fn")]
      `(try ~@body
            (catch InterruptedException _#
              (.interrupt (Thread/currentThread))
              ~fallback)
            (catch Throwable ~e
              (when-let [~wf (resolve-warn-fn)]
                (~wf ~e (str ~label " failed: " (ex-message ~e))))
              (let [~fb ~fallback]
                (if (instance? clojure.lang.IObj ~fb)
                  (with-meta ~fb {::error {:message (ex-message ~e)
                                           :label   ~label
                                           :form    ~(str (first body))}})
                  ~fb)))))))
