(ns hive-dsl.result.agentop
  "Railway-oriented combinators for agent operations.

   Extends hive-dsl.result with additional primitives needed by the agent
   loop, swarm infrastructure, and CRDT-backed shared state:

     tap          — side-effecting inspection (identity for Result value)
     recover      — Err -> Result via a recovery fn
     retry-on     — repeat a Result-producing thunk on matching Err
     fan-out      — run coll of 0-arg Result-thunks in parallel
     fan-in       — combine coll<Result<a>> into Result<vector<a>>
     with-budget  — atomically deduct cost from a budget atom before op
     with-persona — bind *persona* for the duration of op; enrich Err
     with-crdt    — bind *crdt* for the duration of op (stub — full
                    behavior in T7 once replikativ wrappers land)

   Design notes:
   - Every combinator returns a Result (`{:ok v}` or `{:error k ...}`).
   - No exceptions cross combinator boundaries; thunks that throw are
     left to the caller's own rescue/guard policy from hive-dsl.result.
   - `with-budget` consumes budget whether the thunk returns Ok or Err —
     compute was performed, account for it. Callers wanting refund-on-Err
     should wrap with `recover` + an explicit refund.
   - `*persona*` and `*crdt*` are thread-local dynamic vars; they do NOT
     leak across future/thread boundaries unless caller uses `bound-fn`."
  (:require [hive-dsl.result :as r]))

;; =============================================================================
;; Dynamic context
;; =============================================================================

(def ^:dynamic *persona*
  "Currently-bound persona map. Nil outside a `with-persona` scope."
  nil)

(def ^:dynamic *crdt*
  "Currently-bound CRDT ref. Nil outside a `with-crdt` scope.
   T7 will define the ref shape; today it's opaque."
  nil)

;; =============================================================================
;; tap — inspect without unwrapping
;; =============================================================================

(defn tap
  "Call f on the Ok value for side effect; return the Result unchanged.
   Err results pass through without invoking f. f's return value is ignored."
  [result f]
  (when (r/ok? result)
    (f (:ok result)))
  result)

;; =============================================================================
;; recover — Err -> Result
;; =============================================================================

(defn recover
  "If result is Err, call (f err-map) to produce a recovery Result.
   f's return MUST itself be a Result; if it isn't, it's wrapped in ok.
   Ok results pass through unchanged."
  [result f]
  (if (r/err? result)
    (r/ensure-result (f result))
    result))

;; =============================================================================
;; retry-on — retry a thunk on matching Err
;; =============================================================================

(defn retry-on
  "Call (thunk) — a 0-arg fn returning a Result. If the Result is Err and
   (pred err) is truthy, wait (backoff-ms attempt) and retry. Stops after
   `:max` total attempts, returning the last Result.

   Options:
     :max        — max attempts (>= 1). Default 3.
     :pred       — (fn [err-result] bool). Default (constantly true).
     :backoff-ms — (fn [attempt] ms). attempt starts at 1. Default (constantly 0)."
  [thunk {:keys [max pred backoff-ms]
          :or {max 3 pred (constantly true) backoff-ms (constantly 0)}}]
  (assert (pos-int? max) "retry-on :max must be a positive integer")
  (loop [attempt 1]
    (let [result (thunk)]
      (cond
        (r/ok? result) result
        (>= attempt max) result
        (not (pred result)) result
        :else (do
                (let [ms (backoff-ms attempt)]
                  (when (pos? ms) (Thread/sleep (long ms))))
                (recur (inc attempt)))))))

;; =============================================================================
;; fan-in / fan-out — parallel composition
;; =============================================================================

(defn- collect-results
  "Given a seq of Results (preserving order), return Ok[vs] if all Ok, else
   Err with :err/partials carrying all errs and :err/oks carrying the Oks."
  [results]
  (let [errs (filterv r/err? results)]
    (if (empty? errs)
      (r/ok (mapv :ok results))
      (r/err :partial
             {:err/first (first errs)
              :err/partials errs
              :err/oks (into [] (comp (filter r/ok?) (map :ok)) results)}))))

(defn fan-in
  "Combine a collection of Results into a single Result.
   All Ok -> Ok of a vector of unwrapped values, order preserved.
   Any Err -> Err with :err/first, :err/partials, :err/oks."
  [results]
  (collect-results (vec results)))

(defn fan-out
  "Run a collection of 0-arg Result-thunks in parallel via futures.
   Returns the combined Result (semantics of fan-in over the thunk results).
   Any exception thrown by a thunk becomes an Err via hive-dsl.result/try-effect."
  [thunks]
  (let [futs (mapv (fn [t]
                     (future
                       (try
                         (let [x (t)]
                           (if (or (r/ok? x) (r/err? x))
                             x
                             (r/ok x)))
                         (catch InterruptedException ie
                           (throw ie))
                         (catch Throwable t
                           (r/err :thunk-threw {:exception (.getMessage t)
                                                :class (.getName (class t))})))))
                   thunks)
        results (mapv deref futs)]
    (fan-in results)))

;; =============================================================================
;; with-budget — monotonic atomic deduction
;; =============================================================================

(defn- deduct-or-fail!
  "Atomically deduct cost from budget-atom iff remaining >= cost.
   Returns [:deducted new-remaining] or [:exhausted current-remaining].

   Decision is made from `old` (pre-swap value), not by comparing old/new —
   that way cost=0 on a budget of 0 still counts as :deducted (legitimate
   no-op) rather than :exhausted."
  [budget-atom cost]
  (let [[old new] (swap-vals! budget-atom
                              (fn [remaining]
                                (if (>= remaining cost)
                                  (- remaining cost)
                                  remaining)))]
    (if (>= old cost)
      [:deducted new]
      [:exhausted old])))

(defn with-budget
  "Atomically deduct cost from budget-atom, then run (op) — a 0-arg thunk
   returning a Result. If remaining < cost, returns
   Err :budget-exhausted (op is NOT invoked and NO deduction occurs).

   budget-atom must be a plain Clojure atom holding a non-negative integer.
   Deduction is monotonic and safe under concurrent callers (swap-vals!).

   Design: budget is consumed whether op succeeds or fails. Compute was
   performed; account for it. Callers wanting refund-on-Err can compose
   with `recover` that performs an explicit `(swap! budget-atom + cost)`."
  [budget-atom cost op]
  (assert (nat-int? cost) "with-budget cost must be a non-negative integer")
  (let [[status remaining] (deduct-or-fail! budget-atom cost)]
    (case status
      :exhausted (r/err :budget-exhausted
                        {:budget/remaining remaining
                         :budget/needed cost})
      :deducted (r/ensure-result (op)))))

;; =============================================================================
;; with-persona — bind persona, enrich Err on exit
;; =============================================================================

(defn with-persona
  "Bind *persona* to the given persona map while running (op) — a 0-arg
   thunk returning a Result. If op returns Err, merge `:persona/id` (from
   persona) into the error map so downstream observers see attribution.

   persona is expected to be a map with at least `:persona/id`."
  [persona op]
  (binding [*persona* persona]
    (let [result (r/ensure-result (op))]
      (if (and (r/err? result)
               (contains? persona :persona/id))
        (assoc result :persona/id (:persona/id persona))
        result))))

;; =============================================================================
;; with-crdt — bind crdt ref (T2 stub; T7+ adds full semantics)
;; =============================================================================

(defn with-crdt
  "Bind *crdt* to the given CRDT ref while running (op) — a 0-arg thunk
   returning a Result. T2 stub: simply threads the binding through.
   T7 will add merge hooks + reconciliation callbacks."
  [crdt-ref op]
  (binding [*crdt* crdt-ref]
    (r/ensure-result (op))))
