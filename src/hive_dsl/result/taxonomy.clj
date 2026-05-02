(ns hive-dsl.result.taxonomy
  "Extensible error category registry for the Result DSL.

   Base categories are universal (IO, effect, parse, transport).
   Consumers register domain-specific categories at startup via register!.")

;; Set of all registered error category keywords. Thread-safe via atom.
(defonce registry (atom #{}))

(defn register!
  "Register a set of error category keywords.
   Idempotent — safe to call multiple times with overlapping sets.

   (register! #{:kg/node-not-found :chroma/connection-failed})"
  [categories]
  (swap! registry into categories))

(defn known-error?
  "True if category is a registered error keyword."
  [category]
  (contains? @registry category))

(defn registered-categories
  "Return the current set of all registered error categories."
  []
  @registry)

;; =============================================================================
;; Base Categories (universal across all consumers)
;; =============================================================================

(register!
 #{;; I/O errors
   :io/read-failure :io/write-failure :io/timeout :io/not-found

   ;; Effect boundary
   :effect/exception

   ;; Parsing
   :parse/invalid-json :parse/invalid-edn :parse/malformed-input
   :parse/schema-violation

   ;; Transport / networking
   :transport/connection-refused :transport/timeout
   :transport/protocol-error :transport/serialization-error

   ;; Resource lifecycle
   :resource/acquire-failed :resource/release-failed

   ;; Transaction batching
   :batch/flush-failed :batch/empty

   ;; Scope cleanup
   :scope/cleanup-error

   ;; Multi tool — IAddon-native batch DSL
   :multi/missing-ops :multi/invalid-ops
   :multi/missing-tool :multi/missing-command
   :multi/cycle :multi/ref-not-found :multi/dep-failed
   :multi/dsl-unknown-verb :multi/dsl-compile-error
   :multi/budget-exhausted
   :multi/registry-conflict :multi/registry-stale
   ;; Multi plan/run persistence (PR5)
   :multi/plan-not-found :multi/plan-malformed
   :multi/plan-persist-failed :multi/plan-compile-failed})
